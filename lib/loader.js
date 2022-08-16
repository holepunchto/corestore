const { NanoresourcePromise: Nanoresource } = require('nanoresource-promise/emitter')
const hypercore = require('hypercore')
const hypercoreCrypto = require('hypercore-crypto')
const deriveSeed = require('derive-key')
const RefPool = require('refpool')

const BufferFile = require('./buffer-file')
const PendingFile = require('./pending-file')
const errors = require('./errors')

const SEED_NAMESPACE = 'corestore'
const NAMESPACE_SEPARATOR = ':'
const MASTER_KEY_FILENAME = 'master_key'

module.exports = class Loader extends Nanoresource {
  constructor(storage, db, opts = {}) {
    super()

    this.storage = storage
    this.db = db
    this.masterKey = opts.masterKey
    this.overwriteMasterKey = opts.overwriteMasterKey

    this.readyCache = new Map()
    this.cache = new RefPool({
      maxSize: opts.cacheSize || 1000,
      close: core => {
        core.close(err => {
          if (err) this.emit('error', err)
        })
      }
    })
  }

  _loadMasterKey(cb) {
    if (this.masterKey && !this.overwriteMasterKey) return cb(null)
    const keyStorage = this.storage(MASTER_KEY_FILENAME)
    keyStorage.stat((err, st) => {
      if (err && err.code !== 'ENOENT') return cb(err)
      if (err || st.size < 32 || this.overwriteMasterKey) {
        this.masterKey = this.masterKey || hypercoreCrypto.randomBytes(32)
        return keyStorage.write(0, this.masterKey, err => {
          if (err) return cb(err)
          keyStorage.close(cb)
        })
      }
      keyStorage.read(0, 32, (err, key) => {
        if (err) return cb(err)
        this.masterKey = key
        keyStorage.close(cb)
      })
    })
  }

  _flushReadyCache() {
    for (const [id, { core, opts, namespace }] of this.readyCache) {
      const keys = this._generateKeys(namespace, opts)
      const id = this._getCacheId(keys)
      this.cache.set(id, core)
      this.cache.increment(id)
    }
    this.readyCache.clear()
  }

  _generateKeyPair(namespace, name) {
    if (!namespace || !namespace.length) throw new errors.InvalidNamespaceError()
    if (typeof name === 'string') name = Buffer.from(name)

    if (namespace) name = [...namespace, name].join(NAMESPACE_SEPARATOR)
    const seed = deriveSeed(SEED_NAMESPACE, this.masterKey, name)

    const keyPair = hypercoreCrypto.keyPair(seed)
    const discoveryKey = hypercoreCrypto.discoveryKey(keyPair.publicKey)
    return { name, publicKey: keyPair.publicKey, secretKey: keyPair.secretKey, discoveryKey }
  }

  _generateKeys(namespace, opts) {
    if (opts.keyPair) {
      const publicKey = opts.keyPair.publicKey
      const secretKey = opts.keyPair.secretKey
      return {
        publicKey,
        secretKey,
        discoveryKey: hypercoreCrypto.discoveryKey(publicKey),
        name: null
      }
    }
    if (opts.key) {
      const publicKey = opts.key
      return {
        publicKey,
        secretKey: null,
        discoveryKey: hypercoreCrypto.discoveryKey(publicKey),
        name: null
      }
    }
    if (opts.name) return this._generateKeyPair(namespace, opts.name)
    throw new errors.InvalidOptionsError()
  }

  _getCacheId(keys) {
    return keys.discoveryKey.toString('hex')
  }

  // Nanoresource Methods

  async _open() {
    await new Promise((resolve, reject) => {
      this._loadMasterKey(err => {
        if (err) return reject(err)
        return resolve()
      })
    })
    await this._flushReadyCache()
  }

  async _close() {
    if (!this.cache.size) return
    const activeCores = this.getOpenCores()
    const closePromises = activeCores.map(core => new Promise((resolve, reject) => {
      core.close(err => {
        if (err) return reject(err)
        return resolve(null)
      })
    }))
    return Promise.allSettled(closePromises)
  }

  // Hypercore Loading

  _createStorage(namespace, opts, keys) {
    let storageRoot = null
    const configure = () => {
      keys = this._generateKeys(namespace, opts)
      const id = this._getCacheId(keys)
      storageRoot = [id.slice(0, 2), id.slice(2, 4), id].join('/')
    }
    const loadSecretKey = cb => {
      this._db.saveKeys(keys)
        .then(this._db.getName(keys))
        .then(name => this._generateKeys(namespace, { ...opts, name }))
        .then(keys => cb(null, keys.privateKey), err => cb(err))
    }
    const storage = name => {
      if (!keys) configure()
      if (name === 'key') return new BufferFile(keys.publicKey)
      if (name === 'secret_key') return new BufferFile(loadSecretKey)
      else return this.storage(storageRoot + '/' + name)
    }
    return name => {
      if (this.opened) return storage(name)
      return new PendingFile(cb => {
        this.open().then(() => cb(null, storage(name)), err => cb(err))
      })
    }
  }

  _create(id, namespace, opts, keys) {
    const cacheOpts = { ...this.opts.cache }
    if (opts.cache) {
      if (opts.cache.data === false) delete cacheOpts.data
      if (opts.cache.tree === false) delete cacheOpts.tree
    }
    if (cacheOpts.data) cacheOpts.data = cacheOpts.data.namespace()
    if (cacheOpts.tree) cacheOpts.tree = cacheOpts.tree.namespace()

    const storage = this._createStorage(namespace, opts, keys)
    const core = hypercore(storage, keys.publicKey, {
      ...this.opts,
      ...opts,
      cache: cacheOpts,
      createIfMissing: !!keys.publicKey
    })

    if (this.opened) {
      this.cache.set(id, core)
      this.cache.increment(id)
    } else {
      this.readyCache.set(id, { core, opts, namespace })
    }

    core.ifAvailable.wait()

    let errored = false
    const onready = () => {
      if (errored) return
      this.emit('core', core, opts)
      core.removeListener('error', onerror)
      core.ifAvailable.continue()
    }
    const onerror = () => {
      errored = true
      core.ifAvailable.continue()
      this.readyCache.delete(id)
      this.cache.delete(id)
    }
    const onclose () => {
      this.readyCache.delete(id)
      this.cache.delete(id)
    }
    core.once('ready', onready)
    core.once('error', onerror)
    core.once('close', onclose)

    return core
  }

  _getBeforeReady(namespace, opts) {
    let id = opts.name || opts.key || opts
    if (Buffer.isBuffer(id)) id = id.toString('hex')

    if (this.readyCache.has(id)) return this.readyCache.get(id)

    const core = this._create(id, namespace, opts)
    return core
  }

  _get(namespace, opts) {
    const keys = this._generateKeys(namespace, opts)
    const id = this._getCacheId(keys)

    if (this.cache.has(id)) return this.cache.get(id)

    const core = this._create(id, namespace, opts, keys)
    return core
  }

  get(namespace, opts) {
    if (!this.opened) return this._getBeforeReady(namespace, opts)
    return this._get(namespace, opts)
  }

  getPassiveCore(dkey) {
    if (Buffer.isBuffer(dkey)) dkey = dkey.toString('hex')
    const name = await this._db.getPassiveCoreName(dkey)
    if (!name) return null
    return this._get(null, { name, passive: true })
  }

  getOpenCores() {
    return [...this.cache.entries.values()].map(({ value }) => value)
  }
}
