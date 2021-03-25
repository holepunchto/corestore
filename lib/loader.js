const { NanoresourcePromise: Nanoresource } = require('nanoresource-promise/emitter')
const Omega = require('omega')
const hypercoreCrypto = require('hypercore-crypto')
const deriveSeed = require('derive-key')

const PendingFile = require('./pending-file')
const errors = require('./errors')

const SEED_NAMESPACE = 'corestore'
const NAMESPACE_SEPARATOR = ':'
const MASTER_KEY_FILENAME = 'master_key'

module.exports = class Loader extends Nanoresource {
  constructor (storage, db, opts = {}) {
    super()

    this.storage = storage
    this.db = db
    this.masterKey = opts.masterKey
    this.overwriteMasterKey = opts.overwriteMasterKey
    this.opts = opts

    this.readyCache = new Map()
    this.cache = new Map()
  }

  _loadMasterKey (cb) {
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

  _evictOnClose (core, id, namespace, opts) {
    core.once('close', isLastSession => {
      if (!isLastSession) return
      if (namespace) {
        const keys = this._generateKeys(namespace, opts)
        id = this._getCacheId(keys)
      }
      this.readyCache.delete(id)
      this.cache.delete(id)
    })
  }

  _flushReadyCache () {
    for (const { core, opts, namespace } of this.readyCache.values()) {
      const keys = this._generateKeys(namespace, opts)
      const id = this._getCacheId(keys)
      this.cache.set(id, core)
    }
    this.readyCache.clear()
  }

  _generateKeyPair (namespace, name) {
    if (namespace) name = [...namespace, ...name].join(NAMESPACE_SEPARATOR)
    if (!this.masterKey) return { name }
    const seed = deriveSeed(SEED_NAMESPACE, this.masterKey, name)
    const keyPair = hypercoreCrypto.keyPair(seed)
    const discoveryKey = hypercoreCrypto.discoveryKey(keyPair.publicKey)
    return { name, publicKey: keyPair.publicKey, secretKey: keyPair.secretKey, discoveryKey }
  }

  _generateKeys (namespace, opts) {
    // The full name is stored in the index, so if we're loading from disk it should override.
    if (opts.fullName) return this._generateKeyPair(null, opts.fullName)
    if (opts.name) return this._generateKeyPair(namespace, opts.name)
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
    if (opts.key || opts.publicKey) {
      const publicKey = opts.key || opts.publicKey
      return {
        publicKey,
        secretKey: null,
        discoveryKey: hypercoreCrypto.discoveryKey(publicKey),
        name: null
      }
    }
    throw new errors.InvalidOptionsError()
  }

  _getCacheId (keys) {
    return keys.discoveryKey.toString('hex')
  }

  // Nanoresource Methods

  async _open () {
    await this.db.open()
    await new Promise((resolve, reject) => {
      this._loadMasterKey(err => {
        if (err) return reject(err)
        return resolve()
      })
    })
    await this._flushReadyCache()
  }

  async _close () {
    if (!this.cache.size) return
    const activeCores = this.getOpenCores()
    const closePromises = activeCores.map(core => core.close())
    return Promise.allSettled(closePromises)
  }

  // Hypercore Loading

  async _loadKeys (namespace, opts) {
    await this.open()
    const keys = this._generateKeys(namespace, opts)
    await this.db.maybeSaveKeys(keys, NAMESPACE_SEPARATOR)
    if (!keys.name) keys.name = await this.db.getName(keys)
    if (keys.secretKey) return { keyPair: keys }
    return {
      keyPair: this._generateKeys(namespace, {
        ...opts,
        name: keys.name,
        fullName: keys.name
      })
    }
  }

  _createStorage (storage, keysProm) {
    let storageRoot = null
    const storageProm = keysProm.then(({ keyPair: keys }) => {
      const id = this._getCacheId(keys)
      storageRoot = [id.slice(0, 2), id.slice(2, 4), id].join('/')
    }, err => this.emit('loading-error', err))
    return name => {
      if (this.opened && storageRoot) return this.storage(storageRoot + '/' + name)
      return new PendingFile(cb => {
        storageProm.then(() => {
          cb(null, storage(storageRoot + '/' + name))
        }, err => cb(err))
      })
    }
  }

  _create (id, namespace, opts, keys) {
    const cacheOpts = { ...this.opts.cache }
    if (opts.cache) {
      if (opts.cache.data === false) delete cacheOpts.data
      if (opts.cache.tree === false) delete cacheOpts.tree
    }
    if (cacheOpts.data) cacheOpts.data = cacheOpts.data.namespace()
    if (cacheOpts.tree) cacheOpts.tree = cacheOpts.tree.namespace()

    const publicKey = keys && keys.publicKey
    const keysPromise = this._loadKeys(namespace, opts, keys)
    const storage = this._createStorage(this.storage, keysPromise)

    const core = new Omega(storage, publicKey, {
      ...this.opts,
      ...opts,
      cache: cacheOpts,
      storeSecretKey: false,
      createIfMissing: !!publicKey,
      preload: () => keysPromise
    })

    if (this.opened) {
      this.cache.set(id, core)
    } else {
      this.readyCache.set(id, { core, opts, namespace })
    }

    let errored = false
    const onerror = () => {
      if (errored) return
      errored = true
      this.readyCache.delete(id)
      this.cache.delete(id)
    }
    const onready = () => {
      if (errored) return
      this.readyCache.delete(id)
      this.emit('core', core, opts)
    }

    core.ready().then(onready, onerror)

    return core
  }

  _getBeforeReady (namespace, opts) {
    let id = null
    if (opts.name) {
      id = [...namespace, ...opts.name].join(NAMESPACE_SEPARATOR)
    } else {
      id = opts.key || opts
      if (Buffer.isBuffer(id)) id = id.toString('hex')
    }

    const cached = this.readyCache.get(id)
    const core = (cached && cached.core) || this._create(id, namespace, opts)

    const session = cached ? core.session() : core

    this._evictOnClose(session, id, namespace, opts)

    return session
  }

  _get (namespace, opts) {
    const keys = this._generateKeys(namespace, opts)
    const id = this._getCacheId(keys)

    const cached = this.cache.get(id)
    const core = cached || this._create(id, namespace, opts, keys)

    const session = cached ? core.session() : core

    this._evictOnClose(session, id)

    return session
  }

  get (namespace, opts) {
    if (!this.opened) return this._getBeforeReady(namespace, opts)
    return this._get(namespace, opts)
  }

  async getPassiveCore (dkey) {
    if (Buffer.isBuffer(dkey)) dkey = dkey.toString('hex')
    const keys = await this.db.getPassiveCoreKeys(dkey)
    if (!keys) return null
    return this._get(null, { ...keys, passive: true })
  }

  getOpenCores () {
    return [...this.cache.values()]
  }
}
