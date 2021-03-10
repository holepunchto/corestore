const DDatabaseProtocol = require('@ddatabase/protocol')
const Nanoresource = require('nanoresource/emitter')
const ddatabase = require('ddatabase')
const ddatabaseCrypto = require('@ddatabase/crypto')
const dwebEncoding = require('dwebx-encoding')
const maybe = require('call-me-maybe')

const RefPool = require('refpool')
const deriveSeed = require('dweb-derive-key')
const derivedStorage = require('dweb-derived-key-storage')
const raf = require('random-access-file')

const MASTER_KEY_FILENAME = 'master_key'
const NAMESPACE = 'basestorevault'
const NAMESPACE_SEPERATOR = ':'

class InnerBasestore extends Nanoresource {
  constructor (storage, opts = {}) {
    super()

    if (typeof storage === 'string') storage = defaultStorage(storage)
    if (typeof storage !== 'function') throw new Error('Storage should be a function or string')
    this.storage = storage

    this.opts = opts

    this._replicationStreams = []
    this.cache = new RefPool({
      maxSize: opts.cacheSize || 1000,
      close: base => {
        base.close(err => {
          if (err) this.emit('error', err)
        })
      }
    })

    // Generated in _open
    this._masterKey = opts.masterKey || null
    this._id = ddatabaseCrypto.randomBytes(8)

    // As discussed in https://github.com/dwebprotocol/basestorevault/issues/20
    this.setMaxListeners(0)
  }

  // Nanoresource Methods

  _open (cb) {
    if (this._masterKey) return cb()
    const keyStorage = this.storage(MASTER_KEY_FILENAME)
    keyStorage.stat((err, st) => {
      if (err && err.code !== 'ENOENT') return cb(err)
      if (err || st.size < 32) {
        this._masterKey = ddatabaseCrypto.randomBytes(32)
        return keyStorage.write(0, this._masterKey, err => {
          if (err) return cb(err)
          keyStorage.close(cb)
        })
      }
      keyStorage.read(0, 32, (err, key) => {
        if (err) return cb(err)
        this._masterKey = key
        keyStorage.close(cb)
      })
    })
  }

  _close (cb) {
    let error = null
    for (const { stream } of this._replicationStreams) {
      stream.destroy()
    }
    if (!this.cache.size) return process.nextTick(cb, null)
    let remaining = this.cache.size
    for (const { value: base } of this.cache.entries.values()) {
      base.close(err => {
        if (err) error = err
        if (!--remaining) {
          if (error) return cb(error)
          return cb(null)
        }
      })
    }
  }

  // Private Methods

  _checkIfExists (dkey, cb) {
    dkey = encodeKey(dkey)
    if (this.cache.has(dkey)) return process.nextTick(cb, null, true)

    const baseStorage = this.storage([dkey.slice(0, 2), dkey.slice(2, 4), dkey, 'key'].join('/'))

    baseStorage.read(0, 32, (err, key) => {
      if (err) return cb(err)
      baseStorage.close(err => {
        if (err) return cb(err)
        if (!key) return cb(null, false)
        return cb(null, true)
      })
    })
  }

  _injectIntoReplicationStreams (base) {
    for (const { stream, opts } of this._replicationStreams) {
      this._replicateBase(false, base, stream, { ...opts })
    }
  }

  _replicateBase (isInitiator, base, mainStream, opts) {
    if (!base) return
    base.ready(function (err) {
      if (err) return
      base.replicate(isInitiator, {
        ...opts,
        stream: mainStream
      })
    })
  }

  _deriveSecret (namespace, name) {
    return deriveSeed(namespace, this._masterKey, name)
  }

  _generateKeyPair (name) {
    if (typeof name === 'string') name = Buffer.from(name)
    else if (!name) name = ddatabaseCrypto.randomBytes(32)

    const seed = this._deriveSecret(NAMESPACE, name)

    const keyPair = ddatabaseCrypto.keyPair(seed)
    const discoveryKey = ddatabaseCrypto.discoveryKey(keyPair.publicKey)
    return { name, publicKey: keyPair.publicKey, secretKey: keyPair.secretKey, discoveryKey }
  }

  _generateKeys (baseOpts) {
    if (!baseOpts) baseOpts = {}
    if (typeof baseOpts === 'string') baseOpts = Buffer.from(baseOpts, 'hex')
    if (Buffer.isBuffer(baseOpts)) baseOpts = { key: baseOpts }

    if (baseOpts.keyPair) {
      const publicKey = baseOpts.keyPair.publicKey
      const secretKey = baseOpts.keyPair.secretKey
      return {
        publicKey,
        secretKey,
        discoveryKey: ddatabaseCrypto.discoveryKey(publicKey),
        name: null
      }
    }
    if (baseOpts.key) {
      const publicKey = decodeKey(baseOpts.key)
      return {
        publicKey,
        secretKey: null,
        discoveryKey: ddatabaseCrypto.discoveryKey(publicKey),
        name: null
      }
    }
    if (baseOpts.default || baseOpts.name) {
      if (!baseOpts.name) throw new Error('If the default option is set, a name must be specified.')
      return this._generateKeyPair(baseOpts.name)
    }
    if (baseOpts.discoveryKey) {
      const discoveryKey = decodeKey(baseOpts.discoveryKey)
      return {
        publicKey: null,
        secretKey: null,
        discoveryKey,
        name: null
      }
    }
    return this._generateKeyPair(null)
  }

  // Public Methods

  isLoaded (baseOpts) {
    const generatedKeys = this._generateKeys(baseOpts)
    return this.cache.has(encodeKey(generatedKeys.discoveryKey))
  }

  isExternal (baseOpts) {
    const generatedKeys = this._generateKeys(baseOpts)
    const entry = this._cache.entry(encodeKey(generatedKeys.discoveryKey))
    if (!entry) return false
    return entry.refs !== 0
  }

  get (baseOpts = {}) {
    if (!this.opened) throw new Error('Basestorevault.ready must be called before get.')

    const self = this

    const generatedKeys = this._generateKeys(baseOpts)
    const { publicKey, discoveryKey, secretKey } = generatedKeys
    const id = encodeKey(discoveryKey)

    const cached = this.cache.get(id)
    if (cached) return cached

    const storageRoot = [id.slice(0, 2), id.slice(2, 4), id].join('/')

    const keyStorage = derivedStorage(createStorage, (name, cb) => {
      if (name) {
        const res = this._generateKeyPair(name)
        if (discoveryKey && (!discoveryKey.equals((res.discoveryKey)))) {
          return cb(new Error('Stored an incorrect name.'))
        }
        return cb(null, res)
      }
      if (secretKey) return cb(null, generatedKeys)
      if (publicKey) return cb(null, { name: null, publicKey, secretKey: null })
      const err = new Error('Unknown key pair.')
      err.unknownKeyPair = true
      return cb(err)
    })

    const cacheOpts = { ...this.opts.cache }
    if (baseOpts.cache) {
      if (baseOpts.cache.data === false) delete cacheOpts.data
      if (baseOpts.cache.tree === false) delete cacheOpts.tree
    }
    if (cacheOpts.data) cacheOpts.data = cacheOpts.data.namespace()
    if (cacheOpts.tree) cacheOpts.tree = cacheOpts.tree.namespace()

    const base = ddatabase(name => {
      if (name === 'key') return keyStorage.key
      if (name === 'secret_key') return keyStorage.secretKey
      return createStorage(name)
    }, publicKey, {
      ...this.opts,
      ...baseOpts,
      cache: cacheOpts,
      createIfMissing: !!publicKey
    })

    this.cache.set(id, base)
    base.ifAvailable.wait()

    var errored = false
    base.once('error', onerror)
    base.once('ready', onready)
    base.once('close', onclose)

    return base

    function onready () {
      if (errored) return
      self.emit('feed', base, baseOpts)
      base.removeListener('error', onerror)
      self._injectIntoReplicationStreams(base)
      // TODO: nexttick here needed? prob not, just legacy
      process.nextTick(() => base.ifAvailable.continue())
    }

    function onerror (err) {
      errored = true
      base.ifAvailable.continue()
      self.cache.delete(id)
      if (err.unknownKeyPair) {
        // If an error occurs during creation by discovery key, then that base does not exist on disk.
        // TODO: This should not throw, but should propagate somehow.
      }
    }

    function onclose () {
      self.cache.delete(id)
    }

    function createStorage (name) {
      return self.storage(storageRoot + '/' + name)
    }
  }

  replicate (isInitiator, bases, replicationOpts = {}) {
    const self = this

    const finalOpts = { ...this.opts, ...replicationOpts }
    const mainStream = replicationOpts.stream || new DDatabaseProtocol(isInitiator, { ...finalOpts })
    var closed = false

    for (const base of bases) {
      this._replicateBase(isInitiator, base, mainStream, { ...finalOpts })
    }

    mainStream.on('discovery-key', ondiscoverykey)
    mainStream.on('finish', onclose)
    mainStream.on('end', onclose)
    mainStream.on('close', onclose)

    const streamState = { stream: mainStream, opts: finalOpts }
    this._replicationStreams.push(streamState)

    return mainStream

    function ondiscoverykey (dkey) {
      // Get will automatically add the base to all replication streams.
      self._checkIfExists(dkey, (err, exists) => {
        if (closed) return
        if (err || !exists) return mainStream.close(dkey)
        const passiveBase = self.get({ discoveryKey: dkey })
        self._replicateBase(false, passiveBase, mainStream, { ...finalOpts })
      })
    }

    function onclose () {
      if (!closed) {
        self._replicationStreams.splice(self._replicationStreams.indexOf(streamState), 1)
        closed = true
      }
    }
  }
}

class Basestorevault extends Nanoresource {
  constructor (storage, opts = {}) {
    super()

    this.storage = storage
    this.name = opts.name || 'default'
    this.inner = opts.inner || new InnerBasestore(storage, opts)
    this.cache = this.inner.cache
    this.store = this // Backwards-compat for NamespacedBasestore

    this._parent = opts.parent
    this._isNamespaced = !!opts.name
    this._openedBases = new Map()

    const onfeed = feed => this.emit('feed', feed)
    const onerror = err => this.emit('error', err)
    this.inner.on('feed', onfeed)
    this.inner.on('error', onerror)
    this._unlisten = () => {
      this.inner.removeListener('feed', onfeed)
      this.inner.removeListener('error', onerror)
    }
  }

  ready (cb) {
    return maybe(cb, new Promise((resolve, reject) => {
      this.open(err => {
        if (err) return reject(err)
        return resolve()
      })
    }))
  }

  // Nanoresource Methods

  _open (cb) {
    return this.inner.open(cb)
  }

  _close (cb) {
    this._unlisten()
    if (!this._parent) return this.inner.close(cb)
    for (const dkey of this._openedBases) {
      this.cache.decrement(dkey)
    }
    return process.nextTick(cb, null)
  }

  // Private Methods

  _maybeIncrement (base) {
    const id = encodeKey(base.discoveryKey)
    if (this._openedBases.has(id)) return
    this._openedBases.set(id, base)
    this.cache.increment(id)
  }

  // Public Methods

  get (baseOpts = {}) {
    if (Buffer.isBuffer(baseOpts)) baseOpts = { key: baseOpts }
    const base = this.inner.get(baseOpts)
    this._maybeIncrement(base)
    return base
  }

  default (baseOpts = {}) {
    if (Buffer.isBuffer(baseOpts)) baseOpts = { key: baseOpts }
    return this.get({ ...baseOpts, name: this.name })
  }

  namespace (name) {
    if (!name) name = ddatabaseCrypto.randomBytes(32)
    if (Buffer.isBuffer(name)) name = name.toString('hex')
    name = this._isNamespaced ? this.name + NAMESPACE_SEPERATOR + name : name
    return new Basestorevault(this.storage, {
      inner: this.inner,
      parent: this,
      name
    })
  }

  replicate (isInitiator, opts) {
    const bases = !this._parent ? allReferenced(this.cache) : this._openedBases.values()
    return this.inner.replicate(isInitiator, bases, opts)
  }

  isLoaded (baseOpts) {
    return this.inner.isLoaded(baseOpts)
  }

  isExternal (baseOpts) {
    return this.inner.isExternal(baseOpts)
  }

  list () {
    return new Map([...this._openedBases])
  }
}

function * allReferenced (cache) {
  for (const entry of cache.entries.values()) {
    if (entry.refs > 0) yield entry.value
    continue
  }
}

function encodeKey (key) {
  return Buffer.isBuffer(key) ? dwebEncoding.encode(key) : key
}

function decodeKey (key) {
  return (typeof key === 'string') ? dwebEncoding.decode(key) : key
}

function defaultStorage (dir) {
  return function (name) {
    try {
      var lock = name.endsWith('/bitfield') ? require('fd-lock') : null
    } catch (err) {}
    return raf(name, { directory: dir, lock: lock })
  }
}

module.exports = Basestorevault
