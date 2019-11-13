const { EventEmitter } = require('events')

const HypercoreProtocol = require('hypercore-protocol')
const hypercore = require('hypercore')
const hypercoreCrypto = require('hypercore-crypto')
const datEncoding = require('dat-encoding')

const thunky = require('thunky')
const LRU = require('lru')
const Multigraph = require('hypertrie-multigraph')
const deriveSeed = require('derive-key')
const derivedStorage = require('derived-key-storage')
const maybe = require('call-me-maybe')
const raf = require('random-access-file')

const PARENT_LABEL = 'parent'
const CHILD_LABEL = 'child'
const MASTER_KEY_FILENAME = 'master_key'
const NAMESPACE = 'corestore'

class NamespacedCorestore {
  constructor (corestore, name) {
    this.name = name
    this.store = corestore
    this._opened = new Set()
    this.ready = this.store.ready.bind(this.store)
  }

  _processCore (core) {
    if (!this._opened.has(core)) this.store._incrementReference(core)
    this._opened.add(core)
    core.once('close', () => {
      this._opened.delete(core)
    })
    return core
  }

  default (coreOpts = {}) {
    if (Buffer.isBuffer(coreOpts)) coreOpts = { key: coreOpts }
    const core = this.store.default({ ...coreOpts, _name: this.name, namespaced: true })
    return this._processCore(core)
  }

  get (coreOpts = {}) {
    if (Buffer.isBuffer(coreOpts)) coreOpts = { key: coreOpts }
    const core = this.store.get({ ...coreOpts, namespaced: true })
    return this._processCore(core)
  }

  replicate (discoveryKey, opts) {
    // TODO: This should only replicate the cores in this._opened.
    return this.store.replicate(discoveryKey, { ...opts, cores: this._opened })
  }

  close (cb) {
    const self = this
    let pending = this._opened.size + 1
    let error = null

    for (const core of this._opened) {
      const ref = this.store._decrementReference(core)
      if (ref) {
        onclose(null)
        continue
      }
      core.close(onclose)
    }
    onclose(null)

    function onclose (err) {
      if (err) error = err
      if (!--pending) {
        self._opened.clear()
        return cb(error)
      }
    }
  }
}

class Corestore extends EventEmitter {
  constructor (storage, opts = {}) {
    super()

    if (typeof storage === 'string') storage = defaultStorage(storage)
    if (typeof storage !== 'function') throw new Error('Storage should be a function or string')
    this.storage = storage
    this.name = opts.name || Buffer.from('default')

    this.opts = opts

    this._replicationStreams = new Map()
    this._references = new Map()
    this._externalCores = new Map()
    this._internalCores = new LRU(opts.cacheSize || 1000)
    this._internalCores.on('evict', ({ key, value: core }) => {
      if (this._externalCores.get(key)) return
      core.close(err => {
        if (err) this.emit('error', err)
      })
    })

    this._graph = new Multigraph(p => this.storage('.graph/' + p))
    this._graph.trie.on('error', err => this.emit('error', err))

    // Generated in _ready
    this._masterKey = null
    this._isReady = false
    this._readyCallback = thunky(this._ready.bind(this))

    this._id = hypercoreCrypto.randomBytes(8)
  }

  _info () {
    return {
      id: this._id.toString('hex'),
      externalCoresSize: this._externalCores.size,
      internalCoresSize: this._internalCores.length,
      referencesSize: this._references.size
    }
  }

  ready (cb) {
    return maybe(cb, new Promise((resolve, reject) => {
      this._readyCallback(err => {
        if (err) return reject(err)
        this._isReady = true
        return resolve()
      })
    }))
  }

  _ready (cb) {
    const keyStorage = this.storage(MASTER_KEY_FILENAME)
    this._graph.ready(err => {
      if (err) return cb(err)
      keyStorage.read(0, 32, (err, key) => {
        if (err) {
          this._masterKey = hypercoreCrypto.randomBytes(32)
          return keyStorage.write(0, this._masterKey, err => {
            if (err) return cb(err)
            keyStorage.close(cb)
          })
        }
        this._masterKey = key
        keyStorage.close(cb)
      })
    })
  }

  _incrementReference (core) {
    const updated = (this._references.get(core) || 0) + 1
    this._references.set(core, updated)
    return updated
  }

  _decrementReference (core) {
    const refs = this._references.get(core)
    const updated = refs - 1
    if (updated < 0) throw new Error('References should not go negative.')
    this._references.set(core, updated)
    if (!updated) this._references.delete(core)
    return updated
  }

  _removeReferences (core) {
    this._references.delete(core)
  }

  _replicateCore (isInitiator, core, mainStream, opts) {
    if (!core) return
    core.ready(function (err) {
      if (err) return
      core.replicate(isInitiator, {
        ...opts,
        stream: mainStream
      })
    })
  }

  _getCacheIndex (coreOpts) {
    if (coreOpts.default) return 'default'
    else if (coreOpts.discoveryKey) return encodeKey(coreOpts.discoveryKey)
    else if (coreOpts.key) {
      const key = decodeKey(coreOpts.key)
      return hypercoreCrypto.discoveryKey(key)
    }
    return null
  }

  _getCachedCore (discoveryKey) {
    const idx = encodeKey(discoveryKey)
    return this._externalCores.get(idx) || this._internalCores.get(idx)
  }

  _cacheCore (core, opts) {
    const idx = encodeKey(core.discoveryKey)
    if (opts && opts.external) this._externalCores.set(idx, core)
    this._internalCores.set(idx, core)
  }

  _uncacheCore (core) {
    const idx = encodeKey(core.discoveryKey)
    this._externalCores.delete(idx)
    this._internalCores.remove(idx)
  }

  _recordDependencies (idx, core, coreOpts, cb) {
    const parents = coreOpts.parents || []
    const batch = []
    for (const parent of parents) {
      const encoded = encodeKey(hypercoreCrypto.discoveryKey(parent))
      batch.push({ type: 'put', from: idx, to: encoded, label: PARENT_LABEL })
      batch.push({ type: 'put', from: encoded, to: idx, label: CHILD_LABEL })
    }
    // Make a self-link to the idx core.
    // When you replicate with a dkey but no children, the core should still replicates.
    batch.push({ type: 'put', from: idx, to: idx, label: CHILD_LABEL })
    return this._graph.batch(batch, cb)
  }

  _getAllDependencies (discoveryKey, label, cb) {
    const deps = []
    const ite = this._graph.iterator({ from: discoveryKey, label })
    ite.next(function onnext (err, pair) {
      if (err) return cb(err)
      if (!pair) return cb(null, deps)
      deps.push(pair.to)
      return ite.next(onnext)
    })
  }

  _generateKeyPair (name) {
    if (typeof name === 'string') name = Buffer.from(name)
    if (!name) name = hypercoreCrypto.randomBytes(32)
    const seed = deriveSeed(NAMESPACE, this._masterKey, name)
    const keyPair = hypercoreCrypto.keyPair(seed)
    const discoveryKey = hypercoreCrypto.discoveryKey(keyPair.publicKey)
    return { name, publicKey: keyPair.publicKey, secretKey: keyPair.secretKey, discoveryKey }
  }

  _generateKeys (coreOpts) {
    if (Buffer.isBuffer(coreOpts)) coreOpts = { key: coreOpts }

    if (coreOpts.keyPair) {
      const publicKey = coreOpts.keyPair.publicKey
      const secretKey = coreOpts.keyPair.secretKey
      return {
        publicKey,
        secretKey,
        discoveryKey: hypercoreCrypto.discoveryKey(publicKey),
        name: null
      }
    }
    if (coreOpts.key) {
      const publicKey = decodeKey(coreOpts.key)
      return {
        publicKey,
        secretKey: null,
        discoveryKey: hypercoreCrypto.discoveryKey(publicKey),
        name: null
      }
    }
    if (coreOpts.default) {
      const name = coreOpts._name || this.name
      return this._generateKeyPair(name)
    }
    if (coreOpts.discoveryKey) {
      const discoveryKey = decodeKey(coreOpts.discoveryKey)
      return {
        publicKey: null,
        secretKey: null,
        discoveryKey,
        name: null
      }
    }
    return this._generateKeyPair(null)
  }

  default (coreOpts = {}) {
    if (Buffer.isBuffer(coreOpts)) coreOpts = { key: coreOpts }
    return this.get({
      ...coreOpts,
      default: true
    })
  }

  namespace (name) {
    if (!name) name = hypercoreCrypto.randomBytes(32)
    return new NamespacedCorestore(this, name)
  }

  get (coreOpts = {}) {
    if (!this._isReady) throw new Error('Corestore.ready must be called before get.')
    const self = this

    const generatedKeys = this._generateKeys(coreOpts)
    const { publicKey, discoveryKey, secretKey } = generatedKeys
    const id = encodeKey(discoveryKey)
    const isInitiator = !!publicKey

    const cached = this._getCachedCore(discoveryKey)
    if (cached) {
      cached.ifAvailable.wait()
      injectIntoReplicationStreams(cached, () => {
        cached.ifAvailable.continue()
      })
      return cached
    }

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

    const core = hypercore(name => {
      if (name === 'key') return keyStorage.key
      if (name === 'secret_key') return keyStorage.secretKey
      return createStorage(name)
    }, publicKey, {
      ...this.opts,
      ...coreOpts,
      createIfMissing: !!publicKey
    })
    this._cacheCore(core, { external: !!publicKey })
    if (!coreOpts.namespaced) this._incrementReference(core)
    core.ifAvailable.wait()

    var errored = false
    core.once('error', onerror)
    core.once('ready', onready)
    core.once('close', onclose)

    return core

    function onready () {
      if (errored) return
      self.emit('feed', core, coreOpts)
      core.removeListener('error', onerror)
      injectIntoReplicationStreams(core, () => {
        core.ifAvailable.continue()
      })
    }

    function onerror (err) {
      errored = true
      core.ifAvailable.continue()
      self._removeReferences(core)
      if (err.unknownKeyPair) {
        // If an error occurs during creation by discovery key, then that core does not exist on disk.
        // TODO: This should not throw, but should propagate somehow.
      }
    }

    function onclose () {
      self._uncacheCore(core)
    }

    function createStorage (name) {
      return self.storage(storageRoot + '/' + name)
    }

    function injectIntoReplicationStreams (core, cb) {
      self._recordDependencies(id, core, coreOpts, err => {
        if (err) return replicationError(err)
        self._getAllDependencies(id, PARENT_LABEL, (err, parents) => {
          if (err) return replicationError(err)
          // Deduplicate repeated parents.
          parents = [...new Set(parents)]
          // Add null to the list of stream keys, as that encapsulates all complete-store replication streams.
          for (const streamKey of [ ...parents, id, null ]) {
            const streams = self._replicationStreams.get(streamKey)
            if (!streams || !streams.length) continue
            for (const { stream, opts } of streams) {
              self._replicateCore(isInitiator, core, stream, { ...opts })
            }
          }
          if (cb) cb(null)
        })
      })

      function replicationError (err) {
        self.emit('replication-error', err)
        if (cb) cb(err)
      }
    }
  }

  replicate (isInitiator, discoveryKey, replicationOpts = {}) {
    if (discoveryKey && (!Buffer.isBuffer(discoveryKey) && (typeof discoveryKey !== 'string'))) {
      return this.replicate(isInitiator, null, discoveryKey)
    }
    const self = this

    var keyString = null
    if (discoveryKey) {
      keyString = encodeKey(discoveryKey)
    }

    const finalOpts = { ...this.opts, ...replicationOpts, ack: true }
    const mainStream = replicationOpts.stream || new HypercoreProtocol(isInitiator, { ...finalOpts })

    var closed = false

    if (discoveryKey) {
      // If a starting key is specified, only inject all active child cores.
      this._getAllDependencies(keyString, CHILD_LABEL, (err, children) => {
        if (err) return this.emit('replication-error', err)

        const rootCore = this._getCachedCore(discoveryKey)
        this._replicateCore(isInitiator, rootCore, mainStream, { ...finalOpts })

        // Deduplicate repeated children.
        children = [...new Set(children)]
        for (const child of children) {
          const activeCore = this._externalCores.get(child)
          if (!activeCore) {
            continue
          }
          this._replicateCore(isInitiator, activeCore, mainStream, { ...finalOpts })
        }
      })
    } else {
      // Otherwise, inject all active cores.
      const cores = replicationOpts.cores || this._externalCores.values()
      for (const activeCore of cores) {
        this._replicateCore(isInitiator, activeCore, mainStream, { ...finalOpts })
      }
    }

    mainStream.on('discovery-key', ondiscoverykey)
    mainStream.on('finish', onclose)
    mainStream.on('end', onclose)
    mainStream.on('close', onclose)

    let streamState = { stream: mainStream, opts: finalOpts }
    var streams = this._replicationStreams.get(keyString)
    if (!streams) {
      streams = []
      this._replicationStreams.set(keyString, streams)
    }
    streams.push(streamState)

    return mainStream

    function ondiscoverykey (dkey) {
      // Get will automatically add the core to all replication streams.
      const passiveCore = self.get({ discoveryKey: dkey })
      if (passiveCore.opened) return

      passiveCore.once('error', onerror)
      passiveCore.ready(() => {
        passiveCore.removeListener('error', onerror)
      })

      function onerror () {
        mainStream.close(dkey)
      }
    }

    function onclose () {
      if (!closed) {
        streams.splice(streams.indexOf(streamState), 1)
        closed = true
      }
    }
  }

  replicateMetadata (isInitiator, opts) {
    return this._graphTrie.replicate(isInitiator, opts)
  }

  close (cb) {
    const self = this
    var remaining = this._externalCores.size + this._internalCores.length + 1
    var closing = false
    var error = null

    for (const [, streams] of this._replicationStreams) {
      for (const stream of streams) {
        stream.destroy()
      }
    }
    for (let core of this._externalCores.values()) {
      if (!core.closed) core.close(onclose)
      else onclose(null)
    }
    for (let idx of this._internalCores.keys) {
      const core = this._internalCores.get(idx)
      if (!core.closed) core.close(onclose)
      else onclose(null)
    }
    onclose()

    function onclose (err) {
      if (err) error = err
      if (!--remaining) return reset(error)
    }

    function reset (err) {
      if (err) error = err
      if (closing) return
      closing = true
      self._graph.close(err => {
        if (!err) err = error
        if (err) return cb(err)
        self._externalCores.clear()
        self._internalCores.clear()
        self._replicationStreams.clear()
        return cb(err)
      })
    }
  }

  list () {
    return new Map([...this._externalCores])
  }
}

function encodeKey (key) {
  return Buffer.isBuffer(key) ? datEncoding.encode(key) : key
}

function decodeKey (key) {
  return (typeof key === 'string') ? datEncoding.decode(key) : key
}

function defaultStorage (dir) {
  return function (name) {
    try {
      var lock = name.endsWith('/bitfield') ? require('fd-lock') : null
    } catch (err) {}
    return raf(name, { directory: dir, lock: lock })
  }
}

module.exports = Corestore
