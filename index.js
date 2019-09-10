const { EventEmitter } = require('events')

const hypercore = require('hypercore')
const crypto = require('hypercore-crypto')
const protocol = require('hypercore-protocol')
const datEncoding = require('dat-encoding')
const hypertrie = require('hypertrie')
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

class Corestore extends EventEmitter {
  constructor (storage, opts = {}) {
    super()

    if (typeof storage === 'string') storage = defaultStorage(storage)
    if (typeof storage !== 'function') throw new Error('Storage should be a function or string')
    this.storage = storage

    this.opts = opts
    this.id = crypto.randomBytes(5)
    this.defaultCore = null
    this.namespace = opts.namespace || 'corestore'

    this._replicationStreams = new Map()
    this._externalCores = new Map()
    this._internalCores = new LRU(opts.cacheSize || 1000)
    this._internalCores.on('evict', ({ key, value: core }) => {
      if (this._externalCores.get(key)) return
      core.close(err => {
        if (err) this.emit('error', err)
      })
    })

    this._graph = new Multigraph(p => this.storage('.graph/' + p))

    // Generated in _ready
    this._masterKey = null
    this._isReady = false
    this._readyCallback = thunky(this._ready.bind(this))
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

  _ready(cb) {
    const self = this
    const keyStorage = this.storage(MASTER_KEY_FILENAME)
    this._graph.ready(err => {
      if (err) return cb(err)
      keyStorage.read(0, 32, (err, key) => {
        if (err) {
          this._masterKey = crypto.randomBytes(32)
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

  _replicateCore (core, mainStream, opts) {
    const self = this

    core.ready(function (err) {
      if (err) return
      // if (mainStream.has(core.key)) return
      for (const feed of mainStream.feeds) { // TODO: expose mainStream.has(key) instead
        if (feed.peer.feed === core) return
      }
      core.replicate({
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
      return crypto.discoveryKey(key)
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
    if (!coreOpts.parents) return process.nextTick(cb, null)
    const batch = []
    for (const parent of coreOpts.parents) {
      const encoded = encodeKey(parent)
      batch.push({ type: 'put', from: idx, to: encoded, label: PARENT_LABEL })
      batch.push({ type: 'put', from: encoded, to: idx, label: CHILD_LABEL })
    }
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
    if (!name) name = crypto.randomBytes(32)
    const seed = deriveSeed(this.namespace, this._masterKey, name)
    const keyPair = crypto.keyPair(seed)
    const discoveryKey = crypto.discoveryKey(keyPair.publicKey)
    return { name, publicKey: keyPair.publicKey, secretKey: keyPair.secretKey, discoveryKey }
  }

  _generateKeys (coreOpts) {
    if (Buffer.isBuffer(coreOpts)) coreOpts = { key: coreOpts }

    if (coreOpts.key) {
      const publicKey = decodeKey(coreOpts.key)
      return {
        publicKey,
        secretKey: null,
        discoveryKey: crypto.discoveryKey(publicKey),
        name: null,
      }
    }
    if (coreOpts.default) {
      const name = Buffer.from('default')
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

  isDefaultSet () {
    return !!this.defaultCore
  }

  getInfo () {
    return {
      id: this.storeId,
      defaultKey: this.defaultCore && this.defaultCore.key,
      discoveryKey: this.defaultCore && this.defaultCore.discoveryKey,
      replicationId: this.defaultCore.id,
      cores: this.cores
    }
  }

  default (coreOpts = {}) {
    if (Buffer.isBuffer(coreOpts)) coreOpts = { key: coreOpts }
    if (this.defaultCore) return this.defaultCore
    this.defaultCore = this.get({
      ...coreOpts,
      default: true
    })
    return this.defaultCore
  }

  get (coreOpts = {}) {
    if (!this._isReady) throw new Error('Corestore.ready must be called before get.')
    const self = this

    const generatedKeys = this._generateKeys(coreOpts)
    const { publicKey, discoveryKey, name, secretKey } = generatedKeys
    const id = encodeKey(discoveryKey)

    const cached = this._getCachedCore(discoveryKey)
    if (cached) {
      injectIntoReplicationStreams(cached)
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
      if (name === 'key')  return keyStorage.key
      if (name === 'secret_key') return keyStorage.secretKey
      return createStorage(name)
    }, publicKey, {
      ...this.opts,
      ...coreOpts,
      createIfMissing: !publicKey
    })
    this._cacheCore(core, { external: !!publicKey })

    var errored = false
    const errorListener = err => {
      if (err.unknownKeyPair) {
        // If an error occurs during creation by discovery key, then that core does not exist on disk.
        return
      }
    }
    core.once('error', errorListener)
    core.once('ready', () => {
      if (errored) return
      this.emit('feed', core, coreOpts)
      core.removeListener('error', errorListener)
      this._recordDependencies(id, core, coreOpts, err => {
        if (err) this.emit('error', err)
        injectIntoReplicationStreams(core)
      })
    })

    return core

    function injectIntoReplicationStreams (core) {
      self._getAllDependencies(id, PARENT_LABEL, (err, parents) => {
        if (err) {
          self.emit('replication-error', err)
          return
        }
        // Add null to the list of parents, as that encapsulates all complete-store replication streams.
        for (const parent of [ ...parents, null ]) {
          const streams = self._replicationStreams.get(parent)
          if (!streams || !streams.length) continue
          for (const { stream, opts } of streams) {
            self._replicateCore(core, stream, { ...opts })
          }
        }
      })
    }

    function createStorage (name) {
      return self.storage(storageRoot + '/' + name)
    }
  }

  replicate (discoveryKey, replicationOpts) {
    if (discoveryKey && (!Buffer.isBuffer(discoveryKey) && (typeof discoveryKey !== 'string'))) {
      return this.replicate(null, discoveryKey)
    }
    const self = this

    var keyString = null
    if (discoveryKey) {
      keyString = encodeKey(discoveryKey)
    }

    if (!discoveryKey && !this.defaultCore && replicationOpts.encrypt) {
      throw new Error('A main core must be specified before replication.')
    }

    const finalOpts = { ...this.opts, ...replicationOpts }
    const mainStream = (this.defaultCore && !discoveryKey)
      ? this.defaultCore.replicate({ ...finalOpts })
      : protocol({ ...finalOpts })

    var closed = false

    if (discoveryKey) {
      // If a starting key is specified, only inject all active child cores.
      this._getAllDependencies(keyString, CHILD_LABEL, (err, children) => {
        if (err) return this.emit('replication-error', err)
        for (const child of children) {
          const activeCore = this._externalCores.get(child)
          if (!activeCore) continue
          this._replicateCore(activeCore, mainStream, { ...finalOpts })
        }
      })
    } else {
      // Otherwise, inject all active cores.
      for (const [ _, activeCore ] of this._externalCores) {
        this._replicateCore(activeCore, mainStream, { ...finalOpts })
      }
    }

    mainStream.on('feed', dkey => {
      // Get will automatically add the core to all replication streams.
      this.get({ discoveryKey: dkey })
    })

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

    function onclose () {
      if (!closed) {
        streams.splice(streams.indexOf(streamState), 1)
        closed = true
      }
    }
  }

  replicateMetadata (opts) {
    return this._graphTrie.replicate(opts)
  }

  close (cb) {
    const self = this
    var remaining = this._externalCores.size + this._internalCores.length
    var closing = false

    for (const [ rootKey, streams ] of this._replicationStreams) {
      for (const stream of streams) {
        stream.destroy()
      }
    }
    for (let [, core] of this._externalCores) {
      if (!core.closed) core.close(onclose)
    }
    for (let key of this._internalCores.keys) {
      const core = this._internalCores.get(key)
      if (!core.closed) core.close(onclose)
    }

    function onclose (err) {
      if (err) return reset(err)
      if (!--remaining) return reset(null)
    }

    function reset (err) {
      if (closing) return
      closing = true
      self._graph.close(err => {
        if (err) return cb(err)
        self._externalCores = new Map()
        self._internalCores.clear()
        self._replicationStreams = new Map()
        return cb(err)
      })
    }
  }

  list () {
    return new Map([...this.cores])
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
    return raf(name, {directory: dir, lock: lock})
  }
}

module.exports = Corestore
