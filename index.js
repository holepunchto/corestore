const { EventEmitter } = require('events')

const hypercore = require('hypercore')
const crypto = require('hypercore-crypto')
const protocol = require('hypercore-protocol')
const datEncoding = require('dat-encoding')
const hypertrie = require('hypertrie')
const LRU = require('lru')
const Multigraph = require('hypertrie-multigraph')

module.exports = (storage, opts) => new Corestore(storage, opts)

const PARENT_LABEL = 'parent'
const CHILD_LABEL = 'child'

class Corestore extends EventEmitter {
  constructor (storage, opts = {}) {
    super()
    this.storage = storage
    this.opts = opts
    this.id = crypto.randomBytes(5)
    this.defaultCore = null

    this._replicationStreams = new Map()
    this._externalCores = new Map()
    this._internalCores = new LRU(opts.cacheSize || 2000)
    this._internalCores.on('evict', ({ key, value: core }) => {
      if (this._externalCores.get(key)) return
      core.close(err => {
        if (err) this.emit('error', err)
      })
    })

    this._graphTrie = hypertrie(p => this.storage('./graph' + p))
    this._graph = new Multigraph(this._graphTrie)
  }


  _optsToIndex (coreOpts) {
    var publicKey, secretKey
    if (coreOpts.default && !coreOpts.key && !coreOpts.secretKey) {
      var idx = 'default'
    } else {
      idx = coreOpts.key || coreOpts.discoveryKey
      if (!idx) {
        // If no key was specified, then we generate a new keypair or use the one that was passed in.
        const keyPair = coreOpts.keyPair || crypto.keyPair()
        publicKey = keyPair.publicKey
        secretKey = keyPair.secretKey
        idx = crypto.discoveryKey(publicKey)
      } else {
        if (!(idx instanceof Buffer)) idx = datEncoding.decode(idx)
        if (coreOpts.key) {
          idx = crypto.discoveryKey(idx)
        }
      }
    }
    return { idx, key: coreOpts.key || publicKey, secretKey }
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

  _getCachedCore (idx) {
    return this._externalCores.get(idx) || this._internalCores.get(idx)
  }

  _cacheCore (core, opts) {
    const self = this

    cache(encodeKey(core.discoveryKey))
    cache(encodeKey(core.key))

    function cache (idx) {
      if (opts && opts.external) self._externalCores.set(idx, core)
      self._internalCores.set(idx, core)
    }
  }

  _uncacheCore (core) {
    const self = this

    uncache(encodeKey(core.discoveryKey))
    uncache(encodeKey(core.key))

    function uncache (idx) {
      self._externalCores.delete(idx)
      self._internalCores.remove(idx)
    }
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
    if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
    if (this.defaultCore) return this.defaultCore

    if (!coreOpts.keyPair && !coreOpts.key) coreOpts.default = true

    this.defaultCore = this.get(coreOpts)
    return this.defaultCore
  }

  get (coreOpts = {}) {
    if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
    const self = this

    const { idx, key, secretKey } = this._optsToIndex(coreOpts)

    const idxString = encodeKey(idx)
    const cached = this._getCachedCore(idxString)
    if (cached) {
      injectIntoReplicationStreams(cached)
      return cached
    }

    var storageRoot = idxString
    if (idxString !== 'default') {
      storageRoot = [storageRoot.slice(0, 2), storageRoot.slice(2, 4), storageRoot].join('/')
    }

    const core = hypercore(filename => this.storage(storageRoot + '/' + filename), key, {
      ...this.opts,
      ...coreOpts,
      createIfMissing: !coreOpts.discoveryKey,
      secretKey: coreOpts.secretKey || secretKey
    })

    var errored = false
    const errorListener = err => {
      if (coreOpts.discoveryKey) {
        // If an error occurs during creation by discovery key, then that core does not exist on disk.
        errored = true
        return
      }
    }
    core.once('error', errorListener)
    core.once('ready', () => {
      if (errored) return
      this.emit('feed', core, coreOpts)
      core.removeListener('error', errorListener)
      this._cacheCore(core, { external: !coreOpts.discoveryKey })
      this._recordDependencies(idxString, core, coreOpts, err => {
        if (err) this.emit('error', err)
        injectIntoReplicationStreams(core)
      })
    })

    return core

    function injectIntoReplicationStreams (core) {
      self._getAllDependencies(idxString, PARENT_LABEL, (err, parents) => {
        if (err) {
          self.emit('replication-error', err)
          return
        }
        // Add null to the list of parents, as that encapsulates all replication streams without starting cores..
        for (const parent of [ ...parents, null ]) {
          const streams = self._replicationStreams.get(parent)
          if (!streams || !streams.length) continue
          for (const { stream, opts } of streams) {
            self._replicateCore(core, stream, { ...opts })
          }
        }
      })
    }
  }

  replicate (discoveryKey, replicationOpts) {
    if (discoveryKey && (!(discoveryKey instanceof Buffer) && (typeof discoveryKey !== 'string'))) return this.replicate(null, discoveryKey)
    const self = this

    var keyString = null
    if (discoveryKey) {
      keyString = encodeKey(discoveryKey)
    }

    if (!discoveryKey && !this.defaultCore && replicationOpts.encrypt) throw new Error('A main core must be specified before replication.')

    const finalOpts = { ...this.opts, ...replicationOpts }
    const mainStream = (this.defaultCore && !discoveryKey)
      ? this.defaultCore.replicate({ ...finalOpts })
      : protocol({ ...finalOpts })

    var closed = false

    if (discoveryKey) {
      // If a starting key is specified, only inject all active child cores.
      this._getAllDependencies(keyString, CHILD_LABEL, (err, children) => {
        if (err) this.emit('replication-error', err)
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

  replicateGraph (opts) {
    return this._graphTrie.replicate(opts)
  }

  close (cb) {
    const self = this
    var remaining = this._externalCores.size + this._internalCores.size

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
      self._externalCores = new Map()
      self._internalCores.clear()
      self._replicationStreams = new Map()
      return cb(err)
    }
  }

  list () {
    return new Map([...this.cores])
  }

}

function encodeKey (key) {
  return (key instanceof Buffer) ? datEncoding.encode(key) : key
}

module.exports.Corestore = Corestore
