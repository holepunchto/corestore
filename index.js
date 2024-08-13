const safetyCatch = require('safety-catch')
const crypto = require('hypercore-crypto')
const sodium = require('sodium-universal')
const Hypercore = require('hypercore')
const hypercoreId = require('hypercore-id-encoding')
const Xache = require('xache')
const b4a = require('b4a')
const ReadyResource = require('ready-resource')
const RW = require('read-write-mutexify')

const [NS] = crypto.namespace('corestore', 1)
const DEFAULT_NAMESPACE = b4a.alloc(32) // This is meant to be 32 0-bytes

const CORES_DIR = 'cores'
const PRIMARY_KEY_FILE_NAME = 'primary-key'
const USERDATA_NAME_KEY = 'corestore/name'
const USERDATA_NAMESPACE_KEY = 'corestore/namespace'
const POOL_SIZE = 512 // how many open fds to aim for before cycling them
const DEFAULT_MANIFEST = 0 // bump to 1 when this is more widely deployed
const DEFAULT_COMPAT = true

module.exports = class Corestore extends ReadyResource {
  constructor (storage, opts = {}) {
    super()

    const root = opts._root

    this.storage = Hypercore.defaultStorage(storage, { lock: PRIMARY_KEY_FILE_NAME, poolSize: opts.poolSize || POOL_SIZE, rmdir: true })
    this.cores = root ? root.cores : new Map()
    this.cache = !!opts.cache
    this.primaryKey = opts.primaryKey || null
    this.passive = !!opts.passive
    this.manifestVersion = typeof opts.manifestVersion === 'number' ? opts.manifestVersion : (root ? root.manifestVersion : DEFAULT_MANIFEST)
    this.compat = typeof opts.compat === 'boolean' ? opts.compat : (root ? root.compat : DEFAULT_COMPAT)
    this.inflightRange = opts.inflightRange || null
    this.globalCache = opts.globalCache || null

    this._keyStorage = null
    this._bootstrap = opts._bootstrap || null
    this._namespace = opts.namespace || DEFAULT_NAMESPACE
    this._noCoreCache = root ? root._noCoreCache : new Xache({ maxSize: 65536 })

    this._root = root || this
    this._replicationStreams = root ? root._replicationStreams : []
    this._overwrite = opts.overwrite === true
    this._readonly = opts.writable === false
    this._attached = opts._attached || null
    this._notDownloadingLinger = opts.notDownloadingLinger

    this._sessions = new Set() // sessions for THIS namespace
    this._rootStoreSessions = new Set()
    this._locks = root ? root._locks : new Map()

    this._findingPeersCount = 0
    this._findingPeers = []
    this._isCorestore = true

    if (this._namespace.byteLength !== 32) throw new Error('Namespace must be a 32-byte Buffer or Uint8Array')
    this.ready().catch(safetyCatch)
  }

  static isCorestore (obj) {
    return !!(typeof obj === 'object' && obj && obj._isCorestore)
  }

  static from (storage, opts) {
    return this.isCorestore(storage) ? storage : new this(storage, opts)
  }

  // for now just release the lock...
  async suspend () {
    if (this._root !== this) return this._root.suspend()

    await this.ready()

    if (this._keyStorage !== null) {
      await new Promise((resolve, reject) => {
        this._keyStorage.suspend((err) => {
          if (err) return reject(err)
          resolve()
        })
      })
    }
  }

  async resume () {
    if (this._root !== this) return this._root.resume()

    await this.ready()

    if (this._keyStorage !== null) {
      await new Promise((resolve, reject) => {
        this._keyStorage.open((err) => {
          if (err) return reject(err)
          resolve()
        })
      })
    }
  }

  findingPeers () {
    let done = false
    this._incFindingPeers()

    return () => {
      if (done) return
      done = true
      this._decFindingPeers()
    }
  }

  _emitCore (name, core) {
    this.emit(name, core)
    for (const session of this._root._rootStoreSessions) {
      if (session !== this) {
        session.emit(name, core)
      }
    }
    if (this !== this._root) this._root.emit(name, core)
  }

  _incFindingPeers () {
    if (++this._findingPeersCount !== 1) return

    for (const core of this._sessions) {
      this._findingPeers.push(core.findingPeers())
    }
  }

  _decFindingPeers () {
    if (--this._findingPeersCount !== 0) return

    while (this._findingPeers.length > 0) {
      this._findingPeers.pop()()
    }
  }

  async _openNamespaceFromBootstrap () {
    const ns = await this._bootstrap.getUserData(USERDATA_NAMESPACE_KEY)
    if (ns) {
      this._namespace = ns
    }
  }

  async _open () {
    if (this._root !== this) {
      await this._root.ready()
      if (!this.primaryKey) this.primaryKey = this._root.primaryKey
      if (this._bootstrap) await this._openNamespaceFromBootstrap()
      return
    }

    this._keyStorage = this.storage(PRIMARY_KEY_FILE_NAME)

    this.primaryKey = await new Promise((resolve, reject) => {
      this._keyStorage.stat((err, st) => {
        if (err && err.code !== 'ENOENT') return reject(err)
        if (err || st.size < 32 || this._overwrite) {
          const key = this.primaryKey || crypto.randomBytes(32)
          return this._keyStorage.write(0, key, err => {
            if (err) return reject(err)
            return resolve(key)
          })
        }
        this._keyStorage.read(0, 32, (err, key) => {
          if (err) return reject(err)
          if (this.primaryKey) return resolve(this.primaryKey)
          return resolve(key)
        })
      })
    })

    if (this._bootstrap) await this._openNamespaceFromBootstrap()
  }

  async _exists (discoveryKey) {
    const id = b4a.toString(discoveryKey, 'hex')
    const storageRoot = getStorageRoot(id)

    const st = this.storage(storageRoot + '/oplog')

    const exists = await new Promise((resolve) => st.stat((err, st) => resolve(!err && st.size > 0)))
    await new Promise(resolve => st.close(resolve))

    return exists
  }

  async _generateKeys (opts) {
    if (opts._discoveryKey) {
      return {
        manifest: null,
        keyPair: null,
        key: null,
        discoveryKey: opts._discoveryKey
      }
    }

    const keyPair = opts.name
      ? await this.createKeyPair(opts.name)
      : (opts.secretKey)
          ? { secretKey: opts.secretKey, publicKey: opts.publicKey }
          : null

    if (opts.manifest) {
      const key = Hypercore.key(opts.manifest)

      return {
        manifest: opts.manifest,
        keyPair,
        key,
        discoveryKey: crypto.discoveryKey(key)
      }
    }

    if (opts.key) {
      return {
        manifest: null,
        keyPair,
        key: opts.key,
        discoveryKey: crypto.discoveryKey(opts.key)
      }
    }

    const publicKey = opts.publicKey || keyPair.publicKey

    if (opts.compat === false || (opts.compat !== true && !this.compat)) {
      let manifest = { version: this.manifestVersion, signers: [{ publicKey }] } // default manifest
      let key = Hypercore.key(manifest)
      let discoveryKey = crypto.discoveryKey(key)

      if (!(await this._exists(discoveryKey)) && manifest.version !== 0) {
        const manifestV0 = { version: 0, signers: [{ publicKey }] }
        const keyV0 = Hypercore.key(manifestV0)
        const discoveryKeyV0 = crypto.discoveryKey(keyV0)

        if (await this._exists(discoveryKeyV0)) {
          manifest = manifestV0
          key = keyV0
          discoveryKey = discoveryKeyV0
        }
      }

      return {
        manifest,
        keyPair,
        key,
        discoveryKey
      }
    }

    return {
      manifest: null,
      keyPair,
      key: publicKey,
      discoveryKey: crypto.discoveryKey(publicKey)
    }
  }

  _getPrereadyUserData (core, key) {
    // Need to manually read the header values before the Hypercore is ready, hence the ugliness.
    for (const { key: savedKey, value } of core.core.header.userData) {
      if (key === savedKey) return value
    }
    return null
  }

  async _preready (core) {
    const name = this._getPrereadyUserData(core, USERDATA_NAME_KEY)
    if (!name) return

    const namespace = this._getPrereadyUserData(core, USERDATA_NAMESPACE_KEY)
    const keyPair = await this.createKeyPair(b4a.toString(name), namespace)
    core.setKeyPair(keyPair)
  }

  _getLock (id) {
    let rw = this._locks.get(id)

    if (!rw) {
      rw = new RW()
      this._locks.set(id, rw)
    }

    return rw
  }

  async _preload (id, keys, opts) {
    const { manifest, keyPair, key } = keys

    while (this.cores.has(id)) {
      const existing = this.cores.get(id)
      if (existing.opened && !existing.closing) return { from: existing, keyPair, manifest, cache: !!opts.cache }
      if (existing.closing) {
        await existing.close()
      } else {
        await existing.ready().catch(safetyCatch)
      }
    }

    const hasKeyPair = !!(keyPair && keyPair.secretKey)
    const userData = {}
    if (opts.name) {
      userData[USERDATA_NAME_KEY] = b4a.from(opts.name)
      userData[USERDATA_NAMESPACE_KEY] = this._namespace
    }

    // No more async ticks allowed after this point -- necessary for caching

    const storageRoot = getStorageRoot(id)
    const core = new Hypercore(p => this.storage(storageRoot + '/' + p), {
      _preready: this._preready.bind(this),
      notDownloadingLinger: this._notDownloadingLinger,
      inflightRange: this.inflightRange,
      autoClose: true,
      active: false,
      encryptionKey: opts.encryptionKey || null,
      isBlockKey: !!opts.isBlockKey,
      userData,
      manifest,
      key,
      compat: opts.compat,
      cache: opts.cache,
      globalCache: this.globalCache,
      createIfMissing: opts.createIfMissing === false ? false : !opts._discoveryKey,
      keyPair: hasKeyPair ? keyPair : null
    })

    if (this._root.closing) {
      try {
        await core.close()
      } catch {}
      throw new Error('The corestore is closed')
    }

    this.cores.set(id, core)
    this._noCoreCache.delete(id)
    core.ready().then(() => {
      if (core.closing) return // extra safety here as ready is a tick after open
      if (hasKeyPair) core.setKeyPair(keyPair)
      this._emitCore('core-open', core)
      if (this.passive) return

      const ondownloading = () => {
        for (const { stream } of this._replicationStreams) {
          core.replicate(stream, { session: true })
        }
      }
      // when the replicator says we are downloading, answer the call
      core.replicator.ondownloading = ondownloading
      // trigger once if the condition is already true
      if (core.replicator.downloading) ondownloading()
    }, () => {
      this._noCoreCache.set(id, true)
      this.cores.delete(id)
    })
    core.once('close', () => {
      this._emitCore('core-close', core)
      this.cores.delete(id)
    })
    core.on('conflict', (len, fork, proof) => {
      this.emit('conflict', core, len, fork, proof)
    })

    return { from: core, keyPair, manifest, cache: !!opts.cache }
  }

  async createKeyPair (name, namespace = this._namespace) {
    if (!this.opened) await this.ready()

    const keyPair = {
      publicKey: b4a.allocUnsafeSlow(sodium.crypto_sign_PUBLICKEYBYTES),
      secretKey: b4a.alloc(sodium.crypto_sign_SECRETKEYBYTES)
    }

    const seed = deriveSeed(this.primaryKey, namespace, name)
    sodium.crypto_sign_seed_keypair(keyPair.publicKey, keyPair.secretKey, seed)

    return keyPair
  }

  get (opts = {}) {
    if (this.closing || this._root.closing) throw new Error('The corestore is closed')
    opts = validateGetOptions(opts)

    if (opts.cache !== false) {
      opts.cache = opts.cache === true || (this.cache && !opts.cache) ? defaultCache() : opts.cache
    }
    if (this._readonly && opts.writable !== false) {
      opts.writable = false
    }

    let rw = null
    let id = null

    const core = new Hypercore(null, {
      ...opts,
      globalCache: this.globalCache,
      name: null,
      preload: async () => {
        if (opts.preload) opts = { ...opts, ...(await opts.preload()) }
        if (!this.opened) await this.ready()

        const keys = await this._generateKeys(opts)

        id = b4a.toString(keys.discoveryKey, 'hex')
        rw = (opts.exclusive && opts.writable !== false) ? this._getLock(id) : null

        if (rw) await rw.write.lock()
        return await this._preload(id, keys, opts)
      }
    })

    this._sessions.add(core)
    if (this._findingPeersCount > 0) {
      this._findingPeers.push(core.findingPeers())
    }

    const gc = () => {
      // technically better to also clear _findingPeers if we added it,
      // but the lifecycle for those are pretty short so prob not worth the complexity
      // as _decFindingPeers clear them all.
      this._sessions.delete(core)

      if (!rw) return
      rw.write.unlock()
      if (!rw.write.locked) this._locks.delete(id)
    }

    core.ready().catch(gc)
    core.once('close', gc)

    return core
  }

  replicate (isInitiator, opts) {
    const isExternal = isStream(isInitiator) || !!(opts && opts.stream)
    const stream = Hypercore.createProtocolStream(isInitiator, {
      ...opts,
      ondiscoverykey: async discoveryKey => {
        if (this.closing) return

        const id = b4a.toString(discoveryKey, 'hex')
        if (this._noCoreCache.get(id)) return

        const core = this.get({ _discoveryKey: discoveryKey, active: false })

        try {
          await core.ready()
        } catch {
          return
        }

        // remote is asking for the core so we HAVE to answer even if not downloading
        if (!core.closing) core.replicate(stream, { session: true })
        await core.close()
      }
    })

    if (!this.passive) {
      const muxer = stream.noiseStream.userData
      muxer.cork()
      for (const core of this.cores.values()) {
        // If the core is not opened, it will be replicated in preload.
        if (!core.opened || core.closing || !core.replicator.downloading) continue
        core.replicate(stream, { session: true })
      }
      stream.noiseStream.opened.then(() => muxer.uncork())
    }

    const streamRecord = { stream, isExternal }
    this._replicationStreams.push(streamRecord)

    stream.once('close', () => {
      this._replicationStreams.splice(this._replicationStreams.indexOf(streamRecord), 1)
    })

    return stream
  }

  namespace (name, opts) {
    if (name instanceof Hypercore) {
      return this.session({ ...opts, _bootstrap: name })
    }
    return this.session({ ...opts, namespace: generateNamespace(this._namespace, name) })
  }

  session (opts) {
    const session = new Corestore(this.storage, {
      namespace: this._namespace,
      cache: this.cache,
      writable: !this._readonly,
      _attached: opts && opts.detach === false ? this : null,
      _root: this._root,
      inflightRange: this.inflightRange,
      globalCache: this.globalCache,
      ...opts
    })
    if (this === this._root) this._rootStoreSessions.add(session)
    return session
  }

  _closeNamespace () {
    const closePromises = []
    for (const session of this._sessions) {
      closePromises.push(session.close())
    }
    return Promise.allSettled(closePromises)
  }

  async _closePrimaryNamespace () {
    const closePromises = []
    // At this point, the primary namespace is closing.
    for (const { stream, isExternal } of this._replicationStreams) {
      // Only close streams that were created by the Corestore
      if (!isExternal) stream.destroy()
    }
    for (const core of this.cores.values()) {
      closePromises.push(forceClose(core))
    }
    await Promise.allSettled(closePromises)
    await new Promise((resolve, reject) => {
      this._keyStorage.close(err => {
        if (err) return reject(err)
        return resolve(null)
      })
    })
  }

  async _close () {
    this._root._rootStoreSessions.delete(this)

    await this._closeNamespace()

    if (this._root === this) {
      await this._closePrimaryNamespace()
    } else if (this._attached) {
      await this._attached.close()
    }
  }
}

function validateGetOptions (opts) {
  const key = (b4a.isBuffer(opts) || typeof opts === 'string') ? hypercoreId.decode(opts) : null
  if (key) return { key }

  if (opts.key) {
    opts.key = hypercoreId.decode(opts.key)
  }
  if (opts.keyPair) {
    opts.publicKey = opts.keyPair.publicKey
    opts.secretKey = opts.keyPair.secretKey
  }

  if (opts.name && typeof opts.name !== 'string') throw new Error('name option must be a String')
  if (opts.name && opts.secretKey) throw new Error('Cannot provide both a name and a secret key')
  if (opts.publicKey && !b4a.isBuffer(opts.publicKey)) throw new Error('publicKey option must be a Buffer or Uint8Array')
  if (opts.secretKey && !b4a.isBuffer(opts.secretKey)) throw new Error('secretKey option must be a Buffer or Uint8Array')
  if (!opts._discoveryKey && (!opts.name && !opts.publicKey && !opts.manifest && !opts.key && !opts.preload)) throw new Error('Must provide either a name or a publicKey')
  return opts
}

function generateNamespace (namespace, name) {
  if (!b4a.isBuffer(name)) name = b4a.from(name)
  const out = b4a.allocUnsafeSlow(32)
  sodium.crypto_generichash_batch(out, [namespace, name])
  return out
}

function deriveSeed (primaryKey, namespace, name) {
  if (!b4a.isBuffer(name)) name = b4a.from(name)
  const out = b4a.alloc(32)
  sodium.crypto_generichash_batch(out, [NS, namespace, name], primaryKey)
  return out
}

function defaultCache () {
  return new Xache({ maxSize: 65536, maxAge: 0 })
}

function isStream (s) {
  return typeof s === 'object' && s && typeof s.pipe === 'function'
}

async function forceClose (core) {
  await core.ready()
  return Promise.all(core.sessions.map(s => s.close()))
}

function getStorageRoot (id) {
  return CORES_DIR + '/' + id.slice(0, 2) + '/' + id.slice(2, 4) + '/' + id
}
