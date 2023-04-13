const { EventEmitter } = require('events')
const safetyCatch = require('safety-catch')
const crypto = require('hypercore-crypto')
const sodium = require('sodium-universal')
const Hypercore = require('hypercore')
const Xache = require('xache')
const b4a = require('b4a')

const [NS] = crypto.namespace('corestore', 1)
const DEFAULT_NAMESPACE = b4a.alloc(32) // This is meant to be 32 0-bytes

const CORES_DIR = 'cores'
const PRIMARY_KEY_FILE_NAME = 'primary-key'
const USERDATA_NAME_KEY = 'corestore/name'
const USERDATA_NAMESPACE_KEY = 'corestore/namespace'
const POOL_SIZE = 512 // how many open fds to aim for before cycling them

module.exports = class Corestore extends EventEmitter {
  constructor (storage, opts = {}) {
    super()

    const root = opts._root

    this.storage = Hypercore.defaultStorage(storage, { lock: PRIMARY_KEY_FILE_NAME, poolSize: opts.poolSize || POOL_SIZE })
    this.cores = root ? root.cores : new Map()
    this.cache = !!opts.cache
    this.primaryKey = opts.primaryKey || null

    this._keyStorage = null
    this._bootstrap = opts._bootstrap || null
    this._namespace = opts.namespace || DEFAULT_NAMESPACE

    this._root = root || this
    this._replicationStreams = root ? root._replicationStreams : []
    this._overwrite = opts.overwrite === true

    this._sessions = new Set() // sessions for THIS namespace
    this._rootStoreSessions = new Set()

    this._findingPeersCount = 0
    this._findingPeers = []

    if (this._namespace.byteLength !== 32) throw new Error('Namespace must be a 32-byte Buffer or Uint8Array')

    this._closing = null
    this._opening = this._open()
    this._opening.catch(safetyCatch)
  }

  ready () {
    return this._opening
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
      await this._root._opening
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

  async _generateKeys (opts) {
    if (opts._discoveryKey) {
      return {
        keyPair: null,
        auth: null,
        discoveryKey: opts._discoveryKey
      }
    }
    if (!opts.name) {
      return {
        keyPair: {
          publicKey: opts.publicKey,
          secretKey: opts.secretKey
        },
        sign: opts.sign,
        auth: opts.auth,
        discoveryKey: crypto.discoveryKey(opts.publicKey)
      }
    }
    const { publicKey, auth } = await this.createKeyPair(opts.name)
    return {
      keyPair: {
        publicKey,
        secretKey: null
      },
      auth,
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
    const { publicKey, auth } = await this.createKeyPair(b4a.toString(name), namespace)
    if (!b4a.equals(publicKey, core.key)) throw new Error('Stored core key does not match the provided name')

    // TODO: Should Hypercore expose a helper for this, or should preready return keypair/auth?
    core.auth = auth
    core.key = publicKey
    core.writable = true
  }

  async _preload (opts) {
    if (!this.primaryKey) await this._opening

    const { discoveryKey, keyPair, auth } = await this._generateKeys(opts)
    const id = b4a.toString(discoveryKey, 'hex')

    while (this.cores.has(id)) {
      const existing = this.cores.get(id)
      if (existing.opened && !existing.closing) return { from: existing, keyPair, auth }
      if (existing.closing) {
        await existing.close()
      } else {
        await existing.ready().catch(safetyCatch)
      }
    }

    const userData = {}
    if (opts.name) {
      userData[USERDATA_NAME_KEY] = b4a.from(opts.name)
      userData[USERDATA_NAMESPACE_KEY] = this._namespace
    }

    // No more async ticks allowed after this point -- necessary for caching

    const storageRoot = [CORES_DIR, id.slice(0, 2), id.slice(2, 4), id].join('/')
    const core = new Hypercore(p => this.storage(storageRoot + '/' + p), {
      _preready: this._preready.bind(this),
      autoClose: true,
      encryptionKey: opts.encryptionKey || null,
      userData,
      auth,
      cache: opts.cache,
      createIfMissing: opts.createIfMissing === false ? false : !opts._discoveryKey,
      keyPair: keyPair && keyPair.publicKey
        ? {
            publicKey: keyPair.publicKey,
            secretKey: null
          }
        : null
    })

    if (this._root._closing) throw new Error('The corestore is closed')
    this.cores.set(id, core)
    core.ready().then(() => {
      if (core.closing) return // extra safety here as ready is a tick after open
      this._emitCore('core-open', core)
      for (const { stream } of this._replicationStreams) {
        core.replicate(stream, { session: true })
      }
    }, () => {
      this.cores.delete(id)
    })
    core.once('close', () => {
      this._emitCore('core-close', core)
      this.cores.delete(id)
    })
    core.on('conflict', (len, fork, proof) => {
      this.emit('conflict', core, len, fork, proof)
    })

    return { from: core, keyPair, auth }
  }

  async createKeyPair (name, namespace = this._namespace) {
    if (!this.primaryKey) await this._opening

    const keyPair = {
      publicKey: b4a.allocUnsafe(sodium.crypto_sign_PUBLICKEYBYTES),
      secretKey: b4a.alloc(sodium.crypto_sign_SECRETKEYBYTES),
      auth: {
        sign: (msg) => sign(keyPair, msg),
        verify: (signable, signature) => {
          return crypto.verify(signable, signature, keyPair.publicKey)
        }
      }
    }

    const seed = deriveSeed(this.primaryKey, namespace, name)
    sodium.crypto_sign_seed_keypair(keyPair.publicKey, keyPair.secretKey, seed)

    return keyPair
  }

  get (opts = {}) {
    if (this._root._closing) throw new Error('The corestore is closed')
    opts = validateGetOptions(opts)

    if (opts.cache !== false) {
      opts.cache = opts.cache === true || (this.cache && !opts.cache) ? defaultCache() : opts.cache
    }

    const core = new Hypercore(null, {
      ...opts,
      name: null,
      preload: () => this._preload(opts)
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
    }

    core.ready().catch(gc)
    core.once('close', gc)

    return core
  }

  replicate (isInitiator, opts) {
    const isExternal = isStream(isInitiator) || !!(opts && opts.stream)
    const stream = Hypercore.createProtocolStream(isInitiator, {
      ...opts,
      ondiscoverykey: discoveryKey => {
        const core = this.get({ _discoveryKey: discoveryKey })
        return core.ready().catch(safetyCatch)
      }
    })

    for (const core of this.cores.values()) {
      if (!core.opened || core.closing) continue // If the core is not opened, it will be replicated in preload.
      core.replicate(stream, { session: true })
    }

    const streamRecord = { stream, isExternal }
    this._replicationStreams.push(streamRecord)

    stream.once('close', () => {
      this._replicationStreams.splice(this._replicationStreams.indexOf(streamRecord), 1)
    })

    return stream
  }

  namespace (name) {
    if (name instanceof Hypercore) {
      return this.session({ _bootstrap: name })
    }
    return this.session({ namespace: generateNamespace(this._namespace, name) })
  }

  session (opts) {
    const session = new Corestore(this.storage, {
      namespace: this._namespace,
      cache: this.cache,
      _root: this._root,
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
    await this._opening
    this._root._rootStoreSessions.delete(this)
    await this._closeNamespace()
    if (this._root === this) {
      await this._closePrimaryNamespace()
    }
  }

  close () {
    if (this._closing) return this._closing
    this._closing = this._close()
    return this._closing
  }
}

function sign (keyPair, message) {
  if (!keyPair.secretKey) throw new Error('Invalid key pair')
  return crypto.sign(message, keyPair.secretKey)
}

function validateGetOptions (opts) {
  if (b4a.isBuffer(opts)) return { key: opts, publicKey: opts }
  if (opts.key) {
    opts.publicKey = opts.key
  }
  if (opts.keyPair) {
    opts.publicKey = opts.keyPair.publicKey
    opts.secretKey = opts.keyPair.secretKey
  }
  if (opts.name && typeof opts.name !== 'string') throw new Error('name option must be a String')
  if (opts.name && opts.secretKey) throw new Error('Cannot provide both a name and a secret key')
  if (opts.publicKey && !b4a.isBuffer(opts.publicKey)) throw new Error('publicKey option must be a Buffer or Uint8Array')
  if (opts.secretKey && !b4a.isBuffer(opts.secretKey)) throw new Error('secretKey option must be a Buffer or Uint8Array')
  if (!opts._discoveryKey && (!opts.name && !opts.publicKey)) throw new Error('Must provide either a name or a publicKey')
  return opts
}

function generateNamespace (namespace, name) {
  if (!b4a.isBuffer(name)) name = b4a.from(name)
  const out = b4a.allocUnsafe(32)
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
