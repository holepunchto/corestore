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

module.exports = class Corestore extends EventEmitter {
  constructor (storage, opts = {}) {
    super()

    this.storage = Hypercore.defaultStorage(storage, { lock: PRIMARY_KEY_FILE_NAME })
    this.cores = opts._cores || new Map()
    this.primaryKey = null
    this.cache = !!opts.cache

    this._keyStorage = null
    this._primaryKey = opts.primaryKey
    this._namespace = opts.namespace || DEFAULT_NAMESPACE
    this._replicationStreams = opts._streams || []
    this._overwrite = opts.overwrite === true

    this._streamSessions = opts._streamSessions || new Map()
    this._sessions = new Set() // sessions for THIS namespace

    this._findingPeersCount = 0
    this._findingPeers = []

    if (this._namespace.byteLength !== 32) throw new Error('Namespace must be a 32-byte Buffer or Uint8Array')

    this._opening = opts._opening ? opts._opening.then(() => this._open()) : this._open()
    this._opening.catch(safetyCatch)
    this.ready = () => this._opening
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

  async _open () {
    if (this._primaryKey) {
      this.primaryKey = await this._primaryKey
      return this.primaryKey
    }
    this._keyStorage = this.storage(PRIMARY_KEY_FILE_NAME)
    this.primaryKey = await new Promise((resolve, reject) => {
      this._keyStorage.stat((err, st) => {
        if (err && err.code !== 'ENOENT') return reject(err)
        if (err || st.size < 32 || this._overwrite) {
          const key = crypto.randomBytes(32)
          return this._keyStorage.write(0, key, err => {
            if (err) return reject(err)
            return resolve(key)
          })
        }
        this._keyStorage.read(0, 32, (err, key) => {
          if (err) return reject(err)
          return resolve(key)
        })
      })
    })
    return this.primaryKey
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
    await this.ready()

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
      createIfMissing: !opts._discoveryKey,
      keyPair: keyPair && keyPair.publicKey
        ? {
            publicKey: keyPair.publicKey,
            secretKey: null
          }
        : null
    })

    this.cores.set(id, core)
    core.ready().then(() => {
      for (const { stream } of this._replicationStreams) {
        const sessions = this._streamSessions.get(stream)
        const session = core.session()
        sessions.push(session)
        core.replicate(stream)
      }
    }, () => {
      this.cores.delete(id)
    })
    core.once('close', () => {
      this.cores.delete(id)
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

    core.once('close', () => {
      // technically better to also clear _findingPeers if we added it,
      // but the lifecycle for those are pretty short so prob not worth the complexity
      // as _decFindingPeers clear them all.
      this._sessions.delete(core)
    })

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

    const sessions = []
    for (const core of this.cores.values()) {
      if (!core.opened) continue // If the core is not opened, it will be replicated in preload.
      const session = core.session()
      sessions.push(session)
      core.replicate(stream)
    }

    const streamRecord = { stream, isExternal }
    this._replicationStreams.push(streamRecord)
    this._streamSessions.set(stream, sessions)

    stream.once('close', () => {
      this._replicationStreams.splice(this._replicationStreams.indexOf(streamRecord), 1)
      this._streamSessions.delete(stream)
      Promise.all(sessions.map(s => s.close())).catch(safetyCatch)
    })
    return stream
  }

  namespace (name) {
    return new Corestore(this.storage, {
      primaryKey: this._opening.then(() => this.primaryKey),
      namespace: generateNamespace(this._namespace, name),
      cache: this.cache,
      _opening: this._opening,
      _cores: this.cores,
      _streams: this._replicationStreams,
      _streamSessions: this._streamSessions
    })
  }

  async _close () {
    await this._opening
    if (!b4a.equals(this._namespace, DEFAULT_NAMESPACE)) {
      // namespaces should not release resources on close
      // TODO: Refactor the namespace close logic to actually close sessions with ref counting
      return
    }
    const closePromises = []
    for (const core of this.cores.values()) {
      closePromises.push(core.close())
    }
    await Promise.allSettled(closePromises)
    for (const { stream, isExternal } of this._replicationStreams) {
      // Only close streams that were created by the Corestore
      if (!isExternal) stream.destroy()
    }
    if (!this._keyStorage) return
    await new Promise((resolve, reject) => {
      this._keyStorage.close(err => {
        if (err) return reject(err)
        return resolve(null)
      })
    })
  }

  close () {
    if (this._closing) return this._closing
    this._closing = this._close()
    this._closing.catch(safetyCatch)
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
