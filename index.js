const { EventEmitter } = require('events')
const safetyCatch = require('safety-catch')
const crypto = require('hypercore-crypto')
const sodium = require('sodium-universal')
const Hypercore = require('hypercore')
const b4a = require('b4a')

const KeyManager = require('./lib/keys')

const CORES_DIR = 'cores'
const PROFILES_DIR = 'profiles'
const USERDATA_NAME_KEY = '@corestore/name'
const USERDATA_NAMESPACE_KEY = '@corestore/namespace'
const DEFAULT_NAMESPACE = generateNamespace('@corestore/default')

module.exports = class Corestore extends EventEmitter {
  constructor (storage, opts = {}) {
    super()

    this.storage = Hypercore.defaultStorage(storage, { lock: PROFILES_DIR + '/default' })

    this.cores = opts._cores || new Map()
    this.keys = opts.keys

    this._namespace = opts._namespace || DEFAULT_NAMESPACE
    this._replicationStreams = opts._streams || []
    this._streamSessions = opts._streamSessions || new Map()

    this._opening = opts._opening ? opts._opening.then(() => this._open()) : this._open()
    this._opening.catch(safetyCatch)
    this.ready = () => this._opening
  }

  async _open () {
    if (this.keys) {
      this.keys = await this.keys // opts.keys can be a Promise that resolves to a KeyManager
    } else {
      this.keys = await KeyManager.fromStorage(p => this.storage(PROFILES_DIR + '/' + p))
    }
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
    const { publicKey, auth } = await this.keys.createHypercoreKeyPair(opts.name, this._namespace)
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
    for (const { key: savedKey, value } of core.core.header.userData) {
      if (key === savedKey) return value
    }
    return null
  }

  async _preready (core) {
    const name = this._getPrereadyUserData(core, USERDATA_NAME_KEY)
    if (!name) return

    const namespace = this._getPrereadyUserData(core, USERDATA_NAMESPACE_KEY)
    const { publicKey, auth } = await this.keys.createHypercoreKeyPair(b4a.toString(name), namespace)
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
      if (!existing.opened) {
        await existing.ready().catch(safetyCatch)
      } else if (existing.closing) {
        await existing.close()
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

  get (opts = {}) {
    opts = validateGetOptions(opts)
    const core = new Hypercore(null, {
      ...opts,
      name: null,
      preload: () => this._preload(opts)
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
    if (!b4a.isBuffer(name)) name = b4a.from(name)
    return new Corestore(this.storage, {
      _namespace: generateNamespace(this._namespace, name),
      _opening: this._opening,
      _cores: this.cores,
      _streams: this._replicationStreams,
      _streamSessions: this._streamSessions,
      keys: this._opening.then(() => this.keys)
    })
  }

  async _close () {
    await this._opening
    if (!this._namespace.equals(DEFAULT_NAMESPACE)) return // namespaces should not release resources on close
    const closePromises = []
    for (const core of this.cores.values()) {
      closePromises.push(core.close())
    }
    await Promise.allSettled(closePromises)
    for (const { stream, isExternal } of this._replicationStreams) {
      // Only close streams that were created by the Corestore
      if (!isExternal) stream.destroy()
    }
    await this.keys.close()
  }

  close () {
    if (this._closing) return this._closing
    this._closing = this._close()
    this._closing.catch(safetyCatch)
    return this._closing
  }

  static createToken () {
    return KeyManager.createToken()
  }
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

function generateNamespace (first, second) {
  if (!b4a.isBuffer(first)) first = b4a.from(first)
  if (second && !b4a.isBuffer(second)) second = b4a.from(second)
  const out = b4a.allocUnsafe(32)
  sodium.crypto_generichash(out, second ? b4a.concat([first, second]) : first)
  return out
}

function isStream (s) {
  return typeof s === 'object' && s && typeof s.pipe === 'function'
}
