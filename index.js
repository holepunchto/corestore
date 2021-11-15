const { EventEmitter } = require('events')
const crypto = require('hypercore-crypto')
const sodium = require('sodium-universal')
const Hypercore = require('hypercore')

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

    this._opening = opts._opening ? opts._opening.then(() => this._open()) : this._open()
    this._opening.catch(noop)
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
    if (opts.discoveryKey) {
      return {
        keyPair: null,
        sign: null,
        discoveryKey: opts.discoveryKey
      }
    }
    if (!opts.name) {
      return {
        keyPair: {
          publicKey: opts.publicKey,
          secretKey: opts.secretKey
        },
        sign: opts.sign,
        discoveryKey: crypto.discoveryKey(opts.publicKey)
      }
    }
    const { publicKey, sign } = await this.keys.createHypercoreKeyPair(opts.name, this._namespace)
    return {
      keyPair: {
        publicKey,
        secretKey: null
      },
      sign,
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
    const { publicKey, sign } = await this.keys.createHypercoreKeyPair(name.toString(), namespace)
    if (!publicKey.equals(core.key)) throw new Error('Stored core key does not match the provided name')

    // TODO: Should Hypercore expose a helper for this, or should preready return keypair/sign?
    core.sign = sign
    core.key = publicKey
    core.writable = true
  }

  async _preload (opts) {
    await this.ready()

    const { discoveryKey, keyPair, sign } = await this._generateKeys(opts)
    const id = discoveryKey.toString('hex')

    while (this.cores.has(id)) {
      const existing = this.cores.get(id)
      if (existing) {
        if (!existing.closing) return { from: existing, keyPair, sign }
        await existing.close()
      }
    }

    const userData = {}
    if (opts.name) {
      userData[USERDATA_NAME_KEY] = Buffer.from(opts.name)
      userData[USERDATA_NAMESPACE_KEY] = this._namespace
    }

    // No more async ticks allowed after this point -- necessary for caching

    const storageRoot = [CORES_DIR, id.slice(0, 2), id.slice(2, 4), id].join('/')
    const core = new Hypercore(p => this.storage(storageRoot + '/' + p), {
      autoClose: true,
      encryptionKey: opts.encryptionKey || null,
      keyPair: {
        publicKey: keyPair.publicKey,
        secretKey: null
      },
      userData,
      sign: null,
      _preready: this._preready.bind(this),
      createIfMissing: !!opts.keyPair
    })

    this.cores.set(id, core)
    for (const stream of this._replicationStreams) {
      core.replicate(stream)
    }
    core.once('close', () => {
      this.cores.delete(id)
    })

    return { from: core, keyPair, sign }
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

  replicate (opts = {}) {
    const stream = isStream(opts) ? opts : (opts.stream || Hypercore.createProtocolStream(opts))
    for (const core of this.cores.values()) {
      core.replicate(stream)
    }
    stream.on('discovery-key', discoveryKey => {
      const core = this.get({ discoveryKey })
      core.ready().then(() => {
        core.replicate(stream)
      }, () => {
        stream.close(discoveryKey)
      })
    })
    this._replicationStreams.push(stream)
    stream.once('close', () => {
      this._replicationStreams.splice(this._replicationStreams.indexOf(stream), 1)
    })
    return stream
  }

  namespace (name) {
    if (!Buffer.isBuffer(name)) name = Buffer.from(name)
    return new Corestore(this.storage, {
      _namespace: generateNamespace(this._namespace, name),
      _opening: this._opening,
      _cores: this.cores,
      _streams: this._replicationStreams,
      keys: this._opening.then(() => this.keys)
    })
  }

  async _close () {
    if (this._closing) return this._closing
    await this._opening
    const closePromises = []
    for (const core of this.cores.values()) {
      closePromises.push(core.close())
    }
    await Promise.allSettled(closePromises)
    for (const stream of this._replicationStreams) {
      stream.destroy()
    }
    await this.keys.close()
  }

  close () {
    if (this._closing) return this._closing
    this._closing = this._close()
    this._closing.catch(noop)
    return this._closing
  }

  static createToken () {
    return KeyManager.createToken()
  }
}

function validateGetOptions (opts) {
  if (Buffer.isBuffer(opts)) return { key: opts, publicKey: opts }
  if (opts.key) {
    opts.publicKey = opts.key
  }
  if (opts.keyPair) {
    opts.publicKey = opts.keyPair.publicKey
    opts.secretKey = opts.keyPair.secretKey
  }
  if (opts.name && typeof opts.name !== 'string') throw new Error('name option must be a String')
  if (opts.name && opts.secretKey) throw new Error('Cannot provide both a name and a secret key')
  if (opts.publicKey && !Buffer.isBuffer(opts.publicKey)) throw new Error('publicKey option must be a Buffer')
  if (opts.secretKey && !Buffer.isBuffer(opts.secretKey)) throw new Error('secretKey option must be a Buffer')
  if (opts.discoveryKey && !Buffer.isBuffer(opts.discoveryKey)) throw new Error('discoveryKey option must be a Buffer')
  if (!opts.name && !opts.publicKey) throw new Error('Must provide either a name or a publicKey')
  return opts
}

function generateNamespace (first, second) {
  if (!Buffer.isBuffer(first)) first = Buffer.from(first)
  if (second && !Buffer.isBuffer(second)) second = Buffer.from(second)
  const out = Buffer.allocUnsafe(32)
  sodium.crypto_generichash(out, second ? Buffer.concat([first, second]) : first)
  return out
}

function isStream (s) {
  return typeof s === 'object' && s && typeof s.pipe === 'function'
}

function noop () {}
