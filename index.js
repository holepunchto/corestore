const b4a = require('b4a')
const Hypercore = require('hypercore')
const ReadyResource = require('ready-resource')
const EventEmitter = require('events')
const sodium = require('sodium-universal')
const crypto = require('hypercore-crypto')
const ID = require('hypercore-id-encoding')

const [NS] = crypto.namespace('corestore', 1)
const DEFAULT_NAMESPACE = b4a.alloc(32) // This is meant to be 32 0-bytes

class StreamTracker {
  constructor () {
    this.records = []
  }

  add (stream, isExternal) {
    const record = { index: 0, stream, isExternal }
    this.records.push(record)
    return record
  }

  remove (record) {
    const popped = this.records.pop()
    if (popped === record) return
    this.records[popped.index = record.index] = popped
  }

  attachAll (core) {
    for (let i = 0; i < this.records.length; i++) {
      const record = this.records[i]
      const muxer = record.stream.noiseStream.userData
      if (!core.replicator.attached(muxer)) core.replicator.attachTo(muxer)
    }
  }

  destroy () {
    // reverse is safer cause we delete mb
    for (let i = this.records.length - 1; i >= 0; i--) {
      const record = this.records[i]
      if (!record.isExternal) record.stream.destroy()
    }
  }
}

class SessionTracker {
  constructor () {
    this.map = new Map()
  }

  get size () {
    return this.map.size
  }

  get (id) {
    const existing = this.map.get(id)
    if (existing !== undefined) return existing
    const fresh = []
    this.map.set(id, fresh)
    return fresh
  }

  gc (id) {
    this.map.delete(id)
  }

  list (id) {
    return id ? (this.map.get(id) || []) : [...this]
  }

  * [Symbol.iterator] () {
    for (const sessions of this.map.values()) {
      yield * sessions[Symbol.iterator]()
    }
  }
}

class CoreTracker extends EventEmitter {
  constructor () {
    super()
    this.map = new Map()
  }

  get size () {
    return this.map.size
  }

  get (id) {
    return this.map.get(id) || null
  }

  set (id, core) {
    this.map.set(id, core)
    this.emit('add', core)
  }

  delete (id, core) {
    this.map.delete(id)
    this.emit('remove', core)
  }

  [Symbol.iterator] () {
    return this.map.values()
  }
}

class Corestore extends ReadyResource {
  constructor (storage, opts = {}) {
    super()

    this.root = opts.root || null
    this.storage = this.root ? this.root.storage : Hypercore.defaultStorage(storage)
    this.streamTracker = this.root ? this.root.streamTracker : new StreamTracker()
    this.cores = this.root ? this.root.cores : new CoreTracker()
    this.sessions = new SessionTracker()
    this.globalCache = this.root ? this.root.globalCache : (opts.globalCache || null)
    this.primaryKey = this.root ? this.root.primaryKey : (opts.primaryKey || null)
    this.ns = opts.namespace || DEFAULT_NAMESPACE

    this.manifestVersion = 1 // just compat

    this._ongcBound = this._ongc.bind(this)

    this.ready().catch(noop)
  }

  session (opts) {
    const root = this.root || this
    return new Corestore(null, { ...opts, root })
  }

  namespace (name, opts) {
    return this.session({ ...opts, namespace: generateNamespace(this.ns, name) })
  }

  _ongc (session) {
    if (session.sessions.length === 0) this.sessions.gc(session.id)
  }

  async _open () {
    if (this.root !== null) {
      if (this.root.opened === false) await this.root.ready()
      this.primaryKey = this.root.primaryKey
      return
    }

    const primaryKey = await this.storage.getLocalSeed()

    if (primaryKey && this.primaryKey === null) {
      this.primaryKey = primaryKey
      return
    }

    if (this.primaryKey === null) {
      this.primaryKey = crypto.randomBytes(32)
    }

    if (primaryKey === null) {
      await this.storage.setLocalSeed(this.primaryKey, true)
      return
    }

    if (b4a.equals(primaryKey, this.primaryKey)) {
      throw new Error('Another corestore is stored here')
    }
  }

  async _close () {
    const sessions = []
    const hanging = [...this.sessions]
    for (const sess of hanging) sessions.push(sess.close())

    await Promise.all(sessions)
    if (this.root !== null) return

    const cores = []
    for (const core of this.cores) cores.push(core.close())
    await Promise.all(cores)
    await this.storage.close()
  }

  async _attachMaybe (muxer, discoveryKey) {
    if (this.opened === false) await this.ready()
    if (this.cores.get(toHex(discoveryKey)) === null && !(await this.storage.has(discoveryKey))) return

    const core = this._getCore({ discoveryKey, createIfMissing: false })

    if (!core) return
    if (!core.opened) await core.ready()

    if (!core.replicator.attached(muxer)) {
      core.replicator.attachTo(muxer)
    }
  }

  replicate (isInitiator, opts) {
    const isExternal = isStream(isInitiator)
    const stream = Hypercore.createProtocolStream(isInitiator, {
      ...opts,
      ondiscoverykey: discoveryKey => {
        if (this.closing) return
        const muxer = stream.noiseStream.userData
        return this._attachMaybe(muxer, discoveryKey)
      }
    })

    if (this.cores.size > 0) {
      const muxer = stream.noiseStream.userData
      const uncork = muxer.uncork.bind(muxer)
      muxer.cork()

      for (const core of this.cores) {
        if (!core.replicator.downloading || core.replicator.attached(muxer) || !core.opened) continue
        core.replicator.attachTo(muxer)
      }

      stream.noiseStream.opened.then(uncork)
    }

    const record = this.streamTracker.add(stream, isExternal)
    stream.once('close', () => this.streamTracker.remove(record))
    return stream
  }

  get (opts) {
    if (b4a.isBuffer(opts) || typeof opts === 'string') opts = { key: opts }
    if (!opts) opts = {}

    const conf = {
      preload: null,
      parent: null,
      sessions: null,
      ongc: null,
      core: null,
      active: opts.active !== false,
      encryptionKey: opts.encryptionKey || null,
      valueEncoding: opts.valueEncoding || null,
      exclusive: !!opts.exclusive,
      manifest: opts.manifest || null,
      keyPair: opts.keyPair || null,
      onwait: opts.onwait || null,
      wait: opts.wait !== false,
      timeout: opts.timeout || 0,
      draft: !!opts.draft
    }

    if (this.opened === false || opts.preload) {
      conf.preload = this._preload(opts)
    } else {
      this._addInternalOptions(opts, conf)
    }

    return new Hypercore(null, null, conf)
  }

  async createKeyPair (name, ns = this.ns) {
    if (this.opened === false) await this.ready()
    return createKeyPair(this.primaryKey, ns, name)
  }

  async _preload (opts) {
    if (opts.preload) opts = { ...opts, ...(await opts.preload) }
    await this.ready()
    const conf = { parent: null, sessions: null, ongc: null, core: null }
    this._addInternalOptions(opts, conf)
    return conf
  }

  _addInternalOptions (opts, conf) {
    const core = this._getCore(opts)
    if (core === null) return

    if (opts.parent) conf.parent = opts.parent
    conf.sessions = this.sessions.get(core.id)
    conf.ongc = this._ongcBound
    conf.core = core
  }

  _auth (opts) {
    const result = {
      keyPair: null,
      key: null,
      discoveryKey: null,
      manifest: null
    }

    if (opts.name) {
      result.keyPair = createKeyPair(this.primaryKey, this.ns, opts.name)
    } else if (opts.keyPair) {
      result.keyPair = opts.keyPair
    }

    if (opts.manifest) {
      result.manifest = opts.manifest
    } else if (result.keyPair) {
      result.manifest = { version: 1, signers: [{ publicKey: result.keyPair.publicKey }] }
    }

    if (opts.key) result.key = ID.decode(opts.key)
    else if (result.manifest) result.key = Hypercore.key(result.manifest)

    if (opts.discoveryKey) result.discoveryKey = opts.discoveryKey
    else if (result.key) result.discoveryKey = crypto.discoveryKey(result.key)

    if (!result.discoveryKey) {
      throw new Error('Could not derive discovery from input')
    }

    return result
  }

  _getCore (opts) {
    const auth = this._auth(opts)

    const id = toHex(auth.discoveryKey)
    const existing = this.cores.get(id)
    if (existing) return existing

    const userData = []

    if (opts.name) userData.push({ key: 'corestore/name', value: b4a.from(opts.name) })
    if (this.ns) userData.push({ key: 'corestore/namespace', value: this.ns })

    const core = Hypercore.createCore(this.storage, {
      eagerUpgrade: true,
      notDownloadingLinger: opts.notDownloadingLinger,
      allowFork: opts.allowFork !== false,
      inflightRange: opts.inflightRange,
      compat: false, // no compat for now :)
      force: opts.force,
      createIfMissing: opts.createIfMissing,
      discoveryKey: auth.discoveryKey,
      overwrite: opts.overwrite,
      key: auth.key,
      keyPair: auth.keyPair,
      legacy: opts.legacy,
      manifest: auth.manifest,
      globalCache: opts.globalCache || this.globalCache || null,
      userData
    })

    core.onidle = () => {
      core.destroy()
      this.cores.delete(id, core)
    }

    core.replicator.ondownloading = () => {
      this.streamTracker.attachAll(core)
    }

    this.cores.set(id, core)
    return core
  }
}

module.exports = Corestore

function isStream (s) {
  return typeof s === 'object' && s && typeof s.pipe === 'function'
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

function createKeyPair (primaryKey, namespace, name) {
  const seed = deriveSeed(primaryKey, namespace, name)
  const buf = b4a.alloc(sodium.crypto_sign_PUBLICKEYBYTES + sodium.crypto_sign_SECRETKEYBYTES)
  const keyPair = {
    publicKey: buf.subarray(0, sodium.crypto_sign_PUBLICKEYBYTES),
    secretKey: buf.subarray(sodium.crypto_sign_PUBLICKEYBYTES)
  }
  sodium.crypto_sign_seed_keypair(keyPair.publicKey, keyPair.secretKey, seed)
  return keyPair
}

function noop () {}

function toHex (discoveryKey) {
  return b4a.toString(discoveryKey, 'hex')
}
