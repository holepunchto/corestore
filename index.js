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
    this.watching = []
  }

  get size () {
    return this.map.size
  }

  watch (store) {
    if (store.watchIndex !== -1) return
    store.watchIndex = this.watching.push(store) - 1
  }

  unwatch (store) {
    if (store.watchIndex === -1) return
    const head = this.watching.pop()
    if (head !== store) this.watching[(head.watchIndex = store.watchIndex)] = head
    store.watchIndex = -1
  }

  get (id) {
    // we allow you do call this from the outside, so support normal buffers also
    if (b4a.isBuffer(id)) id = b4a.toString(id, 'hex')
    return this.map.get(id) || null
  }

  set (id, core) {
    this.map.set(id, core)
    this.emit('add', core) // TODO: will be removed
    if (this.watching.length > 0) this._emit(core)
  }

  _emit (core) {
    for (let i = this.watching.length - 1; i >= 0; i--) {
      const store = this.watching[i]
      for (const fn of store.watchers) fn(core)
    }
  }

  delete (id, core) {
    this.map.delete(id)
    this.emit('remove', core) // TODO: will be removed
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
    this.corestores = this.root ? this.root.corestores : new Set()
    this.globalCache = this.root ? this.root.globalCache : (opts.globalCache || null)
    this.primaryKey = this.root ? this.root.primaryKey : (opts.primaryKey || null)
    this.ns = opts.namespace || DEFAULT_NAMESPACE

    this.watchers = null
    this.watchIndex = -1

    this.manifestVersion = 1 // just compat

    this._ongcBound = this._ongc.bind(this)

    if (this.root) this.corestores.add(this)

    this.ready().catch(noop)
  }

  watch (fn) {
    if (this.watchers === null) {
      this.watchers = new Set()
      this.cores.watch(this)
    }

    this.watchers.add(fn)
  }

  unwatch (fn) {
    if (this.watchers === null) return

    this.watchers.delete(fn)

    if (this.watchers.size === 0) {
      this.watchers = null
      this.cores.unwatch(this)
    }
  }

  session (opts) {
    this._maybeClosed()
    const root = this.root || this
    return new Corestore(null, { ...opts, root })
  }

  namespace (name, opts) {
    return this.session({ ...opts, namespace: generateNamespace(this.ns, name) })
  }

  getAuth (discoveryKey) {
    return this.storage.getAuth(discoveryKey)
  }

  _ongc (session) {
    if (session.sessions.length === 0) this.sessions.gc(session.id)
  }

  async _getOrSetSeed () {
    const seed = await this.storage.getSeed()
    if (seed !== null) return seed
    return await this.storage.setSeed(this.primaryKey || crypto.randomBytes(32))
  }

  async _open () {
    if (this.root !== null) {
      if (this.root.opened === false) await this.root.ready()
      this.primaryKey = this.root.primaryKey
      return
    }

    const primaryKey = await this._getOrSetSeed()

    if (this.primaryKey === null) {
      this.primaryKey = primaryKey
      return
    }

    if (!b4a.equals(primaryKey, this.primaryKey)) {
      throw new Error('Another corestore is stored here')
    }
  }

  async _close () {
    const closing = []
    const hanging = [...this.sessions]
    for (const sess of hanging) closing.push(sess.close())

    if (this.watchers !== null) this.cores.unwatch(this)

    if (this.root !== null) {
      await Promise.all(closing)
      return
    }

    for (const store of this.corestores) {
      closing.push(store.close())
    }

    await Promise.all(closing)

    const cores = []
    for (const core of this.cores) cores.push(core.close())
    await Promise.all(cores)
    await this.storage.close()
  }

  async _attachMaybe (muxer, discoveryKey) {
    if (this.opened === false) await this.ready()
    if (this.cores.get(toHex(discoveryKey)) === null && !(await this.storage.has(discoveryKey))) return

    const core = this._getCore(discoveryKey, { createIfMissing: false })

    if (!core) return
    if (!core.opened) await core.ready()

    if (!core.replicator.attached(muxer)) {
      core.replicator.attachTo(muxer)
    }
  }

  replicate (isInitiator, opts) {
    this._maybeClosed()

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

  _maybeClosed () {
    if (this.closing || (this.root !== null && this.root.closing)) {
      throw new Error('Corestore is closed')
    }
  }

  get (opts) {
    this._maybeClosed()

    if (b4a.isBuffer(opts) || typeof opts === 'string') opts = { key: opts }
    if (!opts) opts = {}

    const conf = {
      preload: null,
      parent: opts.parent || null,
      sessions: null,
      ongc: null,
      core: null,
      active: opts.active !== false,
      encryption: opts.encryption || null,
      encryptionKey: opts.encryptionKey || null, // back compat, should remove
      isBlockKey: !!opts.isBlockKey, // back compat, should remove
      valueEncoding: opts.valueEncoding || null,
      exclusive: !!opts.exclusive,
      manifest: opts.manifest || null,
      keyPair: opts.keyPair || null,
      onwait: opts.onwait || null,
      wait: opts.wait !== false,
      timeout: opts.timeout || 0,
      draft: !!opts.draft,
      writable: opts.writable
    }

    // name requires us to rt to storage + ready, so needs preload
    // same goes if user has defined async preload obvs
    if (opts.name || opts.preload) {
      conf.preload = this._preload(opts)
      return new Hypercore(null, null, conf)
    }

    // if not not we can sync create it, which just is easier for the
    // upstream user in terms of guarantees (key is there etc etc)
    const core = this._getCore(null, opts)

    conf.core = core
    conf.sessions = this.sessions.get(core.id)
    conf.ongc = this._ongcBound

    return new Hypercore(null, null, conf)
  }

  async createKeyPair (name, ns = this.ns) {
    if (this.opened === false) await this.ready()
    return createKeyPair(this.primaryKey, ns, name)
  }

  async _preload (opts) {
    if (opts.preload) opts = { ...opts, ...(await opts.preload) }
    if (this.opened === false) await this.ready()

    const discoveryKey = opts.name ? await this.storage.getAlias({ name: opts.name, namespace: this.ns }) : null
    this._maybeClosed()

    const core = this._getCore(discoveryKey, opts)

    return {
      parent: opts.parent || null,
      core,
      sessions: this.sessions.get(core.id),
      ongc: this._ongcBound,
      encryption: opts.encryption || null,
      encryptionKey: opts.encryptionKey || null, // back compat, should remove
      isBlockKey: !!opts.isBlockKey // back compat, should remove
    }
  }

  _auth (discoveryKey, opts) {
    const result = {
      keyPair: null,
      key: null,
      discoveryKey,
      manifest: null
    }

    if (opts.name) {
      result.keyPair = createKeyPair(this.primaryKey, this.ns, opts.name)
    } else if (opts.keyPair) {
      result.keyPair = opts.keyPair
    }

    if (opts.manifest) {
      result.manifest = opts.manifest
    } else if (result.keyPair && !result.discoveryKey) {
      result.manifest = { version: 1, signers: [{ publicKey: result.keyPair.publicKey }] }
    }

    if (opts.key) result.key = ID.decode(opts.key)
    else if (result.manifest) result.key = Hypercore.key(result.manifest)

    if (result.discoveryKey) return result

    if (opts.discoveryKey) result.discoveryKey = ID.decode(opts.discoveryKey)
    else if (result.key) result.discoveryKey = crypto.discoveryKey(result.key)
    else throw new Error('Could not derive discovery from input')

    return result
  }

  _hasCore (discoveryKey) {
    return this.cores.get(toHex(discoveryKey)) !== null
  }

  _getCore (discoveryKey, opts) {
    const auth = this._auth(discoveryKey, opts)

    const id = toHex(auth.discoveryKey)
    const existing = this.cores.get(id)
    if (existing) return existing

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
      alias: opts.name ? { name: opts.name, namespace: this.ns } : null
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
