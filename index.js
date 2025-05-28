const b4a = require('b4a')
const Hypercore = require('hypercore')
const ReadyResource = require('ready-resource')
const sodium = require('sodium-universal')
const crypto = require('hypercore-crypto')
const ID = require('hypercore-id-encoding')
const { isAndroid } = require('which-runtime')
const { STORAGE_EMPTY } = require('hypercore-errors')

const auditStore = require('./lib/audit.js')

const [NS] = crypto.namespace('corestore', 1)
const DEFAULT_NAMESPACE = b4a.alloc(32) // This is meant to be 32 0-bytes

class StreamTracker {
  constructor () {
    this.records = []
  }

  add (stream, isExternal) {
    const record = { index: 0, stream, isExternal }
    record.index = this.records.push(record) - 1
    return record
  }

  remove (record) {
    const popped = this.records.pop()
    if (popped === record) return
    this.records[(popped.index = record.index)] = popped
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

class CoreTracker {
  constructor () {
    this.map = new Map()
    this.watching = []

    this._gcing = new Set()
    this._gcInterval = null
    this._gcCycleBound = this._gcCycle.bind(this)
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

  resume (id) {
    const core = this.map.get(id)

    if (!core) return null

    // signal back that we have a closing one stored
    if (core.closing) return core

    if (core.gc) {
      this._gcing.delete(core)
      if (this._gcing.size === 0) this._stopGC()
      core.gc = 0
    }

    return core
  }

  opened (id) {
    const core = this.map.get(id)
    return !!(core && core.opened && !core.closing)
  }

  get (id) {
    // we allow you do call this from the outside, so support normal buffers also
    if (b4a.isBuffer(id)) id = b4a.toString(id, 'hex')
    const core = this.map.get(id)
    if (!core || core.closing) return null
    return core
  }

  set (id, core) {
    this.map.set(id, core)
    if (this.watching.length > 0) this._emit(core)
  }

  _emit (core) {
    for (let i = this.watching.length - 1; i >= 0; i--) {
      const store = this.watching[i]
      for (const fn of store.watchers) fn(core)
    }
  }

  _gc (core) {
    const id = toHex(core.discoveryKey)
    if (this.map.get(id) === core) this.map.delete(id)
  }

  _gcCycle () {
    for (const core of this._gcing) {
      if (++core.gc < 4) continue
      const gc = this._gc.bind(this, core)
      core.close().then(gc, gc)
      this._gcing.delete(core)
    }

    if (this._gcing.size === 0) this._stopGC()
  }

  gc (core) {
    core.gc = 1 // first strike
    this._gcing.add(core)
    if (this._gcing.size === 1) this._startGC()
  }

  _stopGC () {
    clearInterval(this._gcInterval)
    this._gcInterval = null
  }

  _startGC () {
    if (this._gcInterval) return
    this._gcInterval = setInterval(this._gcCycleBound, 2000)
    if (this._gcInterval.unref) this._gcInterval.unref()
  }

  close () {
    this._stopGC()
    this._gcing.clear()

    const all = []
    for (const core of this.map.values()) {
      core.onidle = noop // no reentry
      all.push(core.close())
    }
    this.map.clear()

    return Promise.all(all)
  }

  * [Symbol.iterator] () {
    for (const core of this.map.values()) {
      if (!core.closing) yield core
    }
  }
}

class FindingPeers {
  constructor () {
    this.count = 0
    this.pending = []
  }

  add (core) {
    if (this.count === 0) return
    this.pending.push(core.findingPeers())
  }

  inc (sessions) {
    if (++this.count !== 1) return

    for (const core of sessions) {
      this.pending.push(core.findingPeers())
    }
  }

  dec (sessions) {
    if (--this.count !== 0) return
    while (this.pending.length > 0) this.pending.pop()()
  }
}

class Corestore extends ReadyResource {
  constructor (storage, opts = {}) {
    super()

    this.root = opts.root || null
    this.storage = this.root ? this.root.storage : Hypercore.defaultStorage(storage, { id: opts.id, allowBackup: opts.allowBackup })
    this.streamTracker = this.root ? this.root.streamTracker : new StreamTracker()
    this.cores = this.root ? this.root.cores : new CoreTracker()
    this.sessions = new SessionTracker()
    this.corestores = this.root ? this.root.corestores : new Set()
    this.readOnly = opts.writable === false
    this.globalCache = this.root ? this.root.globalCache : (opts.globalCache || null)
    this.primaryKey = this.root ? this.root.primaryKey : (opts.primaryKey || null)
    this.ns = opts.namespace || DEFAULT_NAMESPACE
    this.manifestVersion = opts.manifestVersion || 1
    this.shouldSuspend = isAndroid ? !!opts.suspend : opts.suspend !== false
    this.active = opts.active !== false

    this.watchers = null
    this.watchIndex = -1

    this._findingPeers = null // here for legacy
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

  findingPeers () {
    if (this._findingPeers === null) this._findingPeers = new FindingPeers()
    this._findingPeers.inc(this.sessions)
    let done = false
    return () => {
      if (done) return
      done = true
      this._findingPeers.dec(this.sessions)
    }
  }

  audit (opts = {}) {
    return auditStore(this, opts)
  }

  async suspend ({ log = noop } = {}) {
    await log('Flushing db...')
    await this.storage.db.flush()
    if (!this.shouldSuspend) return
    await log('Suspending db...')
    await this.storage.db.suspend()
  }

  resume () {
    return this.storage.db.resume()
  }

  session (opts) {
    this._maybeClosed()
    const root = this.root || this
    return new Corestore(null, { manifestVersion: this.manifestVersion, ...opts, root })
  }

  namespace (name, opts) {
    return this.session({ ...opts, namespace: generateNamespace(this.ns, name) })
  }

  list (namespace) {
    return this.storage.createDiscoveryKeyStream(namespace)
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

    await this.cores.close()
    await this.storage.close()
  }

  async _attachMaybe (muxer, discoveryKey) {
    if (this.opened === false) await this.ready()
    if (!this.cores.opened(toHex(discoveryKey)) && !(await this.storage.has(discoveryKey, { ifMigrated: true }))) return
    if (this.closing) return

    const core = this._openCore(discoveryKey, { createIfMissing: false })

    if (!core) return
    if (!core.opened) await core.ready()

    if (!core.replicator.attached(muxer)) {
      core.replicator.attachTo(muxer)
    }

    core.checkIfIdle()
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
        if (!core.replicator.downloading || core.replicator.attached(muxer) || !core.opened || !this.active) continue
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
      writable: opts.writable === undefined && this.readOnly ? false : opts.writable
    }

    // name requires us to rt to storage + ready, so needs preload
    // same goes if user has defined async preload obvs
    if (opts.name || opts.preload) {
      conf.preload = this._preload(opts)
      return this._makeSession(conf)
    }

    if (opts.discoveryKey && !opts.key && !opts.manifest) {
      conf.preload = this._preloadCheckIfExists(opts)
      return this._makeSession(conf)
    }

    // if not not we can sync create it, which just is easier for the
    // upstream user in terms of guarantees (key is there etc etc)
    const core = this._openCore(null, opts)

    conf.core = core
    conf.sessions = this.sessions.get(core.id)
    conf.ongc = this._ongcBound

    return this._makeSession(conf)
  }

  _makeSession (conf) {
    const session = new Hypercore(null, null, conf)
    if (this._findingPeers !== null) this._findingPeers.add(session)
    return session
  }

  async createKeyPair (name, ns = this.ns) {
    if (this.opened === false) await this.ready()
    return createKeyPair(this.primaryKey, ns, name)
  }

  async _preloadCheckIfExists (opts) {
    const has = await this.storage.has(opts.discoveryKey)
    if (!has) throw STORAGE_EMPTY('No Hypercore is stored here')
    return this._preload(opts)
  }

  async _preload (opts) {
    if (opts.preload) opts = { ...opts, ...(await opts.preload) }
    if (this.opened === false) await this.ready()

    const discoveryKey = opts.name ? await this.storage.getAlias({ name: opts.name, namespace: this.ns }) : null
    this._maybeClosed()

    const core = this._openCore(discoveryKey, opts)

    return {
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
      result.manifest = { version: this.manifestVersion, signers: [{ publicKey: result.keyPair.publicKey }] }
    }

    if (opts.key) result.key = ID.decode(opts.key)
    else if (result.manifest) result.key = Hypercore.key(result.manifest)

    if (result.discoveryKey) return result

    if (opts.discoveryKey) result.discoveryKey = ID.decode(opts.discoveryKey)
    else if (result.key) result.discoveryKey = crypto.discoveryKey(result.key)
    else throw new Error('Could not derive discovery from input')

    return result
  }

  _openCore (discoveryKey, opts) {
    const auth = this._auth(discoveryKey, opts)

    const id = toHex(auth.discoveryKey)
    const existing = this.cores.resume(id)
    if (existing && !existing.closing) return existing

    const core = Hypercore.createCore(this.storage, {
      preopen: (existing && existing.opened) ? existing.closing : null, // always wait for the prev one to close first in any case...
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
      this.cores.gc(core)
    }

    core.replicator.ondownloading = () => {
      if (this.active) this.streamTracker.attachAll(core)
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
