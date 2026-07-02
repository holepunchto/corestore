const b4a = require('b4a')
const Hypercore = require('hypercore')
const ReadyResource = require('ready-resource')
const sodium = require('sodium-universal')
const crypto = require('hypercore-crypto')
const ID = require('hypercore-id-encoding')
const { isAndroid } = require('which-runtime')
const { STORAGE_EMPTY, ASSERTION } = require('hypercore-errors')

const auditStore = require('./lib/audit.js')
const GroupNotifyHandle = require('./lib/notify.js')

const [NS] = crypto.namespace('corestore', 1)
const DEFAULT_NAMESPACE = b4a.alloc(32) // This is meant to be 32 0-bytes

/**
 * Options for the {@link Corestore} constructor. Any other options are
 * forwarded to the underlying `hypercore-storage` instance.
 * @typedef {Object} CorestoreOptions
 * @property {Buffer} [primaryKey] - The 32-byte master key from which all writable named cores are derived. Generated and persisted automatically when omitted.
 * @property {boolean} [writable=true] - Set to `false` to open the store read-only; loaded cores default to read-only too.
 * @property {boolean} [readOnly=false] - Open the backing storage in read-only mode.
 * @property {boolean} [active=true] - Whether loaded cores are automatically attached to active replication streams.
 * @property {boolean} [unsafe=false] - Acknowledge that passing `primaryKey` directly is unsafe; required when `primaryKey` is set on a root store.
 */

class StreamTracker {
  constructor() {
    this.records = []
  }

  add(stream, isExternal) {
    const record = { index: 0, stream, isExternal }
    record.index = this.records.push(record) - 1
    return record
  }

  remove(record) {
    const popped = this.records.pop()
    if (popped === record) return
    this.records[(popped.index = record.index)] = popped
  }

  attachAll(core) {
    for (let i = 0; i < this.records.length; i++) {
      const record = this.records[i]
      const muxer = record.stream.noiseStream.userData
      if (!core.replicator.attached(muxer)) core.replicator.attachTo(muxer)
    }
  }

  destroy() {
    // reverse is safer cause we delete mb
    for (let i = this.records.length - 1; i >= 0; i--) {
      const record = this.records[i]
      if (!record.isExternal) record.stream.destroy()
    }
  }
}

class SessionTracker {
  constructor() {
    this.map = new Map()
  }

  get size() {
    return this.map.size
  }

  get(id) {
    const existing = this.map.get(id)
    if (existing !== undefined) return existing
    const fresh = []
    this.map.set(id, fresh)
    return fresh
  }

  gc(id) {
    this.map.delete(id)
  }

  list(id) {
    return id ? this.map.get(id) || [] : [...this]
  }

  *[Symbol.iterator]() {
    for (const sessions of this.map.values()) {
      yield* sessions[Symbol.iterator]()
    }
  }
}

class CoreTracker {
  constructor() {
    this.map = new Map()
    this.watching = []

    this._gcing = new Set()
    this._gcInterval = null
    this._gcCycleBound = this._gcCycle.bind(this)
  }

  get size() {
    return this.map.size
  }

  watch(store) {
    if (store.watchIndex !== -1) return
    store.watchIndex = this.watching.push(store) - 1
  }

  unwatch(store) {
    if (store.watchIndex === -1) return
    const head = this.watching.pop()
    if (head !== store) this.watching[(head.watchIndex = store.watchIndex)] = head
    store.watchIndex = -1
  }

  resume(id) {
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

  opened(id) {
    const core = this.map.get(id)
    return !!(core && core.opened && !core.closing)
  }

  get(id) {
    // we allow you do call this from the outside, so support normal buffers also
    if (b4a.isBuffer(id)) id = b4a.toString(id, 'hex')
    const core = this.map.get(id)
    if (!core || core.closing) return null
    return core
  }

  set(id, core) {
    this.map.set(id, core)
    if (this.watching.length > 0) this._emit(core)
  }

  _emit(core) {
    for (let i = this.watching.length - 1; i >= 0; i--) {
      const store = this.watching[i]
      for (const fn of store.watchers) fn(core)
    }
  }

  _gc(core) {
    const id = toHex(core.discoveryKey)
    if (this.map.get(id) === core) this.map.delete(id)
  }

  _gcCycle() {
    for (const core of this._gcing) {
      if (++core.gc < 4) continue
      const gc = this._gc.bind(this, core)
      core.close().then(gc, gc)
      this._gcing.delete(core)
    }

    if (this._gcing.size === 0) this._stopGC()
  }

  gc(core) {
    core.gc = 1 // first strike
    this._gcing.add(core)
    if (this._gcing.size === 1) this._startGC()
  }

  _stopGC() {
    clearInterval(this._gcInterval)
    this._gcInterval = null
  }

  _startGC() {
    if (this._gcInterval) return
    this._gcInterval = setInterval(this._gcCycleBound, 2000)
    if (this._gcInterval.unref) this._gcInterval.unref()
  }

  close() {
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

  *[Symbol.iterator]() {
    for (const core of this.map.values()) {
      if (!core.closing) yield core
    }
  }
}

class FindingPeers {
  constructor() {
    this.count = 0
    this.pending = []
    this.destroyed = false
  }

  add(core) {
    if (this.count === 0) return
    this.pending.push(core.findingPeers())
  }

  inc(sessions) {
    if (this.destroyed || ++this.count !== 1) return

    for (const core of sessions) {
      this.pending.push(core.findingPeers())
    }
  }

  dec(sessions) {
    if (this.destroyed || --this.count !== 0) return
    while (this.pending.length > 0) this.pending.pop()()
  }

  destroy() {
    this.destroyed = true
    while (this.pending.length > 0) this.pending.pop()()
  }
}

class Corestore extends ReadyResource {
  /**
   * Create a new Corestore instance.
   * @param {string|object} storage - can be either a `hypercore-storage` instance or a string.
   * @param {CorestoreOptions} [opts] - Store options.
   * @throws {ASSERTION} if `primaryKey` is set on a root store without `unsafe: true`.
   * @example
   * {
   *   primaryKey: null, // The primary key to use as the master key for key derivation.
   *   writable: true,
   * }
   */
  constructor(storage, opts = {}) {
    super()

    this.root = opts.root || null
    /**
     * The backing storage adapter used for aliases, seeds, and Hypercore data
     * files.
     * @type {object}
     */
    this.storage = this.root
      ? this.root.storage
      : Hypercore.defaultStorage(storage, {
          id: opts.id,
          allowBackup: opts.allowBackup,
          readOnly: opts.readOnly,
          wait: opts.wait
        })
    this.streamTracker = this.root ? this.root.streamTracker : new StreamTracker()
    this.cores = this.root ? this.root.cores : new CoreTracker()
    this.sessions = new SessionTracker()
    this.corestores = this.root ? this.root.corestores : new Set()
    /**
     * `true` when the store was opened without write access.
     * @type {boolean}
     */
    this.readOnly = opts.writable === false || !!opts.readOnly
    this.globalCache = this.root ? this.root.globalCache : opts.globalCache || null
    /**
     * The 32-byte primary key used to deterministically derive writable named
     * cores and namespaced key pairs.
     * @type {Buffer}
     */
    this.primaryKey = this.root ? this.root.primaryKey : opts.primaryKey || null
    this.ns = opts.namespace || DEFAULT_NAMESPACE
    /**
     * The Hypercore manifest version used when Corestore creates new writable
     * cores.
     * @type {number}
     */
    this.manifestVersion = opts.manifestVersion || 1
    this.shouldSuspend = isAndroid ? !!opts.suspend : opts.suspend !== false
    /**
     * `true` when the store should automatically attach loaded cores to active
     * replication streams.
     * @type {boolean}
     */
    this.active = opts.active !== false

    this.watchers = null
    this.watchIndex = -1

    this._groupNotifiers = new Map() // group notifications
    this._findingPeers = null // here for legacy
    this._ongcBound = this._ongc.bind(this)
    this._onGroupActiveBound = this._onGroupActive.bind(this)

    if (opts.primaryKey && !this.root && !opts.unsafe) {
      throw ASSERTION(
        'Passing the primary key is unsafe unless you know what you are doing. Set unsafe: true to acknowledge that'
      )
    }

    this.ready().catch(noop)
  }

  /**
   * Register a callback called when new Hypercores are opened. `core` is the
   * internal core for the opened Hypercore. It can be used to create weak
   * references to a Hypercore like so:
   * @param {Function} fn - Callback invoked with the internal `core` each time a Hypercore is opened.
   * @returns {void}
   * @example
   * store.watch(function (core) {
   *   const weakCore = new Hypercore({ core, weak: true })
   * })
   */
  watch(fn) {
    if (this.watchers === null) {
      this.watchers = new Set()
      this.cores.watch(this)
    }

    this.watchers.add(fn)
  }

  /**
   * Unregister a callback used with `store.watch(callback)` so it no longer
   * fires.
   * @param {Function} fn - The callback previously passed to `store.watch()`.
   * @returns {void}
   * @example
   * store.unwatch(onCore)
   */
  unwatch(fn) {
    if (this.watchers === null) return

    this.watchers.delete(fn)

    if (this.watchers.size === 0) {
      this.watchers = null
      this.cores.unwatch(this)
    }
  }

  _onGroupActive(topic, update) {
    this.emit('group-active', topic, update)

    const topicHex = b4a.toString(topic, 'hex')
    const handles = this._groupNotifiers.get(topicHex)
    if (!handles) return
    for (const handle of handles) handle.emit('update', update)
  }

  /**
   * Get a `handle` for updates from all `hypercore`s with the group `topic` set.
   * @param {Buffer} topic - The 32-byte group topic to watch.
   * @returns {GroupNotifyHandle} A handle that emits `update` when a core in the group changes.
   * @example
   * const handle = store.notifyGroup(topic)
   * handle.on('update', () => console.log('group changed'))
   */
  notifyGroup(topic) {
    const topicHex = b4a.toString(topic, 'hex')
    let handles = this._groupNotifiers.get(topicHex)

    if (!handles) {
      handles = []
      this._groupNotifiers.set(topicHex, handles)
    }

    const handle = new GroupNotifyHandle(this, topic)
    const length = handles.push(handle)
    handle.index = length - 1

    return handle
  }

  _removeGroupNotify(handle) {
    if (handle.index === -1) return

    const topicHex = b4a.toString(handle._topic, 'hex')
    const handles = this._groupNotifiers.get(topicHex)
    if (!handles) return

    const popped = handles.pop()
    if (popped !== handle) handles[(popped.index = handle.index)] = popped
    if (handles.length === 0) this._groupNotifiers.delete(topicHex)
    handle.index = -1
  }

  /**
   * A completion callback. Call it after the current peer-discovery pass
   * finishes so pending update waits on loaded cores can unblock.
   * @returns {Function} A `done` function to call once peer discovery has finished.
   * @example
   * const done = store.findingPeers()
   * swarm.flush().then(done, done)
   */
  findingPeers() {
    if (this._findingPeers === null) this._findingPeers = new FindingPeers()
    this._findingPeers.inc(this.sessions)
    let done = false
    return () => {
      if (done || !this._findingPeers) return
      done = true
      this._findingPeers.dec(this.sessions)
    }
  }

  /**
   * An audit report describing the state of the backing store and any detected
   * inconsistencies.
   * @param {object} [opts] - Audit options (e.g. `dryRun`).
   * @returns {AsyncGenerator<object>} Yields a result per core as it is audited.
   * @example
   * for await (const report of store.audit()) console.log(report)
   */
  audit(opts = {}) {
    return auditStore(this, opts)
  }

  /**
   * Suspend the underlying storage for the Corestore.
   * @param {object} [options] - Suspend options.
   * @returns {Promise<void>} Resolves once the storage is flushed and suspended.
   * @example
   * await store.suspend()
   */
  async suspend({ log = noop } = {}) {
    await log('Flushing db...')
    // If readOnly we don't need to flush
    if (!this.storage.readOnly) {
      await this.storage.db.flush()
      await log('Flusing db completed')
    }
    if (!this.shouldSuspend) return
    await log('Suspending db...')
    await this.storage.suspend({ log })
    await log('Suspending db completed')
  }

  /**
   * Resume a suspended Corestore.
   * @returns {Promise<void>} Resolves once the storage is resumed.
   * @example
   * await store.resume()
   */
  resume() {
    return this.storage.resume()
  }

  /**
   * Create a new Corestore session. Closing a session will close all cores made
   * from this session.
   * @param {object} opts - Session options (forwarded to the new Corestore).
   * @returns {Corestore} A new Corestore session sharing the same storage.
   * @example
   * const session = store.session()
   */
  session(opts) {
    this._maybeClosed()
    const root = this.root || this
    return new Corestore(null, {
      manifestVersion: this.manifestVersion,
      ...opts,
      root
    })
  }

  /**
   * Create a new namespaced Corestore session. Namespacing is useful if you're
   * going to be sharing a single Corestore instance between many applications or
   * components, as it prevents name collisions.
   * @param {string|Buffer} name - Namespace name; combined with the current namespace to derive a new one.
   * @param {object} opts - Session options (forwarded to the new Corestore).
   * @returns {Corestore} A new namespaced Corestore session.
   * @example
   * const ns1 = store.namespace('a')
   * const ns2 = ns1.namespace('b')
   * const core1 = ns1.get({ name: 'main' }) // These will load different Hypercores
   * const core2 = ns2.get({ name: 'main' })
   */
  namespace(name, opts) {
    return this.session({
      ...opts,
      namespace: generateNamespace(this.ns, name)
    })
  }

  /**
   * Creates a discovery key stream of all cores within a namespace or all cores
   * in general if no namespace is provided.
   * @param {Buffer} [namespace] - to scope to; omit to list all cores.
   * @returns {Readable} A stream of discovery keys.
   * @example
   * for await (const discoveryKey of store.list()) console.log(discoveryKey)
   */
  list(namespace) {
    return this.storage.createDiscoveryKeyStream(namespace)
  }

  /**
   * The storage-layer auth metadata for that core, including manifest details
   * when available.
   * @param {Buffer} discoveryKey - Discovery key of the core to look up.
   * @returns {Promise<object|null>} The auth record, or `null` if not stored.
   * @example
   * const auth = await store.getAuth(discoveryKey)
   */
  getAuth(discoveryKey) {
    return this.storage.getAuth(discoveryKey)
  }

  _ongc(session) {
    if (session.sessions.length === 0) this.sessions.gc(session.id)
  }

  async _getOrSetSeed() {
    const seed = await this.storage.getSeed()
    if (seed !== null) return seed
    return await this.storage.setSeed(this.primaryKey || crypto.randomBytes(32))
  }

  async _open() {
    if (this.root !== null) {
      this.corestores.add(this)
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

  async _close() {
    const closing = []
    const hanging = [...this.sessions]
    for (const sess of hanging) closing.push(sess.close())

    if (this.watchers !== null) {
      this.cores.unwatch(this)
      this.watchers = null
    }

    if (this._findingPeers !== null) {
      this._findingPeers.destroy()
      this._findingPeers = null
    }

    if (this.root !== null) {
      await Promise.all(closing)
      this.corestores.delete(this)
      return
    }

    for (const store of this.corestores) {
      closing.push(store.close())
    }

    await Promise.all(closing)

    await this.cores.close()
    await this.storage.close()
  }

  async _attachMaybe(muxer, discoveryKey) {
    if (this.opened === false) await this.ready()
    if (
      !this.cores.opened(toHex(discoveryKey)) &&
      !(await this.storage.hasCore(discoveryKey, { ifMigrated: true }))
    ) {
      return
    }
    if (this.closing) return

    const core = this._openCore(discoveryKey, { createIfMissing: false })

    if (!core) return
    if (!core.opened) await core.ready()

    if (!core.replicator.attached(muxer)) {
      core.replicator.attachTo(muxer)
    }

    core.checkIfIdle()
  }

  _shouldReplicate(core, muxer) {
    return (
      core.replicator.downloading && !core.replicator.attached(muxer) && core.opened && this.active
    )
  }

  /**
   * Creates a replication stream that's capable of replicating all Hypercores
   * that are managed by the Corestore, assuming the remote peer has the correct
   * capabilities.
   * @param {boolean|Stream} isInitiator - `true`/`false` to initiate, or an existing stream/connection to replicate over.
   * @param {object} opts - Replication options, forwarded to the protocol stream.
   * @returns {Stream} The replication stream.
   * @example
   * const swarm = new Hyperswarm()
   *
   * // join the relevant topic
   * swarm.join(...)
   *
   * // simply pass the connection stream to corestore
   * swarm.on('connection', (connection) => store.replicate(connection))
   */
  replicate(isInitiator, opts) {
    this._maybeClosed()

    const isExternal = isStream(isInitiator)
    const stream = Hypercore.createProtocolStream(isInitiator, {
      ...opts,
      ondiscoverykey: (discoveryKey) => {
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
        if (!this._shouldReplicate(core, muxer)) {
          continue
        }
        core.replicator.attachTo(muxer)
      }

      stream.noiseStream.opened.then(uncork)
    }

    const record = this.streamTracker.add(stream, isExternal)
    stream.once('close', () => this.streamTracker.remove(record))
    return stream
  }

  _maybeClosed() {
    if (this.closing || (this.root !== null && this.root.closing)) {
      throw new Error('Corestore is closed')
    }
  }

  /**
   * A reopened Hypercore whose manifest is converted into a static
   * prologue-based manifest for the current data snapshot.
   * @param {Hypercore} core - The source core to snapshot. Must have data.
   * @param {object} opts - Options forwarded to `store.get()` for the new core.
   * @returns {Promise<Hypercore>} The new static Hypercore.
   * @throws if the core has no data to staticify.
   * @example
   * const staticCore = await store.staticify(core)
   */
  async staticify(core, opts) {
    if (!this.opened) await this.ready()
    if (!core.opened) await core.ready()

    const rx = core.state.storage.read()

    const headPromise = rx.getHead()
    const authPromise = rx.getAuth()

    rx.tryFlush()

    const [head, auth] = await Promise.all([headPromise, authPromise])
    if (!head || head.length === 0) throw new Error('Core must have data')

    const prologue = {
      length: head.length,
      hash: head.rootHash
    }

    const manifest = {
      version: 1,
      hash: auth.manifest.hash,
      quorum: 0,
      signers: [],
      prologue
    }

    const c = {
      key: null,
      discoveryKey: null,
      manifest,
      core: core.state.storage.core
    }

    c.key = Hypercore.key(c.manifest)
    c.discoveryKey = Hypercore.discoveryKey(c.key)

    await this.storage.createCore(c)

    const staticCore = this.get({ ...opts, key: c.key })
    await staticCore.ready()
    return staticCore
  }

  /**
   * Loads a Hypercore, either by name (if the `name` option is provided), or
   * from the provided key (if the first argument is a Buffer or String with
   * hex/z32 key, or if the `key` options is set).
   * @param {Buffer|string|object} opts - A key (Buffer/hex/z32 string), or an options object with `name`, `key`, or `discoveryKey` plus Hypercore options.
   * @returns {Hypercore} A Hypercore session for the requested core.
   * @example
   * const core = store.get({ name: 'my-core' })
   */
  get(opts) {
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
      pushOnly: opts.pushOnly === true,
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

  _makeSession(conf) {
    const session = new Hypercore(null, null, conf)

    if (this._findingPeers !== null) this._findingPeers.add(session)
    return session
  }

  /**
   * Generate a key pair seeded with the Corestore's primary key using a `name`
   * and a `ns` aka namespace. `ns` defaults to the current namespace.
   * @param {string} name - to derive the key pair from.
   * @param {Buffer} [ns]
   * @returns {Promise<{publicKey: Buffer, secretKey: Buffer}>} The derived key pair.
   * @example
   * const keyPair = await store.createKeyPair('signer')
   */
  async createKeyPair(name, ns = this.ns) {
    if (this.opened === false) await this.ready()
    return createKeyPair(this.primaryKey, ns, name)
  }

  async _preloadCheckIfExists(opts) {
    const has = await this.storage.hasCore(opts.discoveryKey)
    if (!has) throw STORAGE_EMPTY('No Hypercore is stored here')
    return this._preload(opts)
  }

  async _preload(opts) {
    if (opts.preload) opts = { ...opts, ...(await opts.preload) }
    if (this.opened === false) await this.ready()

    const discoveryKey = opts.name
      ? await this.storage.getAlias({ name: opts.name, namespace: this.ns })
      : null
    this._maybeClosed()

    const core = this._openCore(discoveryKey, opts)

    return {
      core,
      sessions: this.sessions.get(core.id),
      ongc: this._ongcBound,
      manifest: opts.manifest || null,
      encryption: opts.encryption || null,
      encryptionKey: opts.encryptionKey || null, // back compat, should remove
      isBlockKey: !!opts.isBlockKey // back compat, should remove
    }
  }

  _auth(discoveryKey, opts) {
    const result = {
      keyPair: null,
      key: null,
      discoveryKey,
      manifest: null,
      group: null
    }

    if (opts.name) {
      result.keyPair = createKeyPair(this.primaryKey, this.ns, opts.name)
    } else if (opts.keyPair) {
      result.keyPair = opts.keyPair
    }

    if (opts.manifest) {
      result.manifest = opts.manifest
    } else if (result.keyPair && !result.discoveryKey) {
      result.manifest = {
        version: this.manifestVersion,
        signers: [{ publicKey: result.keyPair.publicKey }]
      }
    }

    if (opts.group) {
      result.group = opts.group
    }

    if (opts.key) result.key = ID.decode(opts.key)
    else if (result.manifest) result.key = Hypercore.key(result.manifest)

    if (result.discoveryKey) return result

    if (opts.discoveryKey) result.discoveryKey = ID.decode(opts.discoveryKey)
    else if (result.key) result.discoveryKey = crypto.discoveryKey(result.key)
    else throw new Error('Could not derive discovery from input')

    return result
  }

  _openCore(discoveryKey, opts) {
    const auth = this._auth(discoveryKey, opts)

    const id = toHex(auth.discoveryKey)
    const existing = this.cores.resume(id, auth.group)
    if (existing && !existing.closing) return existing

    const core = Hypercore.createCore(this.storage, {
      preopen: existing && existing.opened ? existing.closing : null, // always wait for the prev one to close first in any case...
      eagerUpgrade: true,
      notDownloadingLinger: opts.notDownloadingLinger,
      allowFork: opts.allowFork !== false,
      allowPush: !!opts.allowPush,
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
      group: auth.group,
      globalCache: opts.globalCache || this.globalCache || null,
      alias: opts.name ? { name: opts.name, namespace: this.ns } : null
    })

    core.onidle = () => {
      this.cores.gc(core)
    }

    core.ongroupupdate = this._onGroupActiveBound

    core.replicator.ondownloading = () => {
      if (this.active) this.streamTracker.attachAll(core)
    }

    this.cores.set(id, core)
    return core
  }
}

module.exports = Corestore

function isStream(s) {
  return typeof s === 'object' && s && typeof s.pipe === 'function'
}

function generateNamespace(namespace, name) {
  if (!b4a.isBuffer(name)) name = b4a.from(name)
  const out = b4a.allocUnsafeSlow(32)
  sodium.crypto_generichash_batch(out, [namespace, name])
  return out
}

function deriveSeed(primaryKey, namespace, name) {
  if (!b4a.isBuffer(name)) name = b4a.from(name)
  const out = b4a.alloc(32)
  sodium.crypto_generichash_batch(out, [NS, namespace, name], primaryKey)
  return out
}

function createKeyPair(primaryKey, namespace, name) {
  const seed = deriveSeed(primaryKey, namespace, name)
  const buf = b4a.alloc(sodium.crypto_sign_PUBLICKEYBYTES + sodium.crypto_sign_SECRETKEYBYTES)
  const keyPair = {
    publicKey: buf.subarray(0, sodium.crypto_sign_PUBLICKEYBYTES),
    secretKey: buf.subarray(sodium.crypto_sign_PUBLICKEYBYTES)
  }
  sodium.crypto_sign_seed_keypair(keyPair.publicKey, keyPair.secretKey, seed)
  return keyPair
}

function noop() {}

function toHex(discoveryKey) {
  return b4a.toString(discoveryKey, 'hex')
}
