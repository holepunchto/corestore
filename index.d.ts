// Type declarations for the holepunchto/corestore public API.

import type Hypercore from 'hypercore'

export interface CorestoreOptions {
  /** The primary key to use as the master key for key derivation. */
  primaryKey?: any
  writable?: any
}

/**
 * `opts`
 */
export interface GroupNotifyHandleUpdateOptions {
  /** What timestamp to start returning updates from. Default `0` returns all updates */
  since?: any
  /** Flag to return updates in reverse order. Defaults to `true` so latest returned first */
  reverse?: any
}

export class Corestore {
  /**
   * Create a new Corestore instance.
   * @param storage - `storage` can be either a `hypercore-storage` instance or a string.
   */
  constructor(storage: any, opts?: CorestoreOptions)

  /**
   * Register a callback called when new Hypercores are opened. `core` is the internal core for the opened Hypercore. It can be used to create weak references to a Hypercore like so:
   */
  watch(fn?: any): any

  /**
   * Unregister a callback used with `store.watch(callback)` so it no longer fires.
   */
  unwatch(fn: any): any

  /**
   * Get a `handle` for updates from all `hypercore`s with the group `topic` set.
   */
  notifyGroup(topic: any): GroupNotifyHandle

  findingPeers(): any

  audit(opts?: any): any

  /**
   * Suspend the underlying storage for the Corestore.
   */
  suspend(options?: any): Promise<any>

  /**
   * Resume a suspended Corestore.
   */
  resume(): any

  /**
   * Create a new Corestore session. Closing a session will close all cores made from this session.
   */
  session(opts: any): Corestore

  /**
   * Create a new namespaced Corestore session. Namespacing is useful if you're going to be sharing a single Corestore instance between many applications or components, as it prevents name collisions.
   */
  namespace(name: any, opts: any): Corestore

  /**
   * Creates a discovery key stream of all cores within a namespace or all cores in general if no namespace is provided.
   */
  list(namespace: any): any

  getAuth(discoveryKey: any): any

  /**
   * Creates a replication stream that's capable of replicating all Hypercores that are managed by the Corestore, assuming the remote peer has the correct capabilities.
   */
  replicate(isInitiator: any, opts: any): any

  staticify(core: any, opts: any): Promise<any>

  /**
   * Loads a Hypercore, either by name (if the `name` option is provided), or from the provided key (if the first argument is a Buffer or String with hex/z32 key, or if the `key` options is set).
   */
  get(opts: any): Hypercore

  /**
   * Generate a key pair seeded with the Corestore's primary key using a `name` and a `ns` aka namespace. `ns` defaults to the current namespace.
   * @param ns - `ns` defaults to the current namespace.
   */
  createKeyPair(name: any, ns?: any): Promise<any>

  ready(): Promise<any>

  /**
   * Fully close this Corestore instance.
   */
  close(): Promise<any>

  readonly opened: any

  readonly closed: any

  emit(event: any, arg1?: any): any

  root: any

  storage: any

  streamTracker: any

  cores: any

  sessions: any

  corestores: any

  readOnly: any

  globalCache: any

  primaryKey: any

  ns: any

  manifestVersion: any

  shouldSuspend: any

  active: any

  watchers: any

  watchIndex: any

  /**
   * The `group-active` event emits whenever an opened Hypercore in the store updates. The `topic` is the group topic the core belongs to.
   */
  on(event: 'group-active', listener: (topic: any, update: any) => void): this
}

declare class GroupNotifyHandle {
  constructor(store: any, topic: any)

  updates(opts: any): any

  /**
   * Destroys and unregisters the `handle` from its `store`.
   */
  destroy(): any

  emit(event: any, arg1?: any): any

  index: any

  /**
   * Gets updates for the `topic` the handle is for.
   * @param opts - `opts`
   */
  update(opts?: GroupNotifyHandleUpdateOptions): any

  /**
   * Calls the callback whenever a core with the `topic` for the `handle` updates.
   */
  on(event: 'update', listener: (...args: any[]) => void): this
}

declare class FindingPeers {
  constructor()

  add(core: any): any

  inc(sessions: any): any

  dec(sessions: any): any

  destroy(): any

  count: any

  pending: any

  destroyed: any
}

export default Corestore
