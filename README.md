# Corestore

### [See the full API docs at docs.holepunch.to](https://docs.holepunch.to/helpers/corestore)

Corestore is a Hypercore factory that makes it easier to manage large collections of named Hypercores.

Corestore provides:

1. **Key Derivation** - All writable Hypercore keys are derived from a single master key and a user-provided name.
2. **Session Handling** - If a single Hypercore is loaded multiple times through the `get` method, the underlying resources will only be opened once (using Hypercore 10's new session feature). Once all sessions are closed, the resources will be released.
3. **Storage Management** - Hypercores can be stored in any `hypercore-storage` instance, where they will be keyed by their discovery keys.
4. **Namespacing** - You can share a single Corestore instance between multiple applications or components without worrying about naming collisions by creating "namespaces" (e.g. `corestore.namespace('my-app').get({ name: 'main' })`)

### Installation

`npm install corestore`

> [!NOTE]
> This readme reflects Corestore 7, our latest major version that is backed by RocksDB for storage and atomicity.
> Whilst we are fully validating that, the npm dist-tag for latest is set to latest version of Corestore 7, the previous major, to avoid too much disruption.
> It will be updated to 11 in a few weeks.

### Usage

A corestore instance can be constructed with a `hypercore-storage` instance, or a string. If a string is specified, it will be assumed to be a path to a local storage directory:

```js
const Corestore = require('corestore')

const store = new Corestore('./my-storage')
const core1 = store.get({ name: 'core-1' })
const core2 = store.get({ name: 'core-2' })
```

### API

#### `const store = new Corestore(storage, options = {})`

Create a new Corestore instance.

`storage` can be either a `hypercore-storage` instance or a string.

Options:

```
{
  primaryKey: null, // The primary key to use as the master key for key derivation.
  writable: true,
}
```

#### `const core = store.get(key | { name: 'a-name', ...hypercoreOpts})`

Loads a Hypercore, either by name (if the `name` option is provided), or from the provided key (if the first argument is a Buffer or String with hex/z32 key, or if the `key` options is set).

If that Hypercore has previously been loaded, subsequent calls to `get` will return a new Hypercore session on the existing core.

All other options besides `name` and `key` will be forwarded to the Hypercore constructor.

#### `const stream = store.replicate(optsOrStream)`

Creates a replication stream that's capable of replicating all Hypercores that are managed by the Corestore, assuming the remote peer has the correct capabilities.

`opts` will be forwarded to Hypercore's `replicate` function.

Corestore replicates in an "all-to-all" fashion, meaning that when replication begins, it will attempt to replicate every Hypercore that's currently loaded and in memory. These attempts will fail if the remote side doesn't have a Hypercore's capability -- Corestore replication does not exchange Hypercore keys.

If the remote side dynamically adds a new Hypercore to the replication stream, Corestore will load and replicate that core if possible.

Using [Hyperswarm](https://github.com/holepunchto/hyperswarm) you can easily replicate corestores

```js
const swarm = new Hyperswarm()

// join the relevant topic
swarm.join(...)

// simply pass the connection stream to corestore
swarm.on('connection', (connection) => store.replicate(connection))
```

#### `const storeB = storeA.session()`

Create a new Corestore session. Closing a session will close all cores made from this session.

#### `const store = store.namespace(name)`

Create a new namespaced Corestore session. Namespacing is useful if you're going to be sharing a single Corestore instance between many applications or components, as it prevents name collisions.

Namespaces can be chained:

```js
const ns1 = store.namespace('a')
const ns2 = ns1.namespace('b')
const core1 = ns1.get({ name: 'main' }) // These will load different Hypercores
const core2 = ns2.get({ name: 'main' })
```

#### `const stream = store.list(namespace)`

Creates a discovery key stream of all cores within a namespace or all cores in general if no namespace is provided.

#### `store.watch((core) => {})`

Register a callback called when new Hypercores are opened. `core` is the internal core for the opened Hypercore. It can be used to create weak references to a Hypercore like so:

```
store.watch(function (core) {
  const weakCore = new Hypercore({ core, weak: true })
})
```

#### `store.unwatch(callback)`

Unregister a callback used with `store.watch(callback)` so it no longer fires.

#### `await store.suspend()`

Suspend the underlying storage for the Corestore.

#### `await store.resume()`

Resume a suspended Corestore.

#### `const keypair = await store.createKeyPair(name, ns = this.ns)`

Generate a key pair seeded with the Corestore's primary key using a `name` and a `ns` aka namespace. `ns` defaults to the current namespace.

This is useful for creating deterministic key pairs that are unique to a peer.

#### `await store.close()`

Fully close this Corestore instance.

### License

MIT
