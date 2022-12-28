# Corestore

### [See the full API docs at docs.holepunch.to](https://docs.holepunch.to/building-blocks/corestore)

Corestore is a Hypercore factory that makes it easier to manage large collections of named Hypercores.

Corestore provides:
1. __Key Derivation__ - All writable Hypercore keys are derived from a single master key and a user-provided name.
2. __Session Handling__ - If a single Hypercore is loaded multiple times through the `get` method, the underlying resources will only be opened once (using Hypercore 10's new session feature). Once all sessions are closed, the resources will be released.
3. __Storage Management__ - Hypercores can be stored in any random-access-storage instance, where they will be keyed by their discovery keys.
4. __Namespacing__ - You can share a single Corestore instance between multiple applications or components without worrying about naming collisions by creating "namespaces" (e.g. `corestore.namespace('my-app').get({ name: 'main' })`)

### Installation
`npm install corestore`

### Usage
A corestore instance can be constructed with a random-access-storage module, a function that returns a random-access-storage module given a path, or a string. If a string is specified, it will be assumed to be a path to a local storage directory:
```js
const Corestore = require('corestore')

const store = new Corestore('./my-storage')
const core1 = store.get({ name: 'core-1' })
const core2 = store.get({ name: 'core-2' })
```

### API
#### `const store = new Corestore(storage)`
Create a new Corestore instance.

`storage` can be either a random-access-storage module, a string, or a function that takes a path and returns an random-access-storage instance.

#### `const core = store.get(key | { name: 'a-name', ...hypercoreOpts})`
Loads a Hypercore, either by name (if the `name` option is provided), or from the provided key (if the first argument is a Buffer, or if the `key` options is set).

If that Hypercore has previously been loaded, subsequent calls to `get` will return a new Hypercore session on the existing core.

All other options besides `name` and `key` will be forwarded to the Hypercore constructor.

#### `const stream = store.replicate(optsOrStream)`
Creates a replication stream that's capable of replicating all Hypercores that are managed by the Corestore, assuming the remote peer has the correct capabilities.

`opts` will be forwarded to Hypercore's `replicate` function.

Corestore replicates in an "all-to-all" fashion, meaning that when replication begins, it will attempt to replicate every Hypercore that's currently loaded and in memory. These attempts will fail if the remote side doesn't have a Hypercore's capability -- Corestore replication does not exchange Hypercore keys.

If the remote side dynamically adds a new Hypercore to the replication stream, Corestore will load and replicate that core if possible.

Using [Hyperswarm](https://github.com/holepunchto/hyperswarm) you can easily replicate corestores

``` js
const swarm = new Hyperswarm()

// join the relevant topic
swarm.join(...)

// simply pass the connection stream to corestore
swarm.on('connection', (connection) => store.replicate(connection))
```

#### `const store = store.namespace(name)`
Create a new namespaced Corestore. Namespacing is useful if you're going to be sharing a single Corestore instance between many applications or components, as it prevents name collisions.

Namespaces can be chained:
```js
const ns1 = store.namespace('a')
const ns2 = ns1.namespace('b')
const core1 = ns1.get({ name: 'main' }) // These will load different Hypercores
const core2 = ns2.get({ name: 'main' })
```

#### `const storeB = storeA.session(opts)`
Create a new Corestore that shares resources with the original, like cache, cores, replication streams, and storage, while optionally resetting the namespace, overriding `primaryKey`.
Useful when an application wants to accept an optional Corestore, but needs to maintain a predictable key derivation.

`opts` are the same as the constructor options: 

```js
{
  primaryKey, // Overrides the primaryKey for this session
  namespace, // If set to null it will reset to the DEFAULT_NAMESPACE
}
```

#### `store.on('core-open', core)`
Emitted when the first session for a core is opened.

*Note: This core may close at any time, so treat it as a weak reference*

#### `store.on('core-close', core)`
Emitted when the last session for a core is closed.

### License
MIT

