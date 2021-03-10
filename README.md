# basestorevault


This module is the canonical implementation of the "basestorevault" interface, which exposes a dDatabase factory and a set of associated functions for managing generated dDatabases.

A basestorevault is designed to efficiently store and replicate multiple sets of interlinked dDatabases, such as those used by [dDrive](https://github.com/dwebprotocol/ddrive) and [mountable-dwebtrie](https://github.com/dwebprotocol/dwebtrie), removing the responsibility of managing custom storage/replication code from these higher-level modules.

In order to do this, basestorevault provides:
1. __Key derivation__ - all writable dDatabase keys are derived from a single master key.
2. __Caching__ - Two separate caches are used for passively replicating bases (those requested by peers) and active bases (those requested by the owner of the basestorevault).
3. __Storage bootstrapping__ - You can create a `default` dDatabase that will be loaded when a key is not specified, which is useful when you don't want to reload a previously-created dDatabase by key.
4. __Namespacing__ - If you want to create multiple compound data structures backed by a single basestorevault, you can create namespaced basestores such that each data structure's `default` feed is separate.

### Installation
`npm i basestorevault --save`

### Usage
A basestorevault instance can be constructed with a random-access-storage module, a function that returns a random-access-storage module given a path, or a string. If a string is specified, it will be assumed to be a path to a local storage directory:
```js
const Basestore = require('basestorevault')
const ram = require('random-access-memory')
const store = new Basestore(ram)
await store.ready()
```

dDatabases can be generated with both the `get` and `default` methods. If the first writable base is created with `default`, it will be used for storage bootstrapping. We can always reload this bootstrapping base off disk without your having to store its public key externally. Keys for other hypercores should either be stored externally, or referenced from within the default base:
```js
const base1 = store1.default()
```
_Note: You do not have to create a default feed before creating additional ones unless you'd like to bootstrap your basestorevault from disk the next time it's instantiated._

Additional dDatabases can be created by key, using the `get` method. In most scenarios, these additional keys can be extracted from the default (bootstrapping) base. If that's not the case, keys will have to be stored externally:
```js
const base2 = store1.get({ key: Buffer(...) })
```
All dDatabases are indexed by their discovery keys, so that they can be dynamically injected into replication streams when requested.

Two Basestores can be replicated with the `replicate` function, which accepts dDatabase's `replicate` options:
```js
const store1 = new Basestore(ram)
const store2 = new Basestore(ram)
await Promise.all([store1.ready(), store2.ready()]

const base1 = store2.get()
const base2 = store2.get({ key: base1.key })
const stream = store1.replicate(true, { live: true })
stream.pipe(store2.replicate(false, { live: true })).pipe(stream) // This will replicate all common bases.
```

### API
#### `const store = basestorevault(storage, [opts])`
Create a new basestorevault instance. `storage` can be either a random-access-storage module, or a function that takes a path and returns a random-access-storage instance.

Opts is an optional object which can contain any dDatabase constructor options, plus the following:
```js
{
  cacheSize: 1000 // The size of the LRU cache for passively-replicating bases.
}
```

#### `store.default(opts)`
Create a new default dDatabase, which is used for bootstrapping the creation of subsequent dDatabases. Options match those in `get`.

#### `store.get(opts)`
Create a new dDatabase. Options can be one of the following:
```js
{
  key: 0x1232..., // A Buffer representing a dDatabase key
  discoveryKey: 0x1232..., // A Buffer representing a dDatabase discovery key (must have been previously created by key)
  ...opts // All other options accepted by the dDatabase constructor
}
```

If `opts` is a Buffer, it will be interpreted as a dDatabase key.

#### `store.on('feed', feed, options)`

Emitted everytime a feed is loaded internally (ie, the first time get(key) is called).
Options will be the full options map passed to .get.

#### `store.replicate(isInitiator, [opts])`
Create a replication stream that will replicate all bases currently in memory in the basestorevault instance.

When piped to another basestorevault's replication stream, only those bases that are shared between the two dWebStores will be successfully replicated.

#### `store.list()`
Returns a Map of all bases currently cached in memory. For each base in memory, the map will contain the following entries:
```
{
  discoveryKey => base,
  ...
}
```

#### `const namespacedStore = store.namespace('some-name')`
Create a "namespaced" basestorevault that uses the same underlying storage as its parent, and mirrors the complete basestorevault API. 

`namespacedStore.default` returns a different default base, using the namespace as part of key generation, which makes it easier to bootstrap multiple data structures from the same basestorevault. The general pattern is for all data structures to bootstrap themselves from their basestorevault's default feed:
```js
const store = new Basestore(ram)
const drive1 = new DDrive(store.namespace('drive1'))
const drive2 = new DDrive(store.namespace('drive2'))
```

Namespaces currently need to be saved separately outside of basestorevault (as a mapping from key to namespace), so that data structures remain writable across restarts. Extending the above code, this might look like:
```js
async function getDrive (opts = {}) {
  let namespace = opts.key ? await lookupNamespace(opts.key) : await createNamespace()
  const namespacedDWebVault = store.namespace(namespace)
  const drive = new DDrive(namespacedDWebVault)
  await saveNamespace(drive.key, namespace)
}
```

#### `store.close(cb)`
Close all dDatabases previously generated by the basestorevault.

### License
MIT
