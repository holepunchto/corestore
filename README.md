# random-access-corestore
[![Build Status](https://travis-ci.com/andrewosh/random-access-corestore.svg?token=WgJmQm3Kc6qzq1pzYrkx&branch=master)](https://travis-ci.com/andrewosh/random-access-corestore)

A simple corestore that wraps a random-access-storage module. This module is the canonical implementation of the "corestore" interface, which exposes a hypercore factory and a set of associated functions for managing generated hypercores.

Corestore imposes the convention that the first requested hypercore defines the discovery key (and encryption parameters) for its replication stream.

### Installation
`npm i random-access-corestore --save`

### Usage
A random-access-corestore instance can be constructed with either a random-access-storage module, or a function that returns a random-access-storage module given a path:
```js
const corestore = require('random-access-corestore')
const ram = require('random-access-memory')
const raf = require('random-access-file')
const store1 = corestore(ram)
const store2 = corestore(path => raf('store2/' + path))
```

Hypercores can be generated with `get`. The first core that's generated will be considered the "main" core, and will define the corestore's replication parameters:
```js
const core1 = store1.get()
```

Additional hypercores can be created 
const core2 = store1.get({ name: 'second' })

core1.ready(() => {
  const core3 = store2.get(core1.key)
  const stream = store1.replicate()
  stream.pipe(store2.replicate()).pipe(stream)
})
```

### API


### License
MIT
