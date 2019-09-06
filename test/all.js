const p = require('path')
const ram = require('random-access-memory')
const raf = require('random-access-file')
const datEncoding = require('dat-encoding')
const test = require('tape')

const corestore = require('..')
const {
  runAll,
  validateCore,
  delay,
  cleanup
} = require('./helpers')

test('ram-based corestore, different get options', async t => {
  const store1 = corestore(ram)
  const core1 = store1.default()
  var core2, core3, core4, core5

  await runAll([
    cb => core1.ready(cb),
    cb => core1.append('hello', cb),
    cb => {
      // Buffer arg
      core2 = store1.get(core1.key)
      return core2.ready(cb)
    },
    cb => {
      // Object arg
      core3 = store1.get({ key: core1.key })
      return core3.ready(cb)
    },
    cb => {
      // Discovery key option
      core4 = store1.get({ discoveryKey: core1.discoveryKey })
      return core4.ready(cb)
    },
    cb => {
      // String option
      core5 = store1.get({ key: datEncoding.encode(core1.key) })
      return core5.ready(cb)
    }
  ])

  t.same(core1, core2)
  t.same(core1, core3)
  t.same(core1, core4)
  t.same(core1, core5)
  t.end()
})

test('ram-based corestore, simple replication', async t => {
  const store1 = corestore(ram)
  const store2 = corestore(ram)
  const core1 = store1.default()
  const core2 = store1.get()
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => core2.ready(cb),
    cb => {
      core3 = store2.default(core1.key)
      return core3.ready(cb)
    },
    cb => {
      core4 = store2.get({ key: core2.key })
      return core4.ready(cb)
    },
    cb => core1.append('hello', cb),
    cb => core1.append('world', cb),
    cb => core2.append('cat', cb),
    cb => core2.append('dog', cb),
    cb => {
      const stream = store1.replicate()
      stream.pipe(store2.replicate()).pipe(stream)
      stream.on('end', cb)
    }
  ])

  await validateCore(t, core3, [Buffer.from('hello'), Buffer.from('world')])
  await validateCore(t, core4, [Buffer.from('cat'), Buffer.from('dog')])

  t.end()
})

test('ram-based corestore, replicating with different default keys', async t => {
  const store1 = corestore(ram)
  const store2 = corestore(ram)
  const core1 = store1.default()
  const core2 = store1.get()
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => core2.ready(cb),
    cb => {
      core3 = store2.default()
      return core3.ready(cb)
    },
    cb => {
      core4 = store2.get({ key: core1.key })
      return core4.ready(cb)
    },
    cb => core1.append('cat', cb),
    cb => core1.append('dog', cb),
    cb => {
      const stream = store1.replicate({ encrypt: false })
      stream.pipe(store2.replicate({ encrypt: false })).pipe(stream)
      stream.on('end', cb)
    }
  ])

  await validateCore(t, core4, [Buffer.from('cat'), Buffer.from('dog')])
  t.end()
})

test('ram-based corestore, sparse replication', async t => {
  const store1 = corestore(ram, { sparse: true })
  const store2 = corestore(ram, { sparse: true })
  const core1 = store1.default()
  const core2 = store1.get()
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => core2.ready(cb),
    cb => {
      t.same(core2.sparse, true)
      t.same(core1.sparse, true)
      return process.nextTick(cb, null)
    },
    cb => {
      core3 = store2.default(core1.key)
      return core3.ready(cb)
    },
    cb => {
      core4 = store2.get({ key: core2.key })
      return core4.ready(cb)
    },
    cb => {
      const stream = store1.replicate({ live: true, encrypt: false})
      stream.pipe(store2.replicate({ live: true, encrypt: false})).pipe(stream)
      return process.nextTick(cb, null)
    },
    cb => core1.append('hello', cb),
    cb => core1.append('world', cb),
    cb => core2.append('cat', cb),
    cb => core2.append('dog', cb),
    cb => {
      t.same(core3.length, 0)
      t.same(core4.length, 0)
      return process.nextTick(cb, null)
    }
  ])

  await validateCore(t, core3, [Buffer.from('hello'), Buffer.from('world')])
  await validateCore(t, core4, [Buffer.from('cat'), Buffer.from('dog')])
  t.end()
})

test('ram-based corestore, sparse replication with different default keys', async t => {
  const store1 = corestore(ram, { sparse: true })
  const store2 = corestore(ram, { sparse: true })
  const core1 = store1.default()
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => {
      core3 = store2.default()
      return core3.ready(cb)
    },
    cb => {
      const s1 = store1.replicate({ live: true, encrypt: false })
      const s2 = store2.replicate({ live: true, encrypt: false })
      s1.pipe(s2).pipe(s1)
      return process.nextTick(cb, null)
    },
    cb => core1.append('cat', cb),
    cb => core1.append('dog', cb),
    cb => {
      core4 = store2.get({ key: core1.key })
      return core4.ready(cb)
    },
    cb => {
      t.same(core4.length, 0)
      t.same(core1.length, 2)
      return process.nextTick(cb, null)
    }
  ])

  await validateCore(t, core4, [Buffer.from('cat'), Buffer.from('dog')])
  t.end()
})

test('raf-based corestore, simple replication', async t => {
  const store1 = corestore(path => raf(p.join('store1', path)))
  const store2 = corestore(path => raf(p.join('store2', path)))
  const core1 = store1.default()
  const core2 = store1.get()
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => core2.ready(cb),
    cb => {
      core3 = store2.default({ key: core1.key })
      return core3.ready(cb)
    },
    cb => {
      core4 = store2.get({ key: core2.key })
      return core4.ready(cb)
    },
    cb => core1.append('hello', cb),
    cb => core1.append('world', cb),
    cb => core2.append('cat', cb),
    cb => core2.append('dog', cb),
    cb => {
      const stream = store1.replicate()
      stream.pipe(store2.replicate()).pipe(stream)
      stream.on('end', cb)
    }
  ])

  await validateCore(t, core3, [Buffer.from('hello'), Buffer.from('world')])
  await validateCore(t, core4, [Buffer.from('cat'), Buffer.from('dog')])
  await cleanup(['store1', 'store2'])
  t.end()
})

test('raf-based corestore, close and reopen', async t => {
  var store = corestore(path => raf(p.join('store', path)))
  var core = store.default()

  await runAll([
    cb => core.ready(cb),
    cb => core.append('hello', cb),
    cb => store.close(cb),
    cb => {
      t.true(core.closed)
      return process.nextTick(cb, null)
    },
    cb => {
      store =  corestore(path => raf(p.join('store', path)))
      core = store.default()
      return core.ready(cb)
    }
  ])

  await validateCore(t, core, [Buffer.from('hello')])
  await cleanup(['store'])
  t.end()
})

test('live replication with an additional core', async t => {
  const store1 = corestore(ram)
  const store2 = corestore(ram)
  const core1 = store1.default()
  var core2 = null
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => {
      core3 = store2.default({ key: core1.key })
      return core3.ready(cb)
    },
    cb => {
      const stream = store1.replicate({ live: true })
      stream.pipe(store2.replicate({ live: true })).pipe(stream)
      return cb(null)
    },
    cb => {
      core2 = store1.get()
      return core2.ready(cb)
    },
    cb => {
      core4 = store2.get(core2.key)
      return core4.ready(cb)
    },
    cb => core2.append('hello', cb),
    cb => core2.append('world', cb),
    cb => delay(500, cb)
  ])

  await validateCore(t, core4, [Buffer.from('hello'), Buffer.from('world')])
  t.end()
})

test('graph-based replication excludes cores that aren\'t dependencies', async t => {
  const store1 = corestore(ram)
  const store2 = corestore(ram)

  const graphCores1 = await getGraphCores(store1)
  const coreKeys = graphCores1.map(core => core.key)
  const discoveryKeys = graphCores1.map(core => core.discoveryKey)
  const graphCores2 = await getGraphCores(store2, coreKeys)

  await delay(50)

  const s1 = store1.replicate(discoveryKeys[1], { live: true, encrypt: false })
  const s2 = store2.replicate(discoveryKeys[1], { live: true, encrypt: false })
  s1.pipe(s2).pipe(s1)

  await runAll([
    cb => graphCores1[0].append('hello', cb),
    cb => graphCores1[2].append('cat', cb),
    cb => graphCores1[4].append('dog', cb),
    cb => setTimeout(cb, 50),
    cb => {
      t.same(graphCores2[0].length, 0)
      t.same(graphCores2[2].length, 1)
      t.same(graphCores2[4].length, 1)
      return process.nextTick(cb, null)
    }
  ])

  t.end()

  async function getGraphCores (store, keys) {
    const defaultCore = store.default({ key: keys && keys[0] })
    await ready(defaultCore)
    const core1 = store.get({ key: keys && keys[1] })
    await ready(core1)
    const core2 = store.get({ key: keys && keys[2], parents: [core1.discoveryKey] })
    await ready(core2)
    const core3 = store.get({ key: keys && keys[3], parents: [core1.discoveryKey]})
    await ready(core3)
    const core4 = store.get({ key: keys && keys[4], parents: [core3.discoveryKey]})
    await ready(core4)
    return [defaultCore, core1, core2, core3, core4]
  }
})

function ready (core) {
  return new Promise((resolve, reject) => {
    core.ready(err => {
      if (err) return reject(err)
      return resolve()
    })
  })
}
