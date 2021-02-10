const p = require('path')
const ram = require('random-access-memory')
const raf = require('random-access-file')
const test = require('ava')
const hypercoreCrypto = require('hypercore-crypto')

const Corestore = require('..')
const { cleanup } = require('./helpers')

test('ram-based corestore, acceptable get options', async t => {
  const store = create(ram)

  // A name option
  const core1 = store.get({ name: 'default' })
  await core1.ready()

  {
    // Buffer arg
    const core = store.get(core1.key)
    await core.ready()
    t.is(core.writer, core1.writer)
  }

  {
    // String arg
    const core = store.get(core1.key.toString('hex'))
    await core.ready()
    t.is(core.writer, core1.writer)
  }

  {
    // Object arg
    const core = store.get({ key: core1.key })
    await core.ready()
    t.is(core.writer, core1.writer)
  }

  {
    // Object arg with string key
    const core = store.get({ key: core1.key.toString('hex') })
    await core.ready()
    t.is(core.writer, core1.writer)
  }

  {
    // Custom keypair
    const core = store.get({ keyPair: { secretKey: core1.secretKey, publicKey: core1.key } })
    await core.ready()
    t.is(core.writer, core1.writer)
  }
})

test('ram-based corestore, unacceptable get options', async t => {
  const store = create(ram)
  const badGets = [
    () => store.get(),
    () => store.get('abc'),
    () => store.get({ name: null }),
    () => store.get({ key: null })
  ]
  for (const get of badGets) {
    t.throws(get)
  }
})

test('ram-based corestore, many gets before ready', async t => {
  const store = create(ram)
  const core1 = store.get({ name: 'core1' })
  const core2 = store.get({ name: 'core1' })
  const core3 = store.get({ name: 'core3' })

  await Promise.all([
    core1.ready(),
    core2.ready(),
    core3.ready()
  ])

  t.is(core1.writer, core2.writer)
  t.not(core1.writer, core3.writer)

  // At this point, the pre-ready cores should've been moved to the main cache.
  t.is(store.cache.size, 2)
})

test('ram-based corestore, simple replication', async t => {
  const store1 = create(ram)
  const store2 = create(ram)

  const core1 = store1.get({ name: 'core1', valueEncoding: 'utf-8' })
  await core1.append('hello')

  const s1 = store1.replicate(true)
  s1.pipe(store2.replicate(false)).pipe(s1)

  const clone1 = store2.get({ key: core1.key, valueEncoding: 'utf-8' })

  t.is(await clone1.get(0), 'hello')

  const core2 = store1.get({ name: 'core2', valueEncoding: 'utf-8' })
  await core2.append('world')

  const clone2 = store2.get({ key: core2.key, valueEncoding: 'utf-8' })
  t.is(await clone2.get(0), 'world')
})

test('ram-based corestore, sparse replication', async t => {
  const store1 = create(ram, { sparse: true })
  const store2 = create(ram, { sparse: true })

  const core1 = store1.get({ name: 'core1', valueEncoding: 'utf-8' })
  await core1.append('hello')

  const s1 = store1.replicate(true)
  s1.pipe(store2.replicate(false)).pipe(s1)

  const clone1 = store2.get({ key: core1.key, valueEncoding: 'utf-8' })
  t.is(await clone1.get(0), 'hello')

  const core2 = store1.get({ name: 'core2', valueEncoding: 'utf-8' })
  await core2.append('world')

  const clone2 = store2.get({ key: core2.key, valueEncoding: 'utf-8' })
  t.is(await clone2.get(0), 'world')
})

test.serial('raf-based corestore, simple replication', async t => {
  const store1 = create(path => raf(p.join('store1', path)))
  const store2 = create(path => raf(p.join('store2', path)))
  await store1.ready()

  const core1 = store1.get({ name: 'core1', valueEncoding: 'utf-8' })
  await core1.append('hello')

  const s1 = store1.replicate(true)
  s1.pipe(store2.replicate(false)).pipe(s1)

  const clone1 = store2.get({ key: core1.key, valueEncoding: 'utf-8' })
  t.is(await clone1.get(0), 'hello')

  const core2 = store1.get({ name: 'core2', valueEncoding: 'utf-8' })
  await core2.append('world')

  const clone2 = store2.get({ key: core2.key, valueEncoding: 'utf-8' })
  t.is(await clone2.get(0), 'world')

  await cleanup(['store1', 'store2'])
})

test.serial('raf-based corestore, close and reopen', async t => {
  let store = create('test-store')

  let core1 = store.get({ name: 'core1', valueEncoding: 'utf-8' })
  await core1.append('hello')

  t.is(await core1.get(0), 'hello')

  await store.close()

  store = create('test-store')
  core1 = store.get({ name: 'core1', valueEncoding: 'utf-8' })

  t.is(await core1.get(0), 'hello')

  await cleanup(['test-store'])
})

test.serial('raf-based corestore, close and reopen with keypair option', async t => {
  let store = create('test-store')
  const keyPair = hypercoreCrypto.keyPair()

  let core1 = store.get({ keyPair, valueEncoding: 'utf-8' })
  await core1.append('hello')

  t.is(await core1.get(0), 'hello')

  await store.close()
  store = create('test-store')
  core1 = store.get({ keyPair, valueEncoding: 'utf-8' })
  await core1.ready()

  t.deepEqual(core1.key, keyPair.publicKey)
  t.true(core1.writable)
  t.is(await core1.get(0), 'hello')

  await cleanup(['test-store'])
})

test('namespace method is equivalent to name array', async t => {
  const store = create(ram)

  const core1 = store.get({ name: ['a', 'b', 'c'] })
  const core2 = store.namespace('a').namespace('b').get({ name: 'c' })

  await core1.ready()
  await core2.ready()

  t.is(core1.writer, core2.writer)
})

test('can backup/restore', async t => {
  const firstStore = create(ram)
  const core1 = firstStore.get({ name: 'hello-world' })
  await core1.ready()
  const manifest = await firstStore.backup()

  const secondStore = await Corestore.restore(manifest, ram)
  const core2 = secondStore.get({ key: core1.key })
  await core2.ready()

  t.true(core2.writable)
})

test('all sessions closing leads to eviction', async t => {
  const store = create(ram, { cacheSize: 1 })

  const core1 = store.get({ name: 'test-core' })
  const core2 = store.get({ name: 'test-core' })

  const core3 = store.get({ name: 'test-core-2' })

  await Promise.all([core1.ready(), core2.ready(), core3.ready()])

  t.is(store.cache.size, 2)

  await core3.close()

  t.is(store.cache.size, 1)

  await core1.close()
  await core2.close()

  t.is(store.cache.size, 0)
})

function create (storage, opts) {
  const store = new Corestore(storage, opts)
  return store
}
