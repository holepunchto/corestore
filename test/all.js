const p = require('path')
const ram = require('random-access-memory')
const raf = require('random-access-file')
const test = require('tape')
const hypercoreCrypto = require('hypercore-crypto')
const { toPromises } = require('hypercore-promisifier')

const Corestore = require('..')
const { cleanup } = require('./helpers')

test('ram-based corestore, acceptable get options', async t => {
  const store = create(ram)

  // A name option
  const core1 = store.get({ name: 'default' })
  await toPromises(core1).ready()

  {
    // Buffer arg
    const core = store.get(core1.key)
    await toPromises(core).ready()
    t.same(core, core1)
  }

  {
    // String arg
    const core = store.get(core1.key.toString('hex'))
    await toPromises(core).ready()
    t.same(core, core1)
  }

  {
    // Object arg
    const core = store.get({ key: core1.key })
    await toPromises(core).ready()
    t.same(core, core1)
  }

  {
    // Object arg with string key
    const core = store.get({ key: core1.key.toString('hex') })
    await toPromises(core).ready()
    t.same(core, core1)
  }

  {
    // Custom keypair
    const core = store.get({ keyPair: { secretKey: core1.secretKey, publicKey: core1.key } })
    await toPromises(core).ready()
    t.same(core, core1)
  }

  t.end()
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
    try {
      get()
      t.fail('get did not throw correctly')
    } catch (err) {
      t.true(err)
    }
  }
})

test('ram-based corestore, many gets before ready', async t => {
  const store = create(ram)
  const core1 = store.get({ name: 'core1' })
  const core2 = store.get({ name: 'core1' })
  const core3 = store.get({ name: 'core3' })

  await Promise.all([
    toPromises(core1).ready(),
    toPromises(core2).ready(),
    toPromises(core3).ready()
  ])

  t.same(core1, core2)
  t.notSame(core1, core3)

  // At this point, the pre-ready cores should've been moved to the main cache.
  t.same(store.cache.size, 2)

  t.end()
})

test('ram-based corestore, closing a ref decrements refcount', async t => {
  const store = create(ram)
  const core = toPromises(store.get({ name: 'core1' }))
  await core.ready()

  const cacheEntry = store.cache.entries.get(core.discoveryKey.toString('hex'))
  t.same(cacheEntry.refs, 1)

  await core.close()
  t.same(cacheEntry.refs, 0)

  t.end()
})

test('ram-based corestore, simple replication', async t => {
  const store1 = create(ram)
  const store2 = create(ram)

  const core1 = toPromises(store1.get({ name: 'core1', valueEncoding: 'utf-8' }))
  await core1.append('hello')

  const s1 = store1.replicate(true, { live: true })
  s1.pipe(store2.replicate(false, { live: true })).pipe(s1)

  const clone1 = toPromises(store2.get({ key: core1.key, valueEncoding: 'utf-8' }))
  t.same(await clone1.get(0), 'hello')

  const core2 = toPromises(store1.get({ name: 'core2', valueEncoding: 'utf-8' }))
  await core2.append('world')

  const clone2 = toPromises(store2.get({ key: core2.key, valueEncoding: 'utf-8' }))
  t.same(await clone2.get(0), 'world')

  t.end()
})

test('ram-based corestore, sparse replication', async t => {
  const store1 = create(ram, { sparse: true })
  const store2 = create(ram, { sparse: true })

  const core1 = toPromises(store1.get({ name: 'core1', valueEncoding: 'utf-8' }))
  await core1.append('hello')

  const s1 = store1.replicate(true, { live: true })
  s1.pipe(store2.replicate(false, { live: true })).pipe(s1)

  const clone1 = toPromises(store2.get({ key: core1.key, valueEncoding: 'utf-8' }))
  t.same(await clone1.get(0), 'hello')

  const core2 = toPromises(store1.get({ name: 'core2', valueEncoding: 'utf-8' }))
  await core2.append('world')

  const clone2 = toPromises(store2.get({ key: core2.key, valueEncoding: 'utf-8' }))
  t.same(await clone2.get(0), 'world')

  t.end()
})

test('raf-based corestore, simple replication', async t => {
  const store1 = create(path => raf(p.join('store1', path)))
  const store2 = create(path => raf(p.join('store2', path)))

  const core1 = toPromises(store1.get({ name: 'core1', valueEncoding: 'utf-8' }))
  await core1.append('hello')

  const s1 = store1.replicate(true, { live: true })
  s1.pipe(store2.replicate(false, { live: true })).pipe(s1)

  const clone1 = toPromises(store2.get({ key: core1.key, valueEncoding: 'utf-8' }))
  t.same(await clone1.get(0), 'hello')

  const core2 = toPromises(store1.get({ name: 'core2', valueEncoding: 'utf-8' }))
  await core2.append('world')

  const clone2 = toPromises(store2.get({ key: core2.key, valueEncoding: 'utf-8' }))
  t.same(await clone2.get(0), 'world')

  await cleanup(['store1', 'store2'])
  t.end()
})

test('raf-based corestore, close and reopen', async t => {
  let store = create('test-store')

  let core1 = toPromises(store.get({ name: 'core1', valueEncoding: 'utf-8' }))
  await core1.append('hello')

  t.same(await core1.get(0), 'hello')

  await store.close()
  store = create('test-store')
  core1 = toPromises(store.get({ name: 'core1', valueEncoding: 'utf-8' }))

  t.same(await core1.get(0), 'hello')

  await cleanup(['test-store'])
  t.end()
})

test('raf-based corestore, close and reopen with keypair option', async t => {
  let store = create('test-store')
  const keyPair = hypercoreCrypto.keyPair()

  let core1 = toPromises(store.get({ keyPair, valueEncoding: 'utf-8' }))
  await core1.append('hello')

  t.same(await core1.get(0), 'hello')

  await store.close()
  store = create('test-store')
  core1 = toPromises(store.get({ keyPair, valueEncoding: 'utf-8' }))

  t.same(await core1.get(0), 'hello')

  await cleanup(['test-store'])
  t.end()
})

test('can backup/restore', async t => {
  const firstStore = create(ram)
  const core1 = firstStore.get({ name: 'hello-world' })
  await toPromises(core1).ready()
  const manifest = await firstStore.backup()

  const secondStore = await Corestore.restore(manifest, ram)
  const core2 = secondStore.get({ key: core1.key })
  await toPromises(core2).ready()

  t.true(core2.writable)

  t.end()
})

function create (storage, opts) {
  const store = new Corestore(storage, opts)
  return store
}
