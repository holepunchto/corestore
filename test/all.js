const test = require('tape')
const crypto = require('hypercore-crypto')
const ram = require('random-access-memory')
const tmp = require('tmp-promise')

const Corestore = require('..')

test('basic get with caching', async function (t) {
  const store = new Corestore(ram)
  const core1a = store.get({ name: 'core-1' })
  const core1b = store.get({ name: 'core-1' })
  const core2 = store.get({ name: 'core-2' })

  await Promise.all([core1a.ready(), core1b.ready(), core2.ready()])

  t.same(core1a.key, core1b.key)
  t.notSame(core1a.key, core2.key)

  t.true(core1a.writable)
  t.true(core1b.writable)

  t.same(store.cores.size, 2)

  t.end()
})

test('basic get with custom keypair', async function (t) {
  const store = new Corestore(ram)
  const kp1 = crypto.keyPair()
  const kp2 = crypto.keyPair()

  const core1 = store.get(kp1)
  const core2 = store.get(kp2)
  await Promise.all([core1.ready(), core2.ready()])

  t.same(core1.key, kp1.publicKey)
  t.same(core2.key, kp2.publicKey)
  t.true(core1.writable)
  t.true(core2.writable)

  t.end()
})

test('basic namespaces', async function (t) {
  const store = new Corestore(ram)
  const ns1 = store.namespace('ns1')
  const ns2 = store.namespace('ns2')
  const ns3 = store.namespace('ns1') // Duplicate namespace

  const core1 = ns1.get({ name: 'main' })
  const core2 = ns2.get({ name: 'main' })
  const core3 = ns3.get({ name: 'main' })
  await Promise.all([core1.ready(), core2.ready(), core3.ready()])

  t.false(core1.key.equals(core2.key))
  t.true(core1.key.equals(core3.key))
  t.true(core1.writable)
  t.true(core2.writable)
  t.true(core3.writable)
  t.same(store.cores.size, 2)

  t.end()
})

test('basic replication', async function (t) {
  const store1 = new Corestore(ram)
  const store2 = new Corestore(ram)

  const core1 = store1.get({ name: 'core-1' })
  const core2 = store1.get({ name: 'core-2' })
  await core1.append('hello')
  await core2.append('world')

  const core3 = store2.get({ key: core1.key })
  const core4 = store2.get({ key: core2.key })

  const s = store1.replicate(true)
  s.pipe(store2.replicate(false)).pipe(s)

  t.same(await core3.get(0), Buffer.from('hello'))
  t.same(await core4.get(0), Buffer.from('world'))

  t.end()
})

test('nested namespaces', async function (t) {
  const store = new Corestore(ram)
  const ns1a = store.namespace('ns1').namespace('a')
  const ns1b = store.namespace('ns1').namespace('b')

  const core1 = ns1a.get({ name: 'main' })
  const core2 = ns1b.get({ name: 'main' })
  await Promise.all([core1.ready(), core2.ready()])

  t.false(core1.key.equals(core2.key))
  t.true(core1.writable)
  t.true(core2.writable)
  t.same(store.cores.size, 2)

  t.end()
})

test('core uncached when all sessions close', async function (t) {
  const store = new Corestore(ram)
  const core1 = store.get({ name: 'main' })
  await core1.ready()
  t.same(store.cores.size, 1)
  await core1.close()
  t.same(store.cores.size, 0)
  t.end()
})

test('writable core loaded from name userData', async function (t) {
  const dir = await tmp.dir({ unsafeCleanup: true })

  let store = new Corestore(dir.path)
  let core = store.get({ name: 'main' })
  await core.ready()
  const key = core.key

  t.true(core.writable)
  await core.append('hello')
  t.same(core.length, 1)

  await store.close()
  store = new Corestore(dir.path)
  core = store.get(key)
  await core.ready()

  t.true(core.writable)
  await core.append('world')
  t.same(core.length, 2)
  t.same(await core.get(0), Buffer.from('hello'))
  t.same(await core.get(1), Buffer.from('world'))

  await dir.cleanup()
  t.end()
})

test('storage locking', async function (t) {
  const dir = await tmp.dir({ unsafeCleanup: true })

  const store1 = new Corestore(dir.path)
  await store1.ready()

  const store2 = new Corestore(dir.path)
  try {
    await store2.ready()
    t.fail('dir should have been locked')
  } catch {
    t.pass('dir was locked')
  }

  await dir.cleanup()
  t.end()
})
