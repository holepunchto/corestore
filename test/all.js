const test = require('brittle')
const crypto = require('hypercore-crypto')
const ram = require('random-access-memory')
const os = require('os')
const path = require('path')
const b4a = require('b4a')
const sodium = require('sodium-universal')

const Corestore = require('..')

test('basic get with caching', async function (t) {
  const store = new Corestore(ram)
  const core1a = store.get({ name: 'core-1' })
  const core1b = store.get({ name: 'core-1' })
  const core2 = store.get({ name: 'core-2' })

  await Promise.all([core1a.ready(), core1b.ready(), core2.ready()])

  t.alike(core1a.key, core1b.key)
  t.unlike(core1a.key, core2.key)

  t.ok(core1a.writable)
  t.ok(core1b.writable)

  t.is(store.cores.size, 2)
})

test('basic get with custom keypair', async function (t) {
  const store = new Corestore(ram)
  const kp1 = crypto.keyPair()
  const kp2 = crypto.keyPair()

  const core1 = store.get(kp1)
  const core2 = store.get(kp2)
  await Promise.all([core1.ready(), core2.ready()])

  t.alike(core1.key, kp1.publicKey)
  t.alike(core2.key, kp2.publicKey)
  t.ok(core1.writable)
  t.ok(core2.writable)
})

test('get with createIfMissing=false throws if new core', async function (t) {
  const store = new Corestore(ram)
  const core1a = store.get({ name: 'core-1', createIfMissing: false })

  await t.exception(core1a.ready(), 'No Hypercore is stored here')
})

test('get with createIfMissing=false works if no new core', async function (t) {
  const store = new Corestore(ram)
  const name = 'core-1'

  const core1 = store.get({ name })
  await core1.ready()

  const core1Too = store.get({ name, createIfMissing: false })
  await t.execution(core1Too.ready())
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

  t.absent(core1.key.equals(core2.key))
  t.ok(core1.key.equals(core3.key))
  t.ok(core1.writable)
  t.ok(core2.writable)
  t.ok(core3.writable)
  t.is(store.cores.size, 2)

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

  t.alike(await core3.get(0), Buffer.from('hello'))
  t.alike(await core4.get(0), Buffer.from('world'))
})

test('replicating cores created after replication begins', async function (t) {
  const store1 = new Corestore(ram)
  const store2 = new Corestore(ram)

  const s = store1.replicate(true, { live: true })
  s.pipe(store2.replicate(false, { live: true })).pipe(s)

  const core1 = store1.get({ name: 'core-1' })
  const core2 = store1.get({ name: 'core-2' })
  await core1.append('hello')
  await core2.append('world')

  const core3 = store2.get({ key: core1.key })
  const core4 = store2.get({ key: core2.key })

  t.alike(await core3.get(0), Buffer.from('hello'))
  t.alike(await core4.get(0), Buffer.from('world'))
})

test('replicating cores using discovery key hook', async function (t) {
  const dir = tmpdir()
  let store1 = new Corestore(dir)
  const store2 = new Corestore(ram)

  const core = store1.get({ name: 'main' })
  await core.append('hello')
  const key = core.key

  await store1.close()
  store1 = new Corestore(dir)

  const s = store1.replicate(true, { live: true })
  s.pipe(store2.replicate(false, { live: true })).pipe(s)

  const core2 = store2.get(key)
  t.alike(await core2.get(0), Buffer.from('hello'))
})

test('nested namespaces', async function (t) {
  const store = new Corestore(ram)
  const ns1a = store.namespace('ns1').namespace('a')
  const ns1b = store.namespace('ns1').namespace('b')

  const core1 = ns1a.get({ name: 'main' })
  const core2 = ns1b.get({ name: 'main' })
  await Promise.all([core1.ready(), core2.ready()])

  t.not(core1.key.equals(core2.key))
  t.ok(core1.writable)
  t.ok(core2.writable)
  t.is(store.cores.size, 2)
})

test('core uncached when all sessions close', async function (t) {
  const store = new Corestore(ram)
  const core1 = store.get({ name: 'main' })
  await core1.ready()
  t.is(store.cores.size, 1)
  await core1.close()
  t.is(store.cores.size, 0)
})

test('writable core loaded from name userData', async function (t) {
  const dir = tmpdir()

  let store = new Corestore(dir)
  let core = store.get({ name: 'main' })
  await core.ready()
  const key = core.key

  t.ok(core.writable)
  await core.append('hello')
  t.is(core.length, 1)

  await store.close()
  store = new Corestore(dir)
  core = store.get(key)
  await core.ready()

  t.ok(core.writable)
  await core.append('world')
  t.is(core.length, 2)
  t.alike(await core.get(0), Buffer.from('hello'))
  t.alike(await core.get(1), Buffer.from('world'))
})

test('writable core loaded from name and namespace userData', async function (t) {
  const dir = tmpdir()

  let store = new Corestore(dir)
  let core = store.namespace('ns1').get({ name: 'main' })
  await core.ready()
  const key = core.key

  t.ok(core.writable)
  await core.append('hello')
  t.is(core.length, 1)

  await store.close()
  store = new Corestore(dir)
  core = store.get(key)
  await core.ready()

  t.ok(core.writable)
  await core.append('world')
  t.is(core.length, 2)
  t.alike(await core.get(0), Buffer.from('hello'))
  t.alike(await core.get(1), Buffer.from('world'))
})

test('storage locking', async function (t) {
  const dir = tmpdir()

  const store1 = new Corestore(dir)
  await store1.ready()

  const store2 = new Corestore(dir)
  try {
    await store2.ready()
    t.fail('dir should have been locked')
  } catch {
    t.pass('dir was locked')
  }
})

test('cores close when their last referencing namespace closes', async function (t) {
  const store = new Corestore(ram)
  const ns1 = store.namespace('ns1')
  const core1 = ns1.get({ name: 'core-1' })
  const core2 = ns1.get({ name: 'core-2' })
  await Promise.all([core1.ready(), core2.ready()])

  const core3 = store.get(core1.key)
  const core4 = store.get(core2.key)
  await Promise.all([core3.ready(), core4.ready()])

  await ns1.close()

  t.is(store.cores.size, 2)
  t.not(core1.closed)
  t.not(core1.closed)

  await store.close()

  t.is(store.cores.size, 0)
  t.ok(core1.closed)
  t.ok(core2.closed)
})

test('findingPeers', async function (t) {
  t.plan(6)

  const store = new Corestore(ram)

  const ns1 = store.namespace('ns1')
  const ns2 = store.namespace('ns2')

  const a = ns1.get(Buffer.alloc(32).fill('a'))
  const b = ns2.get(Buffer.alloc(32).fill('b'))

  const done = ns1.findingPeers()

  let aUpdated = false
  let bUpdated = false
  let cUpdated = false

  const c = ns1.get(Buffer.alloc(32).fill('c'))

  a.update().then(function (bool) {
    aUpdated = true
  })

  b.update().then(function (bool) {
    bUpdated = true
  })

  c.update().then(function (bool) {
    cUpdated = true
  })

  await new Promise(resolve => setImmediate(resolve))

  t.is(aUpdated, false)
  t.is(bUpdated, true)
  t.is(cUpdated, false)

  done()

  await new Promise(resolve => setImmediate(resolve))

  t.is(aUpdated, true)
  t.is(bUpdated, true)
  t.is(cUpdated, true)
})

test('different primary keys yield different keypairs', async function (t) {
  const pk1 = randomBytes(32)
  const pk2 = randomBytes(32)
  t.unlike(pk1, pk2)

  const store1 = new Corestore(ram, { primaryKey: pk1 })
  const store2 = new Corestore(ram, { primaryKey: pk2 })

  const kp1 = await store1.createKeyPair('hello')
  const kp2 = await store2.createKeyPair('hello')

  t.unlike(kp1.publicKey, kp2.publicKey)
})

test('keypair auth sign', async function (t) {
  const store = new Corestore(ram)
  const keyPair = await store.createKeyPair('foo')
  const message = b4a.from('hello world')

  const sig = keyPair.auth.sign(message)

  t.is(sig.length, 64)
  t.ok(crypto.verify(message, sig, keyPair.publicKey))
  t.absent(crypto.verify(message, b4a.alloc(64), keyPair.publicKey))
})

test('keypair auth verify', async function (t) {
  const store = new Corestore(ram)
  const keyPair = await store.createKeyPair('foo')
  const message = b4a.from('hello world')

  const sig = crypto.sign(message, keyPair.secretKey)

  t.is(sig.length, 64)
  t.ok(keyPair.auth.verify(message, sig))
  t.absent(keyPair.auth.verify(message, b4a.alloc(64)))
})

test('core caching after reopen regression', async function (t) {
  const store = new Corestore(ram)
  const core = store.get({ name: 'test-core' })
  await core.ready()

  core.close()
  await core.opening

  const core2 = store.get({ name: 'test-core' })
  await core2.ready()

  t.pass('did not infinite loop')
})

test('closing a namespace does not close the root corestore', async function (t) {
  const store = new Corestore(ram)
  const core1 = store.get({ name: 'core-1' })
  await core1.ready()

  const ns = store.namespace('test-namespace')
  const core2 = ns.get(core1.key)
  await core2.ready()

  await ns.close()
  t.is(core1.closed, false)
  t.is(core2.closed, true)
})

test('open a new session concurrently with a close should throw', async function (t) {
  const store = new Corestore(ram)
  const ns = store.namespace('test-namespace')

  const core1 = ns.get({ name: 'core-1' })
  store.close()

  try {
    await core1.ready()
    t.fail('core1 should not have opened')
  } catch {
    t.pass('core1 did not open after corestore close')
  }
})

test('closing the root corestore closes all sessions', async function (t) {
  const store = new Corestore(ram)
  const ns = store.namespace('test-namespace')

  const core1 = store.get({ name: 'core-1' })
  const core2 = store.get({ name: 'core-2' })
  await Promise.all([core1.ready(), core2.ready()])

  const core3 = ns.get(core1.key)
  const core4 = ns.get(core2.key)
  await Promise.all([core3.ready(), core4.ready()])

  await store.close()

  t.is(core1.closed, true)
  t.is(core2.closed, true)

  try {
    const core5 = ns.get({ name: 'core-3' })
    await core5.ready()
    t.fail('core5 should not have opened after corestore close')
  } catch (err) {
    t.pass('core5 did not open after corestore close')
  }
})

test('generated namespaces/keys match fixtures', async function (t) {
  const store = new Corestore(ram, { primaryKey: b4a.alloc(32) })
  const ns1 = store.namespace('hello')
  const ns2 = ns1.namespace('world')

  const core1 = store.get({ name: 'core-1' })
  const core2 = ns1.get({ name: 'core-2' })
  const core3 = ns2.get({ name: 'core-3' })
  await Promise.all([core1.ready(), core2.ready(), core3.ready()])

  // Ensure the generated namespaces are correct
  t.snapshot(store._namespace, 'store namespace')
  t.snapshot(ns1._namespace, 'ns1 namespace')
  t.snapshot(ns2._namespace, 'ns2 namespace')

  // Ensure the core keys are correct
  t.snapshot(core1.key, 'core1 key')
  t.snapshot(core2.key, 'core2 key')
  t.snapshot(core3.key, 'core3 key')
})

test('session', async function (t) {
  const store1 = new Corestore(ram)
  const store2 = new Corestore(ram, { primaryKey: Buffer.alloc(32).fill('a') })

  await store1.ready()
  await store2.ready()

  const ns1 = store1.namespace('a')
  const ns2 = store2.namespace('a')
  const ns3 = ns1.session({ primaryKey: store2.primaryKey })

  const ns1core = ns1.get({ name: 'main' })
  const ns2core = ns2.get({ name: 'main' })
  const ns3core = ns3.get({ name: 'main' })
  await Promise.all([ns1core.ready(), ns2core.ready(), ns3core.ready()])

  t.unlike(ns3core.key, ns1core.key, 'override primaryKey')
  t.alike(ns3core.key, ns2core.key, 'Inherit namespace')

  const reset = ns3.session({ primaryKey: store1.primaryKey, namespace: null })

  const core0 = store1.get({ name: 'main' })
  const core1 = reset.get({ name: 'main' })
  await Promise.all([core0.ready(), core1.ready()])

  t.alike(core1.key, core0.key, 'reset namespace and primaryKey')

  await core1.append('hello')

  const remote = new Corestore(ram)
  const s = remote.replicate(true)
  s.pipe(store1.replicate(false)).pipe(s)

  const clone1 = remote.get({ key: core1.key })
  t.alike(await clone1.get(0), Buffer.from('hello'), 'share replication streams with a session')
})

test('core-open and core-close events', async function (t) {
  const store = new Corestore(ram)
  const expected = [crypto.keyPair(), crypto.keyPair()]
  let opened = 0
  let closed = 0

  store.on('core-open', core => {
    t.alike(core.key, expected[opened++].publicKey)
  })
  store.on('core-close', core => {
    t.alike(core.key, expected[closed++].publicKey)
  })

  const core1 = store.get(expected[0])
  const core2 = store.get(expected[1])
  await Promise.all([core1.ready(), core2.ready()])
  t.is(opened, 2)

  await core1.close()
  t.is(closed, 1)
  await core2.close()
  t.is(closed, 2)
})

function tmpdir () {
  return path.join(os.tmpdir(), 'corestore-' + Math.random().toString(16).slice(2))
}

function randomBytes (n) {
  const buf = b4a.allocUnsafe(n)
  sodium.randombytes_buf(buf)
  return buf
}
