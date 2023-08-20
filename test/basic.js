const test = require('brittle')
const crypto = require('hypercore-crypto')
const ram = require('random-access-memory')
const path = require('path')
const b4a = require('b4a')
const sodium = require('sodium-universal')
const fs = require('fs')
const tmp = require('test-tmp')
const idEncoding = require('hypercore-id-encoding')

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

test('basic get with hex key', async function (t) {
  const store = new Corestore(ram)
  const hexKey = 'a'.repeat(64)

  const core = store.get(hexKey)
  await core.ready()
  t.is(b4a.toString(core.key, 'hex'), hexKey)
})

test('basic get with with explicit hex key', async function (t) {
  const store = new Corestore(ram)
  const hexKey = 'a'.repeat(64)

  const core = store.get({ key: hexKey })
  await core.ready()
  t.is(b4a.toString(core.key, 'hex'), hexKey)
})

test('basic get with z32 key', async function (t) {
  const store = new Corestore(ram)
  const bufKey = b4a.from('a'.repeat(64), 'hex')
  const z32Key = idEncoding.encode(bufKey)
  t.is(z32Key.length, 52, 'Sanity check that it is indeed z32')

  const core = store.get(z32Key)
  await core.ready()
  t.alike(core.key, bufKey)
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
  const dir = await tmp()

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

  await store.close()
})

test('writable core loaded from name and namespace userData', async function (t) {
  const dir = await tmp(t)

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

  await store.close()
})

test('storage locking', async function (t) {
  const dir = await tmp(t)

  const store1 = new Corestore(dir)
  await store1.ready()

  const store2 = new Corestore(dir)
  try {
    await store2.ready()
    t.fail('dir should have been locked')
  } catch {
    t.pass('dir was locked')
  }

  await store1.close()
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

test('core-open and core-close are emitted on all root sessions', async function (t) {
  const root = new Corestore(ram)
  const session = root.session()
  const namespace = root.namespace('test-namespace')

  const rootExpected = [crypto.keyPair(), crypto.keyPair()]
  const sessionExpected = [...rootExpected]
  const namespaceExpected = [...rootExpected]

  root.on('core-open', core => {
    t.alike(core.key, rootExpected.shift().publicKey)
  })
  session.on('core-open', core => {
    t.alike(core.key, sessionExpected.shift().publicKey)
  })
  namespace.on('core-open', core => {
    t.alike(core.key, namespaceExpected.shift().publicKey)
  })

  const core1 = namespace.get(rootExpected[0])
  const core2 = namespace.get(rootExpected[1])
  await Promise.all([core1.ready(), core2.ready()])

  t.is(rootExpected.length, 0)
  t.is(sessionExpected.length, 0)
  t.is(namespaceExpected.length, 0)
})

test('root store sessions are cleaned up', async function (t) {
  const root = new Corestore(ram)
  const session1 = root.session()
  t.is(root._rootStoreSessions.size, 1)
  await session1.close()
  t.is(root._rootStoreSessions.size, 0)
})

test('opening a namespace from a bootstrap core', async function (t) {
  const store = new Corestore(ram)

  const ns1 = store.namespace(crypto.randomBytes(32))
  const bootstrap1 = ns1.get({ name: 'bootstrap' })

  const ns2 = store.namespace(bootstrap1)
  const bootstrap2 = ns2.get({ name: 'bootstrap' })

  await Promise.all([bootstrap1.ready(), bootstrap2.ready()])
  t.alike(bootstrap1.key, bootstrap2.key)
})

test('opening a namespace from an invalid bootstrap core is a no-op', async function (t) {
  const store1 = new Corestore(ram)
  const store2 = new Corestore(ram)

  const ns1 = store1.namespace(crypto.randomBytes(32))
  const bootstrap1 = ns1.get({ name: 'bootstrap' })
  await bootstrap1.ready()

  const bootstrap2 = store2.get(bootstrap1.key)
  const ns2 = store2.namespace(bootstrap2)
  await ns2.ready()

  t.alike(ns2._namespace, store2._namespace)
})

test('hypercore purge behaviour interacts correctly with corestore', async function (t) {
  const dir = await tmp(t)

  const store = new Corestore(dir)
  const core = store.get({ name: 'core' })
  await core.append('block 0')

  const key = b4a.toString(core.discoveryKey, 'hex')
  const coreParentDir = path.join(dir, 'cores', key.slice(0, 2), key.slice(2, 4))
  const coreDir = path.join(coreParentDir, key)

  t.is(fs.existsSync(coreDir), true) // Sanity check
  await core.purge()

  t.is(fs.existsSync(coreDir), false)

  // The intermediate dirs are removed too, if they are now empty
  t.is(fs.existsSync(coreParentDir), false)
  t.is(fs.existsSync(path.dirname(coreParentDir)), false)

  t.is(fs.existsSync(dir), true) // Sanity check: corestore itself not cleaned up

  await store.close()
})

test('basic writable option', async function (t) {
  t.plan(3)

  const store = new Corestore(ram)

  const a = store.get({ name: 'a', writable: false })
  await a.ready()
  t.is(a.writable, false)

  try {
    await a.append('abc')
    t.fail('Should have failed')
  } catch (err) {
    t.is(err.code, 'SESSION_NOT_WRITABLE')
  }

  const b = store.get({ name: 'b' })
  await b.ready()
  t.is(b.writable, true)
  await b.append('abc')
})

test('core inherits writable option from the session', async function (t) {
  t.plan(2)

  const store = new Corestore(ram)
  const s = store.session({ writable: false })

  const core = s.get({ name: 'a' })
  await core.ready()
  t.is(core.writable, false)

  try {
    await core.append('abc')
    t.fail('Should have failed')
  } catch (err) {
    t.is(err.code, 'SESSION_NOT_WRITABLE')
  }
})

test('store session inherits writable option from parent session', async function (t) {
  t.plan(1)

  const store = new Corestore(ram)

  const s = store.session({ writable: false })
  const s1 = s.session()

  const core = s1.get({ name: 'a' })
  await core.ready()
  t.is(core.writable, false)
})

test('session get after closing it', async function (t) {
  const store = new Corestore(ram)

  const session = store.session()
  await session.close()

  try {
    session.get({ name: 'a' })
    t.fail('Should have failed')
  } catch (err) {
    t.is(err.message, 'The corestore is closed')
  }

  await store.close()
})

test('close timing regression', async function (t) {
  const store = new Corestore(ram.reusable())

  const a1 = store.get({ name: 'a' })

  await a1.ready()

  const a2 = store.get({ name: 'a' })

  await Promise.resolve()
  await Promise.resolve()

  a1.close()

  await t.execution(a2.append('foo'), 'can append')
  await store.close()
})

test('session that overtakes', async function (t) {
  const store = new Corestore(ram)

  const session = store.session()
  await session.close()

  t.is(store.closed, false)

  const newStore = store.session({ detach: false })
  await newStore.close()

  t.is(store.closed, true)
})

function randomBytes (n) {
  const buf = b4a.allocUnsafe(n)
  sodium.randombytes_buf(buf)
  return buf
}
