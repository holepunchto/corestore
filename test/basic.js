const test = require('brittle')
const b4a = require('b4a')
const tmp = require('test-tmp')
const Rache = require('rache')
const Hypercore = require('hypercore')
const crypto = require('hypercore-crypto')

const Corestore = require('../')

test('basic', async function (t) {
  const store = await create(t)

  const core = store.get({ name: 'test' })
  const core2 = store.get({ name: 'test' })

  await core.ready()
  await core2.ready()

  t.alike(core.key, core2.key, 'same core')
  t.is(core.core, core2.core, 'same internal core')

  t.is(core.manifest.signers.length, 1)
  t.unlike(core.key, core.manifest.signers[0].publicKey)

  await core.close()
  await core2.close()
})

test('basic non parallel', async function (t) {
  const store = await create(t)

  const core = store.get({ name: 'test' })
  await core.ready()

  const core2 = store.get({ name: 'test' })
  await core2.ready()

  t.alike(core.key, core2.key, 'same core')
  t.is(core.core, core2.core, 'same internal core')

  t.is(core.manifest.signers.length, 1)
  t.unlike(core.key, core.manifest.signers[0].publicKey)

  await core.close()
  await core2.close()
})

test('pass primary key', async function (t) {
  const primaryKey = b4a.alloc(32, 1)
  let key = null

  {
    const dir = await tmp(t)
    const store = new Corestore(dir, { primaryKey })

    t.alike(store.primaryKey, primaryKey)

    const core = store.get({ name: 'test' })
    await core.ready()

    key = core.key

    await core.close()
    await store.close()

    const store2 = new Corestore(dir)
    await store2.ready()

    t.alike(store2.primaryKey, primaryKey)

    await store2.close()
  }

  {
    const dir = await tmp(t)

    const store = new Corestore(dir, { primaryKey })

    const core = store.get({ name: 'test' })
    await core.ready()

    t.alike(core.key, key)

    await core.close()
    await store.close()
  }
})

test('global cache is passed down', async function (t) {
  const dir = await tmp(t)
  const store = new Corestore(dir, { globalCache: new Rache({ maxSize: 4 }) })

  t.ok(store.globalCache)

  const core = store.get({ name: 'hello' })
  await core.ready()

  t.ok(core.globalCache)

  await core.close()
  await store.close()
})

test('session pre ready', async function (t) {
  const dir = await tmp(t)
  const store = new Corestore(dir)

  const a = store.get({ name: 'test' })
  const b = a.session()

  await a.ready()
  await b.ready()

  await a.close()
  await b.close()

  await store.close()
})

test('weak ref to react to cores opening', async function (t) {
  t.plan(2)

  const s = setInterval(() => {}, 1000)

  const dir = await tmp(t)
  const store = new Corestore(dir)

  t.teardown(() => clearInterval(s))
  t.teardown(() => store.close())

  store.watch(function (core) {
    const s = new Hypercore({ core, weak: true })

    t.pass('weak ref opened passively')

    s.on('close', function () {
      t.pass('weak ref closed passively')
    })
  })

  const core = store.get({ name: 'hello' })

  await core.ready()
  await core.close()
})

test('session of hypercore sessions are tracked in corestore sessions', async function (t) {
  const dir = await tmp(t)
  const store = new Corestore(dir)

  const session = store.session()

  const closed = t.test('session')
  closed.plan(2)

  const a = session.get({ name: 'test' })
  const b = a.session()

  a.on('close', () => closed.pass('a closed (explicit)'))
  b.on('close', () => closed.pass('b closed (implicit)'))

  await a.ready()
  await b.ready()

  await session.close()

  await closed

  await store.close()
})

test('named cores are stable', async function (t) {
  const dir = await tmp(t)
  const store = new Corestore(dir)

  await store.ready()
  const keyPair = await store.createKeyPair('test')

  const oldManifest = {
    version: 0,
    signers: [{ publicKey: keyPair.publicKey }]
  }

  const core = store.get({ name: 'test', manifest: oldManifest })
  await core.ready()

  const expected = core.manifest

  await core.close()
  await store.close()

  const fresh = new Corestore(dir)

  const freshCore = fresh.get({ name: 'test' })
  await freshCore.ready()

  t.alike(freshCore.manifest, expected)

  await freshCore.close()
  await fresh.close()
})

test('replicates', async function (t) {
  const store = new Corestore(await tmp(t))

  const a = store.get({ name: 'foo' })
  await a.append('hello')
  await a.close()

  const store2 = new Corestore(await tmp(t))
  const clone = store.get(a.key)

  const s1 = store2.replicate(true)
  const s2 = store.replicate(false)

  s1.pipe(s2).pipe(s1)

  t.alike(await clone.get(0), b4a.from('hello'))

  s1.destroy()
  s2.destroy()

  await store.close()
  await store2.close()
})

test('if key is passed, its available immediately', async function (t) {
  const store = new Corestore(await tmp(t))

  const a = store.get({ key: b4a.alloc(32) })
  t.alike(a.key, b4a.alloc(32))

  await a.close()
  await store.close()
})

test('finding peers (compat)', async function (t) {
  const store = new Corestore(await tmp(t))

  const done = store.findingPeers()

  const core = store.get({ key: b4a.alloc(32) })
  let waited = false

  setTimeout(() => {
    waited = true
    done()
  }, 500)

  await core.update()
  t.ok(waited, 'waited')

  await store.close()
})

test('audit', async function (t) {
  const store = new Corestore(await tmp(t))

  const a = store.get({ keyPair: crypto.keyPair() })
  const b = store.get({ keyPair: crypto.keyPair() })
  const c = store.get({ name: 'test' })
  const d = store.get({ name: 'another' })

  for (let i = 0; i < 100; i++) {
    if (i < 20) await a.append(i.toString())
    if (i < 40) await b.append(i.toString())
    if (i < 80) await c.append(i.toString())
    await d.append(i.toString())
  }

  let n = 0
  for await (const { audit } of store.audit()) {
    n++
    if (
      audit.droppedBits ||
      audit.droppedBlocks ||
      audit.droppedTreeNodes ||
      audit.corrupt
    ) {
      t.fail('bad core')
    }
  }

  t.is(n, 4)

  await a.close()
  await b.close()
  await c.close()
  await d.close()
  await store.close()
})

test('open by discovery key', async function (t) {
  const store = new Corestore(await tmp(t))

  const a = store.get({ discoveryKey: b4a.alloc(32) })

  try {
    await a.ready()
  } catch {
    t.ok('should fail')
  }

  const keyPair = crypto.keyPair()
  const manifest = {
    signers: [{ publicKey: keyPair.publicKey }]
  }

  const key = Hypercore.key(manifest)
  const discoveryKey = Hypercore.discoveryKey(key)

  const a1 = store.get({ discoveryKey })
  const a2 = store.get({ discoveryKey, key, manifest })

  try {
    await a1.ready()
  } catch {}

  await a2.ready()
  t.pass('a2 worked')

  a2.close()
  await store.close()
})

test('basic - open with a keypair, read-only session concurrently opened', async (t) => {
  const store = await create(t)

  {
    const keyPair = crypto.keyPair()
    const key = Hypercore.key({
      version: store.manifestVersion,
      signers: [{ signature: 'ed25519', publicKey: keyPair.publicKey }]
    })

    const core1 = store.get({ key, keyPair })
    await core1.ready()

    t.ok(core1.manifest)
  }

  {
    const keyPair = crypto.keyPair()
    const key = Hypercore.key({
      version: store.manifestVersion,
      signers: [{ signature: 'ed25519', publicKey: keyPair.publicKey }]
    })

    const core1 = store.get({ key })

    await core1.ready()
    t.absent(core1.manifest)

    const core2 = store.get({
      key,
      manifest: { signers: [{ publicKey: keyPair.publicKey }] },
      keyPair
    })
    await core2.ready()

    t.ok(core2.manifest)
  }
})

test('can set default manifest', async function (t) {
  const store = await create(t)

  const test = store.get({ name: 'test' })
  await test.ready()

  const key = test.key
  t.is(test.manifest.version, store.manifestVersion)

  await test.close()

  const other = store.manifestVersion === 1 ? 2 : 1
  const updated = store.session({ manifestVersion: other })

  const test2 = updated.get({ name: 'test' })
  await test2.ready()

  t.is(test2.manifest.version, store.manifestVersion)
  t.alike(test2.key, key)

  const test3 = updated.get({ name: 'test3' })
  await test3.ready()

  t.is(test3.manifest.version, other)

  await updated.close()
  await store.close()
})

test('list stream', async function (t) {
  const store = await create(t)
  const namespace = store.namespace('test')
  t.teardown(() => store.close())

  const expectedAll = []
  const expeactedNamespace = []

  for (let i = 0; i < 10; i++) {
    if (i < 5) {
      const core = store.get({ name: `core${i}` })
      await core.ready()
      expectedAll.push(core.discoveryKey)
    } else {
      const core = namespace.get({ name: `core${i}` })
      await core.ready()
      expectedAll.push(core.discoveryKey)
      expeactedNamespace.push(core.discoveryKey)
    }
  }

  const discoveryKeysAll = await toArray(store.list())
  const discoveryKeysNamespace = await toArray(store.list(namespace.ns))

  t.comment('stream without arg sends all core discoveryKeys')
  t.alike(
    discoveryKeysAll
      .slice()
      .map((item) => item)
      .sort((a, b) => Buffer.compare(a, b)),
    expectedAll.slice().sort((a, b) => Buffer.compare(a, b))
  )
  t.comment('stream without namespace arg sends namespace core discoveryKeys')
  t.alike(
    discoveryKeysNamespace
      .slice()
      .map((item) => item)
      .sort((a, b) => Buffer.compare(a, b)),
    expeactedNamespace.slice().sort((a, b) => Buffer.compare(a, b))
  )

  t.comment('with and withough namespace dont return the same')
  t.unlike(
    discoveryKeysAll.slice().sort((a, b) => Buffer.compare(a, b)),
    discoveryKeysNamespace.slice().sort((a, b) => Buffer.compare(a, b))
  )

  t.comment('without namespace includes all of with namespace')
  t.ok(
    discoveryKeysNamespace.every((b) =>
      discoveryKeysAll.some((a) => a.equals(b))
    )
  )
})

test('manifest is persisted', async function (t) {
  const dir = await tmp()

  const random = crypto.randomBytes(32)
  const manifest = {
    quorum: 1,
    signers: [{ publicKey: random }]
  }
  const key = Hypercore.key(manifest)

  {
    const store = new Corestore(dir)

    const a = store.get({ key })
    await a.ready()

    t.is(a.manifest, null)

    const a2 = store.get({ manifest })
    await a2.ready()

    t.not(a.manifest, null)
    t.not(a2.manifest, null)

    await a.close()
    await a2.close()

    await store.close()
  }

  {
    const store = new Corestore(dir)

    const a = store.get({ key })
    await a.ready()
    await a.close()

    t.not(a.manifest, null)

    await store.close()
  }
})

function toArray(stream) {
  return new Promise((resolve, reject) => {
    const all = []
    stream.on('data', (data) => {
      all.push(data)
    })
    stream.on('end', () => {
      resolve(all)
    })
    stream.on('error', (err) => {
      reject(err)
    })
  })
}

async function create(t) {
  const dir = await tmp(t)
  const store = new Corestore(dir)
  t.teardown(() => store.close())
  return store
}
