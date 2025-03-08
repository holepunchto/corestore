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

  const dir = await tmp(t)
  const store = new Corestore(dir)

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
    if (audit.droppedBits || audit.droppedBlocks || audit.droppedTreeNodes || audit.corrupt) {
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

async function create (t) {
  const dir = await tmp(t)
  const store = new Corestore(dir)
  t.teardown(() => store.close())
  return store
}
