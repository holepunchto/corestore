const test = require('brittle')
const b4a = require('b4a')
const tmp = require('test-tmp')
const Rache = require('rache')
const Hypercore = require('hypercore')
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

async function create (t) {
  const dir = await tmp(t)
  const store = new Corestore(dir)
  t.teardown(() => store.close())
  return store
}
