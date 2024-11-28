const test = require('brittle')
const b4a = require('b4a')
const tmp = require('test-tmp')
const Rache = require('rache')
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

test('session from a core', async function (t) {
  const store = await create(t)

  const ns = store.namespace('yo')
  const core = ns.get({ name: 'test' })

  const session = store.namespace(core)

  const core2 = session.get({ name: 'test' })

  await core.ready()
  await core2.ready()

  t.is(core.id, core2.id)

  await core.close()
  await core2.close()

  await session.close()
  await ns.close()
  await store.close()
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

test('setNamespace', async function (t) {
  const dir = await tmp(t)
  const store = new Corestore(dir)

  const ns = store.namespace('hello world')

  const core = ns.get({ name: 'cool burger' })

  const session = store.session()

  session.setNamespace(core)

  const core2 = session.get({ name: 'cool burger' })

  await core.ready()
  await core2.ready()

  t.is(core.id, core2.id)

  await core.close()
  await core2.close()
  await session.close()
  await ns.close()
  await store.close()
})

test('setNamespace before ready', async function (t) {
  const dir = await tmp(t)
  const store = new Corestore(dir)

  const core = store.get({ name: 'cool burger' })

  store.setNamespace(core)

  const core2 = store.get({ name: 'cool burger' })

  await core.ready()
  await core2.ready()

  t.is(core.id, core2.id)

  await core.close()
  await core2.close()
  await store.close()
})

async function create (t) {
  const dir = await tmp(t)
  const store = new Corestore(dir)
  t.teardown(() => store.close())
  return store
}
