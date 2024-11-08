const test = require('brittle')
const tmp = require('test-tmp')
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

async function create (t) {
  const dir = await tmp(t)
  const store = new Corestore(dir)
  t.teardown(() => store.close())
  return store
}
