const test = require('brittle')
const RAM = require('random-access-memory')
const Rache = require('rache')

const Corestore = require('..')

test('core cache', async function (t) {
  const store = new Corestore(RAM, { cache: true })

  const core = store.get({ name: 'core' })
  await core.append(['a', 'b', 'c'])

  const p = core.get(0)
  const q = core.get(0)

  t.is(await p, await q)
})

test('clear cache on truncate', async function (t) {
  const store = new Corestore(RAM, { cache: true })

  const core = store.get({ name: 'core' })
  await core.append(['a', 'b', 'c'])

  const p = core.get(0)

  await core.truncate(0)
  await core.append('d')

  const q = core.get(0)

  t.alike(await p, Buffer.from('a'))
  t.alike(await q, Buffer.from('d'))
})

test('core cache on namespace', async function (t) {
  const store = new Corestore(RAM, { cache: true })
  const ns1 = store.namespace('test-namespace-1')

  const c1 = store.get({ name: 'test-core' })
  const c2 = ns1.get({ name: 'test-core' })

  await Promise.all([c1.ready(), c2.ready()])

  t.ok(c1.cache)
  t.ok(c2.cache)
})

test('global cache used by all derived cores', async t => {
  const globalCache = new Rache()
  const store = new Corestore(RAM, { globalCache })

  const core = store.get({ name: 'core' })
  await core.ready()
  const core2 = store.get({ name: 'core2' })
  await core2.ready()

  t.is(core.globalCache, globalCache, 'passed to generated core')
  t.is(core.globalCache, core2.globalCache, 'sanity check')
})
