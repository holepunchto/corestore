const test = require('brittle')
const RAM = require('random-access-memory')
const Corestore = require('../index.js')

test('basic', async function (t) {
  const storage = RAM.reusable()

  const store = new Corestore(storage)

  const a = store.get({ name: 'test' })
  await a.ready()
  t.is(a.writable, true)
  await a.close()

  await store.close()

  const store2 = new Corestore(storage, { writable: false })

  const b = store2.get({ key: a.key })
  await b.ready()
  t.is(b.writable, false)

  for (const [, core] of store2.cores) {
    await core.ready()
    t.is(core.writable, true)
  }

  await b.close()

  t.alike(a.key, b.key)

  await store2.close()
})
