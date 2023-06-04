const test = require('brittle')
const RAM = require('random-access-memory')
const Corestore = require('../')

test('basic exlusive mode', async function (t) {
  t.plan(1)

  const store = new Corestore(RAM)

  const a1 = store.get({ name: 'a', exclusive: true })
  const a2 = store.get({ name: 'a', exclusive: true })

  await a1.ready()

  a2.ready().then(() => {
    t.ok(a1.closed, 'waited for other exclusive session')
  })

  await new Promise(resolve => setImmediate(resolve))
  await a1.close()
})

test('exclusive works for names and keys', async function (t) {
  t.plan(1)

  const store = new Corestore(RAM)

  const a1 = store.get({ name: 'a', exclusive: true })
  await a1.ready()

  const a2 = store.get({ key: a1.key, exclusive: true })

  a2.ready().then(() => {
    t.ok(a1.closed, 'waited for other exclusive session')
  })

  await new Promise(resolve => setImmediate(resolve))
  await a1.close()
})

test('exclusive when other session is closing', async function (t) {
  t.plan(1)

  const store = new Corestore(RAM)

  const a1 = store.get({ name: 'a', exclusive: true })
  await a1.ready()

  a1.close() // trigger early

  const a2 = store.get({ key: a1.key, exclusive: true })

  a2.ready().then(() => {
    t.ok(a1.closed, 'waited for other exclusive session')
  })
})

test('exclusive only for writable sessions', async function (t) {
  t.plan(1)

  const store = new Corestore(RAM)

  const a1 = store.get({ name: 'a', exclusive: true })
  await a1.ready()

  const a2 = store.get({ key: a1.key, exclusive: true, writable: false })

  a2.ready().then(() => {
    t.ok(!a1.closed, 'did not wait since not writable')
  })

  await new Promise(resolve => setImmediate(resolve))
  await a1.close()
})
