const test = require('brittle')
const RAM = require('random-access-memory')
const Corestore = require('../')

test('basic exclusive mode', async function (t) {
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

test('exclusive always releases the lock on close', async function (t) {
  t.plan(2)

  const store = new Corestore(RAM)

  const a1 = store.get({ name: 'a', exclusive: true })
  await a1.ready()

  const r = store.get({ key: a1.key, writable: false })
  await r.ready()

  const a2 = store.get({ key: a1.key, exclusive: true })
  const a3 = store.get({ key: a1.key, exclusive: true })

  a2.ready().then(() => {
    t.pass('a2 got it')
    a2.close()
  })

  a3.ready().then(() => {
    t.pass('a3 got it')
    r.close()
  })

  await new Promise(resolve => setImmediate(resolve))
  await a1.close()
})

test('session on an exclusive mode core', async function (t) {
  t.plan(1)

  const store = new Corestore(RAM)

  const a1 = store.get({ name: 'a', exclusive: true })
  const a2 = store.get({ name: 'a', exclusive: true })

  await a1.ready()

  a2.session().ready().then(() => {
    t.ok(a1.closed, 'waited for other exclusive session')
  })

  await new Promise(resolve => setImmediate(resolve))
  await a1.close()
})

test('sessions on exclusive mode do not deadlock', async function (t) {
  const store = new Corestore(RAM)

  const ex = store.get({ name: 'a', exclusive: true })
  const s1 = ex.session()
  const s2 = ex.session()

  await s1.ready()
  await s2.ready()
  await ex.ready()

  t.ok(ex.writable)
  t.ok(s1.writable)
  t.ok(s2.writable)
})
