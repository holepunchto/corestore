const test = require('brittle')
const b4a = require('b4a')
const { create, toArray, includesKey } = require('./helpers')

test('notify handler - empty when no cores in group', async function (t) {
  const store = await create(t)
  const handle = store.notifyGroup(b4a.alloc(32, 1))
  t.teardown(() => handle.destroy())
  t.alike(await toArray(handle.updates()), [])
})

test('notify handler - returns keys of cores that have been active in group', async function (t) {
  const store = await create(t)
  const topic = b4a.alloc(32, 1)

  const a = store.get({ name: 'a', group: topic })
  const b = store.get({ name: 'b', group: topic })
  await a.append('hello')
  await b.append('world')

  const handle = store.notifyGroup(topic)
  t.teardown(() => handle.destroy())

  const keys = await toArray(handle.updates())
  t.ok(includesKey(keys, a.key), 'a key present')
  t.ok(includesKey(keys, b.key), 'b key present')
})

test('notify handler - stream can be abandoned mid-read without hanging', async function (t) {
  const store = await create(t)
  const topic = b4a.alloc(32, 1)
  const a = store.get({ name: 'a', group: topic })
  await a.append('hello')

  const handle = store.notifyGroup(topic)
  t.teardown(() => handle.destroy())

  const stream = handle.updates()
  const close = new Promise((resolve) => stream.on('close', resolve))
  // user gives up on the stream immediately
  stream.destroy()
  await close
  t.pass('closed cleanly')
})

test('notify handler - multiple independent streams from same handle', async function (t) {
  const store = await create(t)
  const topic = b4a.alloc(32, 1)
  const a = store.get({ name: 'a', group: topic })
  await a.append('hello')

  const handle = store.notifyGroup(topic)
  t.teardown(() => handle.destroy())

  const [keys1, keys2] = await Promise.all([toArray(handle.updates()), toArray(handle.updates())])

  t.ok(includesKey(keys1, a.key), 'a key present in first stream')
  t.ok(includesKey(keys2, a.key), 'a key present in second stream')
})

test('notify handler - destroy stops update events from firing', async function (t) {
  const store = await create(t)
  const topic = b4a.alloc(32, 1)
  const a = store.get({ name: 'a', group: topic })

  const handle = store.notifyGroup(topic)
  let count = 0
  handle.on('update', () => count++)

  await a.append('one')
  handle.destroy()
  await a.append('two')

  t.is(count, 1, 'only fired before destroy')
})

test('notify handler - update events continue on other handles', async function (t) {
  t.plan(1)
  const store = await create(t)
  const topic = b4a.alloc(32, 1)
  const a = store.get({ name: 'a', group: topic })

  const h1 = store.notifyGroup(topic)
  const h2 = store.notifyGroup(topic)
  t.teardown(() => h2.destroy())

  h2.on('update', () => t.pass('h2 still fires'))
  h1.destroy()

  await a.append('hello')
})

test('notify handler - destroy called twice does not throw', async function (t) {
  const store = await create(t)
  const handle = store.notifyGroup(b4a.alloc(32, 1))
  handle.destroy()
  handle.destroy()
  t.pass()
})
