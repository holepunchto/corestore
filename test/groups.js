const test = require('brittle')
const b4a = require('b4a')

const Corestore = require('../')
const { create, toArray, includesKey } = require('./helpers')
const GroupNotifyHandle = require('../lib/notify')

test('groups', async function (t) {
  t.plan(3)

  // source
  const store = await create(t)

  const a = store.get({ name: 'foo' })
  await a.append('hello')

  const b = store.get({ name: 'bar' })
  await b.append('world')

  const topic1 = b4a.alloc(32, 1)
  const topic2 = b4a.alloc(32, 2)

  // clones
  const store2 = await create(t)

  const clone = store2.get({ key: a.key, group: topic1 })
  const cloneB = store2.get({ key: b.key, group: topic1 })

  t.comment('replicating')

  const cloneAppended = new Promise((resolve) => clone.on('append', resolve))
  const cloneBAppended = new Promise((resolve) => cloneB.on('append', resolve))

  const s1 = store2.replicate(true)
  const s2 = store.replicate(false)

  s1.pipe(s2).pipe(s1)

  t.comment('waiting for append')

  await cloneAppended
  await cloneBAppended

  s1.destroy()
  s2.destroy()

  const keys = await toArray(store2.notifyGroup(topic1).updates())
  t.ok(includesKey(keys, a.key))
  t.ok(includesKey(keys, b.key))
  t.alike(await toArray(store2.notifyGroup(topic2).updates()), [])
})

test('group - persistance', async function (t) {
  const dir = await t.tmp()

  const topic = b4a.alloc(32, 1)

  const store = await create(t)
  const store2 = new Corestore(dir)

  const a = store.get({ name: 'foo' })
  const b = store.get({ name: 'bar' })

  await a.append('hello')

  const aclone = store2.get({ key: a.key, group: topic })
  store2.get({ key: b.key, group: topic }) // mark in group

  {
    const s1 = store2.replicate(true)
    const s2 = store.replicate(false)

    s1.pipe(s2).pipe(s1)

    await new Promise((resolve) => aclone.on('append', resolve))

    s1.destroy()
    s2.destroy()
  }

  await store2.close()

  const store3 = new Corestore(dir)

  await b.append('goodbye')

  const b2 = store3.get({ key: b.key }) // group remembered
  await b2.ready()

  {
    const s1 = store3.replicate(true)
    const s2 = store.replicate(false)

    s1.pipe(s2).pipe(s1)

    await new Promise((resolve) => b2.on('append', resolve))

    s1.destroy()
    s2.destroy()
  }

  t.alike(await toArray(store3.notifyGroup(topic).updates()), [b.key, a.key])

  await store3.close()
})

test('group-active - fired per core not per session', async function (t) {
  t.plan(1)
  const topic = b4a.alloc(32, 1)

  const store = await create(t)

  const a = store.get({ name: 'foo', group: topic })
  await a.ready()
  t.teardown(() => a.close())
  const a2 = store.get({ name: 'foo' })
  await a2.ready()
  t.teardown(() => a2.close())

  store.on('group-active', (group) => {
    t.is(group, topic, 'group active')
  })

  await a.append('hello')
})

test('group-active - fired via passive', async function (t) {
  t.plan(2)
  const topic = b4a.alloc(32, 1)

  const store = await create(t)

  const dir = await t.tmp()
  let store2 = new Corestore(dir)

  const a = store.get({ name: 'foo', group: topic })
  await a.ready()

  const a2 = store2.get({ key: a.key, group: topic })
  t.teardown(() => a2.close())
  await a2.ready()

  await store2.close()

  store2 = new Corestore(dir)
  t.teardown(() => store2.close())

  store2.on('group-active', (group) => {
    t.alike(group, topic, 'group active from passive core')
  })
  replicate(t, store, store2)

  t.absent(store2.cores.map.has(a.key.toString('hex')), 'reopened store2 doesnt have core loaded')
  await a.append('hello')
})

test('notifyGroup handle', async function (t) {
  t.plan(5)
  const topic = b4a.alloc(32, 1)

  const store = await create(t)

  const a = store.get({ name: 'foo', group: topic })
  t.teardown(() => a.close())

  const handle = store.notifyGroup(b4a.alloc(32, 1))
  t.ok(store._groupNotifiers.has(topic.toString('hex')), 'registered handle in map')

  handle.on('update', () => t.pass('notified'))

  await a.append('hello')
  await a.append('hello') // fires each time

  t.ok(includesKey(await toArray(handle.updates()), a.key), 'updates stream returns key')

  handle.destroy()
  t.is(store._groupNotifiers.size, 0, 'cleared handle')

  await a.append('hello')
})

test('notifyGroup handle - updates empty for unknown topic', async function (t) {
  const store = await create(t)
  const handle = store.notifyGroup(b4a.alloc(32, 99))
  t.alike(await toArray(handle.updates()), [], 'empty stream when topic has no group')
  handle.destroy()
})

test('notifyGroup - passed handle never attached ends early', async function (t) {
  const store = await create(t)

  const topic = b4a.alloc(32, 1)
  const legit = store.notifyGroup(topic)
  t.teardown(() => legit.destroy())

  const handle = new GroupNotifyHandle(store, b4a.alloc(32, 1))
  t.is(handle.index, -1, 'non-attached handle has index -1')
  t.execution(() => handle.destroy(), 'destroy runs w/o issue')

  t.not(legit.index, -1, 'legit index wasnt updated')

  const a = store.get({ name: 'a', group: topic })
  await a.ready()
  t.teardown(() => a.close())

  await a.append('boo')

  t.ok(includesKey(await toArray(handle.updates()), a.key), 'legit updates stream returns key')
})

function replicate(t, a, b) {
  const s1 = a.replicate(true)
  const s2 = b.replicate(false)

  s1.pipe(s2).pipe(s1)

  s1.on('error', console.error)
  s2.on('error', console.error)

  t.teardown(async () => {
    s1.destroy()
    s2.destroy()
    await Promise.all([
      new Promise((resolve) => s1.once('close', resolve)),
      new Promise((resolve) => s2.once('close', resolve))
    ])
  })
}
