const test = require('brittle')
const b4a = require('b4a')

const Corestore = require('../')
const { create, toArray } = require('./helpers')

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

  const keys = await toArray(store2.getGroupUpdates(topic1))
  t.ok(includesKey(keys, a.key))
  t.ok(includesKey(keys, b.key))
  t.alike(await toArray(store2.getGroupUpdates(topic2)), [])
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

  t.alike(await toArray(store3.getGroupUpdates(topic)), [b.key, a.key])

  await store3.close()
})

test('group-active - fired per core not per session', async function (t) {
  t.plan(1)
  const topic = b4a.alloc(32, 1)

  const store = await create(t)

  const a = store.get({ name: 'foo', group: topic })
  t.teardown(() => a.close())
  const a2 = store.get({ name: 'foo' })
  t.teardown(() => a2.close())

  store.on('group-active', (group) => {
    t.is(group, topic, 'group active')
  })

  await a.append('hello')
})

function includesKey(keys, key) {
  return keys.find((k) => k.toString('hex') === key.toString('hex'))
}
