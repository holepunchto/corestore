const test = require('brittle')
const b4a = require('b4a')
const Corestore = require('../')

test('suspendReplication pauses every core, resume catches up', async function (t) {
  const store = new Corestore(await t.tmp())

  const foo = store.get({ name: 'foo' })
  const bar = store.get({ name: 'bar' })
  await foo.append(['a', 'b'])
  await bar.append(['x', 'y'])

  const store2 = new Corestore(await t.tmp())
  const fooClone = store2.get(foo.key)
  const barClone = store2.get(bar.key)

  const streams = replicate(store, store2, t)

  t.alike(await fooClone.get(0), b4a.from('a'))
  t.alike(await barClone.get(0), b4a.from('x'))

  await store2.suspendReplication()

  await foo.append('c')
  await bar.append('z')
  await sleep(300)

  t.is(fooClone.length, 2, 'foo not upgraded while suspended')
  t.is(barClone.length, 2, 'bar not upgraded while suspended')

  store2.resumeReplication()

  t.alike(await fooClone.get(2), b4a.from('c'), 'foo caught up after resume')
  t.alike(await barClone.get(2), b4a.from('z'), 'bar caught up after resume')

  await teardown(streams, store, store2)
})

test('core opened while suspended comes up suspended', async function (t) {
  const store = new Corestore(await t.tmp())

  const a = store.get({ name: 'a' })
  await a.append(['a', 'b', 'c'])

  const store2 = new Corestore(await t.tmp())
  await store2.suspendReplication()

  const clone = store2.get(a.key)
  const streams = replicate(store, store2, t)

  const get = clone.get(1)
  const early = await Promise.race([get.then(() => 'served'), sleep(300).then(() => 'pending')])
  t.is(early, 'pending', 'no download while suspended')

  store2.resumeReplication()

  t.alike(await get, b4a.from('b'), 'downloaded after resume')

  await teardown(streams, store, store2)
})

test('storage suspends cleanly under suspended replication', async function (t) {
  const store = new Corestore(await t.tmp())

  const a = store.get({ name: 'a' })
  await a.append(['a', 'b', 'c', 'd', 'e'])

  const store2 = new Corestore(await t.tmp())
  const clone = store2.get(a.key)

  const streams = replicate(store, store2, t)

  t.alike(await clone.get(0), b4a.from('a'))

  await store.suspendReplication()
  await store.suspend()

  // incoming request while fully suspended must queue, not park on storage
  const get = clone.get(3)
  await sleep(300)

  t.is(store.storage.rocks.diagnostics().io, 0, 'no parked storage io while suspended')

  await store.resume()
  store.resumeReplication()

  t.alike(await get, b4a.from('d'), 'served after resume')

  await teardown(streams, store, store2)
})

function replicate(a, b, t) {
  const s1 = a.replicate(true)
  const s2 = b.replicate(false)
  s1.pipe(s2).pipe(s1)
  return [s1, s2]
}

async function teardown(streams, ...stores) {
  for (const s of streams) s.destroy()
  for (const store of stores) await store.close()
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
