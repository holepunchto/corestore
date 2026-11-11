const test = require('brittle')
const b4a = require('b4a')
const { once } = require('events')
const Hypercore = require('hypercore')
const crypto = require('hypercore-crypto')

const { create, replicate } = require('./helpers')

test('passive - replicates a core to a peer that asked for it before it had it', async function (t) {
  const writer = await create(t)
  const core = writer.get({ name: 'core' })
  await core.append(['a', 'b'])

  const passive = await create(t, { active: false })

  const reader = await create(t)
  const clone = reader.get({ key: core.key })
  await clone.ready()

  const [s1] = replicate(reader, passive, t)
  t.is(await answered(s1, core.discoveryKey), false, 'sanity check: did not have it')

  const copy = passive.get({ key: core.key })
  await copy.ready()

  replicate(writer, passive, t)
  t.alike(await copy.get(1), b4a.from('b'), 'sanity check: got it from the writer')

  await t.execution(clone.get(1, { timeout: 5000 }), 'the peer got it')
  t.absent(s1.destroying, 'over the same stream')
  t.is(passive.missing.byId.size, 0, 'forgot the ask once attached')
})

test('passive - replicates a core opened while it looked it up', async function (t) {
  const writer = await create(t)
  const core = writer.get({ name: 'core' })
  await core.append(['a', 'b'])

  const passive = await create(t, { active: false })
  const lookup = await holdLookup(passive, core.discoveryKey)

  const reader = await create(t)
  const clone = reader.get({ key: core.key })
  await clone.ready()

  replicate(reader, passive, t)
  t.is(await lookup.found, false, 'sanity check: did not have it')

  const copy = passive.get({ key: core.key })
  await copy.ready()
  lookup.release()

  replicate(writer, passive, t)
  t.alike(await copy.get(1), b4a.from('b'), 'sanity check: got it from the writer')

  await t.execution(clone.get(1, { timeout: 5000 }), 'the peer got it')
})

test('passive - replicates a core only to the peers that asked for it', async function (t) {
  const writer = await create(t)
  const core = writer.get({ name: 'core' })
  await core.ready()

  const passive = await create(t, { active: false })

  const reader = await create(t)
  const clone = reader.get({ key: core.key })
  await clone.ready()

  const [s1, s2] = replicate(reader, passive, t)
  t.is(await answered(s1, core.discoveryKey), false, 'sanity check: did not have it')

  const [, s3] = replicate(await create(t), passive, t)

  const copy = passive.get({ key: core.key })
  await copy.ready()

  t.ok(copy.replicator.attached(s2.noiseStream.userData), 'attached to the peer that asked')
  t.absent(copy.replicator.attached(s3.noiseStream.userData), 'not to the other peer')
})

test('passive - remembers at most maxMissingPerStream cores per stream', async function (t) {
  const passive = await create(t, { active: false, maxMissingPerStream: 2 })

  const [s1, s2] = replicate(await create(t), passive, t)
  const keys = [crypto.randomBytes(32), crypto.randomBytes(32), crypto.randomBytes(32)]
  const [a, b, c] = keys.map((key) => crypto.discoveryKey(key))

  t.is(await ask(s1, a), false, 'sanity check: did not have it')
  await ask(s1, b)
  await ask(s1, a) // asked again, so now the newest
  await ask(s1, c)

  const cores = keys.map((key) => passive.get({ key }))
  await Promise.all(cores.map((core) => core.ready()))

  const muxer = s2.noiseStream.userData
  t.ok(cores[0].replicator.attached(muxer), 'kept the one asked for again')
  t.absent(cores[1].replicator.attached(muxer), 'dropped the oldest')
  t.ok(cores[2].replicator.attached(muxer), 'kept the newest')
})

test('passive - remembers nothing if maxMissingPerStream is 0', async function (t) {
  const passive = await create(t, { active: false, maxMissingPerStream: 0 })

  const [s1, s2] = replicate(await create(t), passive, t)
  const key = crypto.randomBytes(32)
  t.is(await ask(s1, crypto.discoveryKey(key)), false, 'sanity check: did not have it')

  const core = passive.get({ key })
  await core.ready()

  t.absent(core.replicator.attached(s2.noiseStream.userData), 'not attached')
  t.is(passive.missing.byStream.size, 0, 'remembered nothing')
})

test('passive - forgets what a stream asked for when it closes', async function (t) {
  const passive = await create(t, { active: false })

  const [s1, s2] = replicate(await create(t), passive, t)
  t.is(await ask(s1, crypto.randomBytes(32)), false, 'sanity check: did not have it')
  t.is(passive.missing.byId.size, 1, 'sanity check: remembered it')

  s1.destroy()
  s2.destroy()
  await once(s2.noiseStream, 'close')

  t.is(passive.missing.byId.size, 0, 'forgot the core')
  t.is(passive.missing.byStream.size, 0, 'forgot the stream')
})

test('passive - does not remember asks of a stream that closed while it looked them up', async function (t) {
  const passive = await create(t, { active: false })
  const discoveryKey = crypto.randomBytes(32)
  const lookup = await holdLookup(passive, discoveryKey)

  const [s1, s2] = replicate(await create(t), passive, t)
  ask(s1, discoveryKey)
  t.is(await lookup.found, false, 'sanity check: did not have it')

  s1.destroy()
  s2.destroy()
  await once(s2.noiseStream, 'close')
  lookup.release()
  await new Promise(setImmediate)

  t.is(passive.missing.byStream.size, 0, 'remembered nothing')
})

test('passive - active stores still attach only downloading cores', async function (t) {
  const keyPair = crypto.keyPair()
  const key = Hypercore.key({ signers: [{ publicKey: keyPair.publicKey }] })

  const store = await create(t)

  const reader = await create(t)
  const clone = reader.get({ key })
  await clone.ready()

  const [s1, s2] = replicate(reader, store, t)
  t.is(await answered(s1, clone.discoveryKey), false, 'sanity check: did not have it')

  const core = store.get({ keyPair, active: false })
  await core.ready()

  t.absent(core.replicator.downloading, 'sanity check: not downloading')
  t.absent(core.replicator.attached(s2.noiseStream.userData), 'not attached')
  t.is(store.missing.byStream.size, 0, 'remembered nothing')
})

// resolves to whether the remote had the core the store asked it for
async function answered(stream, discoveryKey) {
  await stream.noiseStream.opened
  const muxer = stream.noiseStream.userData
  const channel = muxer.getLastChannel({ protocol: 'hypercore/alpha', id: discoveryKey })
  return channel.fullyOpened()
}

// asks the remote for a core without a store, resolves to whether it had it
function ask(stream, discoveryKey) {
  const muxer = stream.noiseStream.userData
  const channel = muxer.createChannel({ protocol: 'hypercore/alpha', id: discoveryKey })
  channel.open()
  return channel.fullyOpened()
}

// holds the store's first lookup of the core until released
async function holdLookup(store, discoveryKey) {
  await store.ready()

  const hasCore = store.storage.hasCore.bind(store.storage)
  const lookup = { found: null, release: null }
  const released = new Promise((resolve) => (lookup.release = resolve))

  lookup.found = new Promise((resolve) => {
    store.storage.hasCore = async function (key, opts) {
      const has = await hasCore(key, opts)
      if (!b4a.equals(key, discoveryKey)) return has
      store.storage.hasCore = hasCore
      resolve(has)
      await released
      return has
    }
  })

  return lookup
}
