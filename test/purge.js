const test = require('brittle')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const CorestoreStorage = require('hypercore-storage')

const Corestore = require('../')
const { create } = require('./helpers')

test('purge - removes the listed cores and keeps the rest', async function (t) {
  const store = await create(t)

  const a = store.get({ name: 'a' })
  const b = store.get({ name: 'b', manifestVersion: 2 })
  const remote = store.get({ key: crypto.keyPair().publicKey })

  await a.append(['a0', 'a1'])
  await b.append(['b0', 'b1', 'b2'])
  await remote.ready()

  const keyB = b.key
  const dkB = b.discoveryKey

  await a.close()
  await b.close()
  await remote.close()

  const purged = await store.purge([keyB])

  t.is(purged.length, 1)
  t.alike(purged[0], dkB)
  t.is(await store.storage.hasCore(dkB), false, 'the purged core is gone from storage')
  t.is(await store.storage.hasCore(a.discoveryKey), true, 'other named cores survive')
  t.is(await store.storage.hasCore(remote.discoveryKey), true, 'cores opened by key survive')

  const again = store.get({ name: 'a' })
  t.alike(await again.get(1), b4a.from('a1'), 'the surviving core still reads')
  await again.close()

  const byDiscoveryKey = store.get({ discoveryKey: dkB })
  await t.exception(byDiscoveryKey.ready(), /No Hypercore is stored here/)
  await byDiscoveryKey.close().catch(() => {})
})

test('purge - accepts keys, discovery keys and objects', async function (t) {
  const store = await create(t)

  const cores = [store.get({ name: 'x' }), store.get({ name: 'y' }), store.get({ name: 'z' })]
  for (const core of cores) await core.append('data')
  const dks = cores.map((c) => c.discoveryKey)
  const keys = cores.map((c) => c.key)
  for (const core of cores) await core.close()

  const purged = await store.purge([keys[0], { discoveryKey: dks[1] }, { key: keys[2] }])

  t.alike(purged, dks)
  for (const dk of dks) t.is(await store.storage.hasCore(dk), false)
})

test('purge - an unknown key is a noop and is not created', async function (t) {
  const store = await create(t)

  const key = crypto.keyPair().publicKey
  const purged = await store.purge(key)

  t.alike(purged, [])
  t.is(await store.storage.hasCore(crypto.discoveryKey(key)), false)
})

test('purge - a core with open sessions is skipped unless forced', async function (t) {
  const store = await create(t)

  const core = store.get({ name: 'busy' })
  await core.append('data')
  const key = core.key
  const dk = core.discoveryKey

  t.alike(await store.purge(key), [], 'skipped while a session is open')
  t.is(await store.storage.hasCore(dk), true)
  t.is(core.closed, false, 'the open session was left alone')

  t.alike(await store.purge(key, { force: true }), [dk], 'forced')
  t.is(await store.storage.hasCore(dk), false)
  t.is(core.closed, true, 'the open session was closed by the forced purge')
})

test('purge - two concurrent purge() calls on the same key do not falsely skip each other', async function (t) {
  const dir = await t.tmp()

  // _purgeCore opens its own internal session on the core to decide whether
  // it's safe to purge. GatedStorage is to control when that happens to show edgecase timing
  let pending = null
  let gated = false

  class GatedStorage extends CorestoreStorage {
    async hasCore(discoveryKey, opts) {
      const result = await super.hasCore(discoveryKey, opts)
      if (!gated) return result

      if (!pending) {
        let release
        const promise = new Promise((resolve) => { release = resolve })
        pending = { promise, release }
        await pending.promise
      } else {
        pending.release()
        pending = null
      }

      return result
    }
  }

  const store = new Corestore(new GatedStorage(dir))
  t.teardown(() => store.close())

  const core = store.get({ name: 'race' })
  await core.append('data')
  const key = core.key
  const dk = core.discoveryKey
  await core.close()

  gated = true
  const [a, b] = await Promise.all([store.purge(key), store.purge(key)])
  gated = false

  t.is(a.length, 1, 'the first concurrent call purges the core')
  t.is(b.length, 1, 'the second concurrent call purges the core too, instead of seeing the first as a busy session')
  t.is(await store.storage.hasCore(dk), false)
})

test('purge - sessions on a namespace of the same root are closed too', async function (t) {
  const store = await create(t)
  const ns = store.namespace('ns')

  const core = ns.get({ name: 'shared' })
  await core.append('data')
  const key = core.key
  const dk = core.discoveryKey

  t.alike(await store.purge(key, { force: true }), [dk])
  t.is(core.closed, true)
  t.is(await store.storage.hasCore(dk), false)

  await ns.close()
})

test('purge - a purged name is minted fresh on the next open', async function (t) {
  const dir = await t.tmp()

  let store = new Corestore(dir)
  let core = store.get({ name: 'bee' })
  await core.append('old')
  const oldKey = core.key
  await core.close()

  t.alike(await store.purge(oldKey), [crypto.discoveryKey(oldKey)])
  await store.close()

  store = new Corestore(dir)
  t.teardown(() => store.close())

  core = store.get({ name: 'bee', manifestVersion: 2 })
  await core.ready()

  t.is(core.length, 0, 'the name resolves to an empty core')
  t.is(core.manifest.version, 2, 'minted with the requested manifest version')
  t.absent(b4a.equals(core.key, oldKey), 'the key changed with the manifest version')
  await core.close()
})
