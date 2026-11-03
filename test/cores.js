const test = require('brittle')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const Corestore = require('../')

test('core tracker - matches a map under random set, replace and gc', async function (t) {
  const store = new Corestore(await t.tmp())
  t.teardown(() => store.close())

  const CoreTracker = store.cores.constructor
  const tracker = new CoreTracker()
  const expected = new Map()

  // half the ids share their first 8 bytes, so they land in one probe chain
  const discoveryKeys = []
  const keys = []
  for (let i = 0; i < 400; i++) {
    const discoveryKey = crypto.randomBytes(32)
    const key = crypto.randomBytes(32)
    if (i % 2 === 0) {
      discoveryKey.fill(7, 0, 8)
      key.fill(9, 0, 8)
    }
    discoveryKeys.push(discoveryKey)
    keys.push(key)
  }

  let seed = 1
  const random = (n) => {
    seed = (seed * 1103515245 + 12345) % 2147483648
    return seed % n
  }

  for (let i = 0; i < 20000; i++) {
    const n = random(discoveryKeys.length)
    const discoveryKey = discoveryKeys[n]
    const id = b4a.toString(discoveryKey, 'hex')

    if (random(3) === 0 && expected.has(id)) {
      tracker._gc(expected.get(id))
      expected.delete(id)
    } else {
      // a third are opened by discovery key only, so their key is not known yet
      const key = random(3) === 0 ? null : keys[n]
      const core = { discoveryKey, key, closing: null, trackerIndex: -1 }
      tracker.set(core)
      expected.set(id, core)
    }
  }

  t.is(tracker.size, expected.size, 'same size')

  let found = 0
  let foundByKey = 0
  for (let i = 0; i < discoveryKeys.length; i++) {
    const want = expected.get(b4a.toString(discoveryKeys[i], 'hex')) || null
    if (tracker.get(discoveryKeys[i]) === want) found++
    if (tracker.byKey.get(keys[i]) === (want && want.key ? want : null)) foundByKey++
  }
  t.is(found, discoveryKeys.length, 'every lookup by discovery key matches the map')
  t.is(foundByKey, keys.length, 'every lookup by key matches the map')
  t.alike(new Set(tracker), new Set(expected.values()), 'iterates the same cores')
})
