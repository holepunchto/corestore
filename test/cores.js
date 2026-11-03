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

  // half the keys share their first 4 bytes, so they land in one probe chain
  const keys = []
  for (let i = 0; i < 400; i++) {
    const discoveryKey = crypto.randomBytes(32)
    if (i % 2 === 0) discoveryKey.fill(7, 0, 4)
    keys.push(discoveryKey)
  }

  let seed = 1
  const random = (n) => {
    seed = (seed * 1103515245 + 12345) % 2147483648
    return seed % n
  }

  for (let i = 0; i < 20000; i++) {
    const discoveryKey = keys[random(keys.length)]
    const id = b4a.toString(discoveryKey, 'hex')

    if (random(3) === 0 && expected.has(id)) {
      tracker._gc(expected.get(id))
      expected.delete(id)
    } else {
      const core = { discoveryKey, closing: null, trackerIndex: -1 }
      tracker.set(core)
      expected.set(id, core)
    }
  }

  t.is(tracker.size, expected.size, 'same size')

  let found = 0
  for (const discoveryKey of keys) {
    const want = expected.get(b4a.toString(discoveryKey, 'hex')) || null
    if (tracker.get(discoveryKey) === want) found++
  }
  t.is(found, keys.length, 'every lookup matches the map')
  t.alike(new Set(tracker), new Set(expected.values()), 'iterates the same cores')
})
