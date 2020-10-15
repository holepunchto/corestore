/**
 * Makes a couple dozen namespaced corestores
 * Point is, every time a new corestore is made event listeners for 'feed' and 'error' are
 * added but never removed unless the namespace is closed.
 *
 * emit 'feed' seems to be only needed for onready,
 * if _unlisten is added after onready emits feed, this test passes
 */
const ram = require('random-access-memory')
const test = require('tape')
const Corestore = require('..')
const { once } = require('events')

test('make a couple dozen namespaced corestores (without MaxListener warning)', async t => {
  const store = await create(ram)
  let spaces = []
  let cores = []

  let index = 0
  while (index < 24) {
    try {
      spaces[index] = store.namespace('namespace-' + index)
      cores[index] = spaces[index].default()
      await cores[index].ready()

      t.ok(store.inner._events.feed.length < 3, '## feed event listener length before is only 1 or 2')
      t.ok(spaces[index].inner._events.feed.length < 3, '## feed event listener length before is only 1 or 2')
      t.ok(store.inner._events.error.length < 3, '## feed event listener length before is only 1 or 2')
      t.ok(spaces[index].inner._events.error.length < 3, '## feed event listener length before is only 1 or 2')

      await once(store.inner, 'feed')

      t.ok(store.inner._events.feed === undefined, '## feed event listener length after is zero')
      t.ok(spaces[index].inner._events.feed === undefined, '## feed event listener length after is zero')
      t.ok(store.inner._events.error === undefined, '## feed event listener length after is zero')
      t.ok(spaces[index].inner._events.error === undefined, '## feed event listener length after is zero')
    } catch (err) {
      console.error('error happened', err)
    }
    t.ok(store.inner._events.feed === undefined, 'no event listner memory leak')
    index++
  }

  t.end()
})

async function create (storage, opts) {
  const store = new Corestore(storage, opts)
  await store.ready()
  return store
}
