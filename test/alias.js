const test = require('brittle')
const crypto = require('hypercore-crypto')

const { create } = require('./helpers')

test('alias - a dangling alias fails the open instead of resolving to nothing', async function (t) {
  const store = await create(t)

  const fine = store.get({ name: 'fine' })
  await fine.append('ok')
  await fine.close()

  // an alias row whose core is gone, as left behind by storages that deleted
  // cores without their alias
  const dangling = crypto.discoveryKey(crypto.keyPair().publicKey)
  const getAlias = store.storage.getAlias.bind(store.storage)
  store.storage.getAlias = (alias) => (alias.name === 'bee' ? dangling : getAlias(alias))

  const bee = store.get({ name: 'bee' })
  const err = await bee.ready().then(
    () => null,
    (err) => err
  )
  t.ok(err, 'the open fails')
  t.is(err.code, 'STORAGE_EMPTY')
  t.ok(/bee/.test(err.message), 'names the alias: ' + err.message)
  t.is(
    await store.storage.getAlias({ name: 'bee', namespace: store.ns }),
    dangling,
    'the alias is left alone'
  )

  const again = store.get({ name: 'fine' })
  await again.ready()
  t.is(again.length, 1, 'other names still open')
  await again.close()

  await store.close()
})
