const Corestore = require('../..')
const b4a = require('b4a')

async function toArray(ite) {
  const all = []
  for await (const data of ite) {
    all.push(data)
  }
  return all
}

async function create(t) {
  const dir = await t.tmp()
  const store = new Corestore(dir)
  t.teardown(() => store.close())
  return store
}

function includesKey(keys, key) {
  return keys.find((k) => b4a.equals(k, key))
}

module.exports = {
  toArray,
  create,
  includesKey
}
