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

function replicate(a, b, t) {
  const s1 = a.replicate(true)
  const s2 = b.replicate(false)

  s1.pipe(s2).pipe(s1)

  t.teardown(() => {
    s1.destroy()
    s2.destroy()
  })
}

module.exports = {
  toArray,
  create,
  includesKey,
  replicate
}
