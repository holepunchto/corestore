const Corestore = require('../..')

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

module.exports = {
  toArray,
  create
}
