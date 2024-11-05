import Corestore from './index.js'

const store = new Corestore('./store.db')

const core = store.get({ name: 'yo' })
// await core.close()

const store2 = new Corestore('./store2.db')

await core.ready()
await core.append('yo')

const clone = store2.get({ key: core.key })

const stream = store.replicate(true)
const stream2 = store2.replicate(false)

stream.pipe(stream2).pipe(stream)

await clone.ready()
console.log(core, clone)
