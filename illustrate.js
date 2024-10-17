const Corestore = require('.')
const b4a = require('b4a')

async function main () {
  const loc = 'test-store'
  const password = b4a.from('some-dummy-password')

  const store = new Corestore(loc, { useEncryptedPrimaryKey: true, password })
  await store.ready()
  console.log('Loaded store with primary key', store.primaryKey.toString('hex'))

  console.log('\nIllustration that we cannot create an unencrypted core:')
  try {
    store.get({ name: 'my unencrypted core' })
  } catch (e) {
    console.log(e.message)
  }

  console.log('\nIllustration that we can create an encrypted core:')
  const encryptionKey = b4a.from('a'.repeat(64, 'hex'))
  const core = store.get({ name: 'encrypted-core', encryptionKey })
  await core.ready()
  if (core.length > 0) {
    console.log('Loaded existing core--last entry:', (await core.get(core.length - 1)).toString())
  } else {
    await core.append('block 0')
    await core.append('block 1')
    console.log('Created a core. Rerun to show it can be reloaded')
  }
}

main()
