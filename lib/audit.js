const auditCore = require('hypercore-audit')

module.exports = async function * audit (store, { dryRun = false } = {}) {
  for await (const { discoveryKey } of store.storage.createCoreStream()) {
    const storage = await store.storage.resume(discoveryKey)

    const rx = storage.read()
    const authPromise = rx.getAuth()
    rx.tryFlush()

    const auth = await authPromise

    yield {
      discoveryKey,
      key: auth ? auth.key : null,
      audit: await auditCore(storage, { dryRun })
    }
  }
}
