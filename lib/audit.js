module.exports = async function * audit (store, { dryRun = false } = {}) {
  for await (const { discoveryKey } of store.storage.createCoreStream()) {
    const core = store.get({ discoveryKey, active: false })
    await core.ready()

    yield { discoveryKey, key: core.key, audit: await core.core.audit({ dryRun }) }

    try {
      await core.close()
    } catch {
      // ignore if failed, we are auditing...
    }
  }
}
