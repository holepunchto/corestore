module.exports = async function* audit(store, { dryRun = false } = {}) {
  for await (const { discoveryKey, core } of store.storage.createCoreStream()) {
    if (core.version < 1) continue // not migrated, ignore

    const c = store.get({ discoveryKey, active: false })
    await c.ready()

    yield { discoveryKey, key: c.key, audit: await c.core.audit({ dryRun }) }

    try {
      await c.close()
    } catch {
      // ignore if failed, we are auditing...
    }
  }

  // an alias whose core is gone makes get({ name }) fail until it is dropped
  for await (const { alias, discoveryKey } of store.storage.createAliasStream(null)) {
    const dangling = !(await store.storage.hasCore(discoveryKey))
    const pruned =
      dangling && !dryRun ? await store.storage.deleteAlias(alias, discoveryKey) : false

    yield { discoveryKey, alias, dangling, pruned }
  }
}
