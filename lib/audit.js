module.exports = async function audit (store, { dryRun = false } = {}) {
  const stats = { cores: 0, skipped: 0, rootless: 0, dropped: 0 }

  for await (const { discoveryKey } of store.storage.createCoreStream()) {
    stats.cores++

    const core = store.get({ discoveryKey, active: false })
    await core.ready()

    const audit = await core.core.audit({ dryRun })

    try {
      await core.close()
    } catch {
      // ignore if failed, we are auditing...
    }

    if (audit === null || audit.corrupt) {
      stats.rootless++
      continue
    }

    if (audit.droppedTreeNodes || audit.droppedBlocks || audit.droppedBits) {
      stats.dropped++
    }
  }

  return stats
}
