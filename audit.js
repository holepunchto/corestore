const auditCore = require('hypercore/lib/audit.js')

module.exports = async function audit (store, { dryRun = false } = {}) {
  const stats = { cores: 0, skipped: 0, rootless: 0, dropped: 0 }

  for await (const { discoveryKey } of store.storage.createCoreStream()) {
    stats.cores++

    const core = await store.get({ discoveryKey })
    await core.ready()

    const audit = await auditCore(core.core, { dryRun })

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
