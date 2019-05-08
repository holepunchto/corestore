const p = require('path')
const hypercore = require('hypercore')
const datEncoding = require('dat-encoding')

module.exports = function (storage, opts) {
  const cores = new Map()
  const replicationStreams = []
  var mainCore = null

  return {
    get,
    replicate
  }

  function get (coreOpts = {}) {
    if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
    const idx = coreOpts.key || coreOpts.discoveryKey || coreOpts.name || (coreOpts.main && 'main')

    const existing = cores.get(idx)
    if (existing) return existing

    const core = hypercore(filename => {
      const idxString = (idx instanceof Buffer) ? datEncoding.encode(idx) : idx
      const path = p.join(idxString, filename)
      return storage(path)
    }, coreOpts.key, { ...coreOpts, ...opts })

    if (coreOpts.name) cores.set(coreOpts.name, core)
    if (coreOpts.main) mainCore = core

    core.ready(_ => {
      cores.set(core.key, core) 
      cores.set(core.discoveryKey, core)
      for (let [stream, opts] of replicationStreams) {
        replicateCore(core, stream, opts)
      }
    })

    return core
  }

  function replicate (opts) {
    if (!mainCore) throw new Error('A main core must be specified before replication.')
    const mainStream = mainCore.replicate(opts)
    for (let [_, core] of cores) {
      replicateCore(core, mainStream, opts)
    }
    mainStream.on('feed', dkey => {
      let core = cores.get(dkey)
      if (core) replicateCore(core, mainStream, opts)
    })
    replicationStreams.push([mainStream, opts])
    return mainStream
  }

  function replicateCore (core, mainStream, opts) {
    core.ready(function (err) {
      if (err) return
      for (const feed of mainStream.feeds) { // TODO: expose mainStream.has(key) instead
        if (feed.peer.feed === core) return
      }
      core.replicate({
        ...opts,
        stream: mainStream
      })
    })
  }
}
