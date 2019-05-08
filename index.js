const p = require('path')
const hypercore = require('hypercore')
const crypto = require('hypercore-crypto')
const datEncoding = require('dat-encoding')

module.exports = function (storage, opts) {
  console.log('CORESTORE STORAGE:', storage)
  const cores = new Map()
  const replicationStreams = []
  var mainCore = null

  return {
    get,
    replicate,
  }

  function get (coreOpts = {}) {
    console.log('getting with opts:', coreOpts)
    if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
    const idx = coreOpts.key || coreOpts.discoveryKey || coreOpts.name || (coreOpts.main && 'main')

    console.log('idx:', idx)
    const idxString = (idx instanceof Buffer) ? datEncoding.encode(idx) : idx
    const existing = cores.get(idxString)
    if (existing) return existing

    if (coreOpts.key) {
      var publicKey = coreOpts.key
    } else {
      var { publicKey, secretKey } = crypto.keyPair()
    }
    const discoveryKey = crypto.discoveryKey(publicKey)
    const core = hypercore(filename => {
      console.log('idxString:', idxString, 'filename:', filename)
      const path = p.join(idxString, filename)
      return storage(path)
    }, publicKey, {
      ...coreOpts,
      ...opts,
      secretKey: secretKey || coreOpts.secretKey || null
    })
    cores.set(datEncoding.encode(publicKey), core)
    cores.set(datEncoding.encode(discoveryKey), core)

    if (coreOpts.name) cores.set(coreOpts.name, core)
    if (coreOpts.main && !mainCore) mainCore = core

    for (let [stream, opts] of replicationStreams) {
      replicateCore(core, stream, opts)
    }

    return core
  }

  function replicate (opts) {
    if (!mainCore) throw new Error('A main core must be specified before replication.')
    const mainStream = mainCore.replicate(opts)
    for (let [_, core] of cores) {
      replicateCore(core, mainStream, opts)
    }
    mainStream.on('feed', dkey => {
      let core = cores.get(datEncoding.encode(dkey))
      console.log('ON FEED:', dkey, 'CORE IS:', core)
      if (core) {
        replicateCore(core, mainStream, opts)
        setTimeout(() => {
          console.log('AFTER REPLICATING, MY CORE IS:', core)
        }, 1000)
      }
    })
    replicationStreams.push([mainStream, opts])
    mainStream.on('finish', () => {
      console.log('AFTER REPLICATION, cores:', cores)
    })
    return mainStream
  }

  function replicateCore (core, mainStream, opts) {
    core.replicate({
      ...opts,
      stream: mainStream
    })
  }
}
