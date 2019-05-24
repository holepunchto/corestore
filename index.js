const p = require('path')
const hypercore = require('hypercore')
const crypto = require('hypercore-crypto')
const datEncoding = require('dat-encoding')
const { promisify } = require('util')

module.exports = function (storage, opts = {}) {
  if (typeof storage !== 'function') storage = path => storage(path)

  var cores = new Map()
  var replicationStreams = []
  var mainCore = null

  return {
    get,
    replicate,
    close
  }

  function get (coreOpts = {}) {
    console.log('getting with opts:', coreOpts)
    if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
    const idx = coreOpts.key || coreOpts.discoveryKey || coreOpts.name || (coreOpts.main && 'main')

    console.log('idx:', idx)
    const idxString = (idx instanceof Buffer) ? datEncoding.encode(idx) : idx
    const existing = cores.get(idxString)
    if (existing) return existing

    console.log('CREATING A CORE WITH OPTS:', opts, 'IDXSTRING:', idxString)

    const core = hypercore(filename => storage(idxString + '/' + filename), coreOpts.key, {
      ...coreOpts,
      secretKey: coreOpts.secretKey
    })
    core.ready(() => {
      cores.set(datEncoding.encode(core.key), core)
      cores.set(datEncoding.encode(core.discoveryKey), core)
    })

    if (coreOpts.name) cores.set(coreOpts.name, core)
    if (coreOpts.main && !mainCore) mainCore = core

    for (let { stream, opts }  of replicationStreams) {
      replicateCore(core, stream, opts)
    }

    console.log('CREATED CORE WITH KEY:', core.key, 'AND OPTS:', coreOpts)

    return core
  }

  function replicate (opts) {
    if (!mainCore) throw new Error('A main core must be specified before replication.')
    const mainStream = mainCore.replicate(opts)
    var closed = false

    for (let [_, core] of cores) {
      replicateCore(core, mainStream, opts)
    }

    mainStream.on('feed', dkey => {
      console.log('GOT A REQUEST FOR DKEY:', dkey, 'ON REPLICATION STREAM')
      let core = cores.get(datEncoding.encode(dkey))
      if (core) {
        console.log('NOW REPLICATING CORE:', core)
        replicateCore(core, mainStream, opts)
      }
    })
    mainStream.on('finish', onclose)
    mainStream.on('end', onclose)
    mainStream.on('close', onclose)

    let streamState = { stream: mainStream, opts } 
    replicationStreams.push(streamState)

    return mainStream

    function onclose () {
      if (!closed) {
        replicationStreams.splice(replicationStreams.indexOf(streamState), 1)
        closed = true
      }
    }
  }

  function replicateCore (core, mainStream, opts) {
    core.ready(function (err) {
      if (err) return
      // if (mainStream.has(core.key)) return
      for (const feed of mainStream.feeds) { // TODO: expose mainStream.has(key) instead
        if (feed.peer.feed === core) return
      }
      core.replicate({
        ...opts,
        stream: mainStream
      })
    })
  }

  async function close (cb) {
    try {
      for (let [, core] of cores) {
        if (!core.closed) await promisify(core.close.bind(core))()
      }
      for (let { stream } of replicationStreams) {
        stream.destroy()
      }
      cores = new Map()
      replicationStreams = []
    } catch (err) {
      if (cb) return process.nextTick(cb, err)
      else throw err
    }
    if (cb) return process.nextTick(cb, null)
  }
}
