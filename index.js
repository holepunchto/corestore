const p = require('path')
const hypercore = require('hypercore')
const crypto = require('hypercore-crypto')
const datEncoding = require('dat-encoding')
const { promisify } = require('util')

module.exports = function (storage, opts = {}) {
  if (typeof storage !== 'function') storage = path => storage(path)

  var cores = new Map()
  var replicationStreams = []
  var defaultCore = null

  return {
    default: getDefault,
    get,
    replicate,
    close
  }

  function getDefault (coreOpts = {}) {
    if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
    if (defaultCore) return defaultCore

    if (opts.secretKey) coreOpts.secretKey = opts.secretKey
    else coreOpts.default = true

    defaultCore = get(coreOpts)
    return defaultCore
  }

  function get (coreOpts = {}) {
    if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
    var idx = coreOpts.key || coreOpts.discoveryKey || (coreOpts.default && 'default')

    if (!idx) {
      // If no key was specified, then we generate a new keypair.
      var { publicKey, secretKey } = crypto.keyPair()
      idx = publicKey
    }

    const idxString = (idx instanceof Buffer) ? datEncoding.encode(idx) : idx
    const existing = cores.get(idxString)
    if (existing) return existing

    const core = hypercore(filename => storage(idxString + '/' + filename), coreOpts.key, {
      ...coreOpts,
      secretKey: coreOpts.secretKey || secretKey
    })
    core.ready(() => {
      cores.set(datEncoding.encode(core.key), core)
      cores.set(datEncoding.encode(core.discoveryKey), core)
    })

    for (let { stream, opts }  of replicationStreams) {
      replicateCore(core, stream, opts)
    }

    return core
  }

  function replicate (opts) {
    if (!defaultCore) throw new Error('A main core must be specified before replication.')
    const mainStream = defaultCore.replicate(opts)
    var closed = false

    for (let [_, core] of cores) {
      replicateCore(core, mainStream, opts)
    }

    mainStream.on('feed', dkey => {
      let core = cores.get(datEncoding.encode(dkey))
      if (core) {
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

  function close (cb) {
    var remaining = cores.size

    for (let { stream } of replicationStreams) {
      stream.destroy()
    }
    for (let [, core] of cores) {
      if (!core.closed) core.close(onclose)
    }

    function onclose (err) {
      if (err) return reset(err)
      if (!--remaining) return reset(null)
    }

    function reset (err) {
      cores = new Map()
      replicationStreams = []
      return cb(err)
    }
  }
}
