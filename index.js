const hypercore = require('hypercore')
const crypto = require('hypercore-crypto')
const datEncoding = require('dat-encoding')

module.exports = function (storage, opts = {}) {
  if (typeof storage !== 'function') storage = path => storage(path)

  var cores = new Map()
  var replicationStreams = []
  var defaultCore = null

  const storeId = crypto.randomBytes(5)

  return {
    default: getDefault,
    get,
    replicate,
    list,
    close,
    getInfo
  }

  function getInfo () {
    return {
      id: storeId,
      defaultKey: defaultCore && defaultCore.key,
      discoveryKey: defaultCore && defaultCore.discoveryKey
    }
  }

  function getDefault (coreOpts = {}) {
    if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
    if (defaultCore) return defaultCore

    if (!coreOpts.keyPair && !coreOpts.key) coreOpts.default = true

    defaultCore = get(coreOpts)
    return defaultCore
  }

  function optsToIndex (coreOpts) {
    if (coreOpts.default && !coreOpts.key && !coreOpts.secretKey) {
      var idx = 'default'
    } else {
      idx = coreOpts.key || coreOpts.discoveryKey
      if (!idx) {
        // If no key was specified, then we generate a new keypair or use the one that was passed in.
        var { publicKey, secretKey } = coreOpts.keyPair || crypto.keyPair()
        idx = crypto.discoveryKey(publicKey)
      } else {
        if (!(idx instanceof Buffer)) idx = datEncoding.decode(idx)
        if (coreOpts.key) {
          idx = crypto.discoveryKey(idx)
        }
      }
    }
    return { idx, key: coreOpts.key || publicKey, secretKey }
  }

  function get (coreOpts = {}) {
    if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
    const { idx, key, secretKey } = optsToIndex(coreOpts)

    const idxString = (idx instanceof Buffer) ? datEncoding.encode(idx) : idx
    console.log('IDX STRING:', idxString)
    const existing = cores.get(idxString)
    if (existing) return existing

    var core = hypercore(filename => storage(idxString + '/' + filename), key, {
      ...opts,
      ...coreOpts,
      createIfMissing: !coreOpts.discoveryKey,
      secretKey: coreOpts.secretKey || secretKey
    })

    var errored = false
    const errorListener = err => {
      if (coreOpts.discoveryKey) {
        // If an error occurs during creation by discovery key, then that core does not exist on disk.
        errored = true
        return
      }
    }
    core.once('error', errorListener)
    core.once('ready', () => {
      if (errored) return
      core.removeListener('error', errorListener)
      cores.set(datEncoding.encode(core.key), core)
      cores.set(datEncoding.encode(core.discoveryKey), core)
      for (let { stream, opts }  of replicationStreams) {
        replicateCore(core, stream, opts)
      }
    })

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
      // Get will automatically add the core to all replication streams.
      console.log(storeId,'CORESTORE GETTING KEY ON FEED:', dkey)
      get({ discoveryKey: dkey })
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
      console.log(storeId, 'REPLICATING', core, 'INTO', mainStream)
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

  function list () {
    return new Map([...cores])
  }
}
