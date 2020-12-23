const { EventEmitter } = require('events')
const HypercoreProtocol = require('hypercore-protocol')
const eos = require('end-of-stream')

module.exports = class Replicator extends EventEmitter {
  constructor (loader, opts = {}) {
    super()

    this.loader = loader
    this.opts = opts
    this.streams = []
  }

  _replicateCore (isInitiator, core, mainStream, opts) {
    core.ready(function (err) {
      if (err) return
      core.replicate(isInitiator, {
        ...opts,
        stream: mainStream
      })
    })
  }

  inject (core) {
    for (const { stream, opts } of this.streams) {
      this._replicateCore(false, core, stream, { ...opts })
    }
  }

  replicate (isInitiator, replicationOpts = {}) {
    const finalOpts = { ...this.opts, ...replicationOpts }
    const mainStream = replicationOpts.stream || new HypercoreProtocol(isInitiator, { ...finalOpts })

    for (const core of this.loader.getOpenCores()) {
      this._replicateCore(isInitiator, core, mainStream, { ...finalOpts })
    }

    const streamState = { stream: mainStream, opts: finalOpts }
    this.streams.push(streamState)

    mainStream.on('discovery-key', async (dkey) => {
      const core = await this.loader.getPassiveCore(dkey)
      if (!core) return mainStream.close(dkey)
      this._replicateCore(false, core, mainStream, { ...finalOpts })
    })
    eos(mainStream, err => {
      if (err) this.emit('replication-error', err)
      const idx = this.streams.indexOf(streamState)
      if (idx === -1) return
      this.streams.splice(idx, 1)
    })

    return mainStream
  }

  close () {
    if (!this.streams.length) return
    for (const stream of this.streams) {
      stream.destroy()
    }
  }
}
