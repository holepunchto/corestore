const HypercoreProtocol = require('hypercore-protocol')
const eos = require('end-of-stream')

module.exports = class Replicator {
  constructor (loader, opts = {}) {
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
    for (const { stream, opts } of this._replicationStreams) {
      this._replicateCore(false, core, stream, { ...opts })
    }
  }

  replicate (isInitiator, replicationOpts = {}) {
    const finalOpts = { ...this.opts, ...replicationOpts }
    const mainStream = replicationOpts.stream || new HypercoreProtocol(isInitiator, { ...finalOpts })

    for (const core of this.loader.getActiveCores()) {
      this._replicateCore(isInitiator, core, mainStream, { ...finalOpts })
    }

    const streamState = { stream: mainStream,opts: finalOpts }
    this._replicationStreams.push(streamState)

    mainStream.on('discovery-key', async (dkey) => {
      const core = await this.loader.getPassiveCore(dkey)
      if (!core) return mainStream.close(dkey)
      this._replicateCore(false, core, mainStream, { ...finalOpts })
    })
    eos(mainStream, () => {
      const idx = this._replicationStreams.indexOf(streamState)
      if (idx === -1) return
      this._replicationStreawms.splice(idx, 1)
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
