const { EventEmitter } = require('events')
const Omega = require('omega')
const eos = require('end-of-stream')

module.exports = class Replicator extends EventEmitter {
  constructor (loader, opts = {}) {
    super()

    this.loader = loader
    this.opts = opts
    this.streams = []

    this.loader.on('core', core => {
      this.inject(core)
    })
  }

  async inject (core) {
    for (const { stream, opts } of this.streams) {
      core.replicate(false, {
        ...opts,
        stream
      })
    }
  }

  replicate (isInitiator, replicationOpts = {}) {
    const finalOpts = { ...this.opts, ...replicationOpts }
    const mainStream = replicationOpts.stream || Omega.createProtocolStream(isInitiator)

    for (const core of this.loader.getOpenCores()) {
      core.ready().then(() => {
        core.replicate(isInitiator, { ...finalOpts, stream: mainStream })
      }, err => this.emit('replication-error', err))
    }

    const streamState = { stream: mainStream, opts: finalOpts }
    this.streams.push(streamState)

    mainStream.on('discovery-key', async (dkey) => {
      const core = await this.loader.getPassiveCore(dkey)
      if (core) core.replicate(false, { ...finalOpts, stream: mainStream })
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
