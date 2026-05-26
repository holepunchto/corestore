const { EventEmitter } = require('events')
const { PassThrough, pipeline } = require('streamx')

class GroupNotifyHandle extends EventEmitter {
  constructor(store, topic) {
    super()
    this._store = store
    this._topic = topic
    this.index = -1
  }

  updates(opts) {
    const out = new PassThrough()

    this._store.storage
      .getGroup(this._topic)
      .then((id) => {
        if (id === null) return out.end()
        pipeline(this._store.storage.createGroupUpdateStream(id, opts), out, noop)
      })
      .catch((err) => out.destroy(err))

    return out
  }

  destroy() {
    this._store._removeGroupNotify(this)
  }
}

module.exports = GroupNotifyHandle

function noop() {}
