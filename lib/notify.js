const { EventEmitter } = require('events')
const { Readable } = require('streamx')

class GroupNotifyHandle extends EventEmitter {
  constructor(store, topic) {
    super()
    this._store = store
    this._topic = topic
    this._groupId = null
    this.index = -1
  }

  updates(opts) {
    const self = this
    const trigger = new Readable({
      async open(cb) {
        self._groupId = await self._store.storage.getGroup(self._topic).catch(noop)
        if (self._groupId !== null) {
          this.push(self._store.storage.createGroupUpdateStream(self._groupId, opts))
        }
        this.push(null)
        cb(null)
      }
    })

    return Readable.DeferredStream(trigger)
  }

  destroy() {
    this._store._removeGroupNotify(this)
  }
}

module.exports = GroupNotifyHandle

function noop() {}
