const { EventEmitter } = require('events')
const { Readable } = require('streamx')

class GroupNotifyHandle extends EventEmitter {
  constructor(store, topic) {
    super()
    this._store = store
    this._topic = topic
    this.index = -1
  }

  updates(opts) {
    return Readable.deferred(async () => {
      const groupId = await this._store.storage.getGroup(this._topic).catch(noop)
      if (groupId === null) return null
      return this._store.storage.createGroupUpdateStream(groupId, opts)
    })
  }

  /**
   * Destroys and unregisters the `handle` from its `store`.
   */
  destroy() {
    this._store._removeGroupNotify(this)
  }
}

module.exports = GroupNotifyHandle

function noop() {}
