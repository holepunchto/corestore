const RAS = require('random-access-storage')

module.exports = class PendingFile extends RAS {
  constructor (wait) {
    super()
    this._wait = wait
    this._storage = null
  }

  _open (req) {
    this._wait((err, storage) => {
      if (err) return req.callback(err)
      this._storage = storage
      req.callback(null)
    })
  }

  _stat (req) {
    this._storage.stat(req.callback.bind(req))
  }

  _read (req) {
    this._storage.read(req.offset, req.size, req.callback.bind(req))
  }

  _write (req) {
    this._storage.write(req.offset, req.data, req.callback.bind(req))
  }

  _del (req) {
    this._storage.delete(req.offset, req.size, req.callback.bind(req))
  }

  _close (req) {
    this._storage.close(req.callback.bind(req))
  }
}
