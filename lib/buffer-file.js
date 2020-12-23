const RAS = require('random-access-storage')

module.exports = class BufferFile extends RAS {
  constructor (buf) {
    super()
    this._buf = buf
  }

  _open (req) {
    if (typeof this._buf !== 'function') return req.callback(null)
    this._buf((err, b) => {
      if (err) return req.callback(err)
      this._buf = b
      req.callback(null)
    })
  }

  _stat (req) {
    req.callback(null, { size: this._buf ? this._buf.length : 0 })
  }

  _read (req) {
    if (!this._buf) return req.callback(new Error('Could not satisfy length'))
    req.callback(null, this._buf.slice(req.offset, req.offset + req.size))
  }
}
