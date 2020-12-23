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

  _read (req) {
    req.callback(null, this._buf.slice(req.offset, req.offset + req.size))
  }
}
