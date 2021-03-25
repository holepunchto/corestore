const RAS = require('random-access-storage')

const { Info } = require('omega/lib/info')
// TODO: Should this also be exported?
const INFO_SIZE = 64 + 32 + 64 + 8

// TODO: This is pretty hacky and should be updated.
module.exports = class OverrideInfoFile extends RAS {
  constructor (storage, loader) {
    super()
    this.storage = storage
    this._loader = loader
    this._file = null
    this._keys = null
  }

  _updateInfo (info) {
    console.log('updating info:', info)
    if (Buffer.isBuffer(info)) info = Info.decode(info)
    info.publicKey = this._keys.publicKey
    info.secretKey = this._keys.secretKey
    return info.encode()
  }

  _open (req) {
    this._loader((err, keys) => {
      if (err) return req.callback(err)
      this._keys = keys
      this.storage.open(err => {
        if (err) return req.callback(err)
        req.callback(null)
      })
    })
  }

  _stat (req) {
    this.storage.read(0, INFO_SIZE, (err, buf) => {
      if (err) return req.callback(err)
      const updated = this._updateInfo(buf)
      req.callback(null, { size: updated.length })
    })
  }

  _read (req) {
    this.storage.read(0, INFO_SIZE, (err, buf) => {
      if (err) return req.callback(err)
      const updated = this._updateInfo(buf)
      req.callback(null, updated.slice(req.offset, req.offset + req.size))
    })
  }

  _write (req) {
    return this.storage.write(req.offset, req.data, err => {
      if (err) return req.callback(err)
      req.callback(null)
    })
  }
}
