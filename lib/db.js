const { NanoresourcePromise: Nanoresource } = require('nanoresource-promise/emitter')
const Hyperbee = require('hyperbee')
const Omega = require('omega')

const INDEX_PATH = 'index'
const INDEX_VERSION = '@corestore/v1'
const KEYS_NAMESPACE = 'by-key'
const DKEYS_NAMESPACE = 'by-dkey'

module.exports = class Index extends Nanoresource {
  constructor (storage, opts = {}) {
    super()

    this.storage = storage
    this._core = new Omega(p => this.storage(INDEX_PATH + '/' + p))
    this._db = new Hyperbee(this._core, {
      keyEncoding: 'utf-8',
      valueEncoding: 'json',
      sep: '!'
    })
    this._versionSub = this._db.sub(INDEX_VERSION)

    this._byKey = this._versionSub.sub(KEYS_NAMESPACE)
    this._byDKey = this._versionSub.sub(DKEYS_NAMESPACE)

    this._core.on('error', err => this.emit('error', err))
  }

  // Nanoresource Methods

  async _open () {
    return this._core.ready()
  }

  _close () {
    return this._core.close()
  }

  // Private Methods

  async _putCoreBatch (keys, batch) {
    if (!keys.publicKey) throw new Error('Invalid core batch')
    const record = {
      name: keys.name,
      publicKey: toString(keys.publicKey),
      discoveryKey: toString(keys.discoveryKey)
    }
    const b = batch || this._db.batch()
    await b.put(this._byKey.keyEncoding.encode(record.publicKey), record)
    await b.put(this._byDKey.keyEncoding.encode(record.discoveryKey), record)
    if (!batch) await b.flush()
  }

  // Public Methods

  async getName (keys) {
    if (!keys.publicKey) return null
    const node = await this._byKey.get(toString(keys.publicKey))
    return node && node.value.name
  }

  async getPassiveCoreKeys (dkey) {
    const node = await this._byDKey.get(toString(dkey))
    return node && toKeys(node.value)
  }

  async getAllCores () {
    const allCores = []
    for await (const { value } of this._byKey.createReadStream()) {
      allCores.push(value)
    }
    return allCores
  }

  async saveKeys (keys) {
    const existing = await this.getName(keys)
    if (existing) return
    return this._putCoreBatch(keys)
  }

  async restore (manifest) {
    if (!manifest.cores || !Array.isArray(manifest.cores)) throw new Error('Malformed manifest.')
    for (const keys of manifest.cores) {
      await this.saveKeys(keys)
    }
  }
}

function toString (buf) {
  if (typeof buf === 'string') return buf
  return buf.toString('hex')
}

function toKeys (record) {
  return {
    name: record.name,
    publicKey: Buffer.from(record.publicKey, 'hex'),
    discoveryKey: Buffer.from(record.discoveryKey, 'hex')
  }
}
