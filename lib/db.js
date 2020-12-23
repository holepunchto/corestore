const { NanoresourcePromise: Nanoresource } = require('nanoresource-promise/emitter')
const Hyperbee = require('hyperbee')
const hypercore = require('hypercore')

const INDEX_PATH = 'index'
const INDEX_VERSION = '@corestore/v1'
const KEYS_NAMESPACE = 'by-key'
const DKEYS_NAMESPACE = 'by-dkey'

module.exports = class Index extends Nanoresource {
  constructor (storage, opts = {}) {
    super()

    this._core = hypercore(this.storage(INDEX_PATH))
    this._db = new Hyperbee(this._core, {
      keyEncoding: 'utf-8',
      valueEncoding: 'utf-8'
    }).sub(INDEX_VERSION)

    this._byKey = this._db.sub(KEYS_NAMESPACE)
    this._byDKey = this._db.sub(DKEYS_NAMESPACE)
  }

  // Nanoresource Methods

  _open () {
    return this._db.open()
  }

  _close () {
    return new Promise((resolve, reject) => {
      this._core.close(err => {
        if (err) return reject(err)
        return resolve()
      })
    })
  }

  // Public Methods

  async getName (keys) {
    if (keys.name) return keys.name
    const key = keys.publicKey.toString('hex')
    const node = await this._byKey.get(key)
    return node && node.value
  }

  async getPassiveCoreName (dkey) {
    if (typeof dkey !== 'string') dkey = dkey.toString('hex')
    const node = await this._byDKey.get(dkey)
    return node && node.value
  }

  async getAllNames () {
    const allNames = new Map()
    for await (const { key, name } of this._byKey.createReadStream()) {
      allNames.set(name, key)
    }
    return allNames
  }

  async saveKeys (keys) {
    const key = keys.publicKey.toString('hex')
    const dkey = keys.discoveryKey.toString('hex')

    const existing = await this.getName(key)
    if (existing) return

    const b = this._db.batch()
    await b.put(KEYS_NAMESPACE + this._db.sep + key, keys.name || '')
    await b.put(DKEYS_NAMESPACE + this._db.sep + dkey, keys.name || '')
    return b.flush()
  }

  async restore (manifest) {
    if (!manifest.cores || !Array.isArray(manifest.cores)) throw new Error('Malformed manifest.')
    const b = this._byKey.batch()
    for (const [name, key] of manifest.cores) {
      await b.put(key, name)
    }
    return b.flush()
  }
}
