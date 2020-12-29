const p = require('path')
const fs = require('fs').promises

const varint = require('varint')
const Hyperbee = require('hyperbee')
const hypercore = require('hypercore')
const hypercoreCrypto = require('hypercore-crypto')
const readdir = require('@jsdevtools/readdir-enhanced')
const { NanoresourcePromise: Nanoresource } = require('nanoresource-promise/emitter')

const INDEX_PATH = 'index'
const INDEX_VERSION = '@corestore/v1'
const MIGRATION_KEY = '@migrated'
const KEYS_NAMESPACE = 'by-key'
const DKEYS_NAMESPACE = 'by-dkey'

module.exports = class Index extends Nanoresource {
  constructor (storage, opts = {}) {
    super()

    this.storage = storage
    this._migrationRoot = opts.migrationRoot
    this._core = hypercore(p => this.storage(INDEX_PATH + '/' + p))
    this._db = new Hyperbee(this._core, {
      keyEncoding: 'utf-8',
      valueEncoding: 'json'
    }).sub(INDEX_VERSION)

    this._byKey = this._db.sub(KEYS_NAMESPACE)
    this._byDKey = this._db.sub(DKEYS_NAMESPACE)
  }

  // Nanoresource Methods

  async _open () {
    await new Promise((resolve, reject) => {
      this._core.open(err => {
        if (err) return reject(err)
        return resolve()
      })
    })
    return this._maybeMigrate()
  }

  _close () {
    return new Promise((resolve, reject) => {
      this._core.close(err => {
        if (err) return reject(err)
        return resolve()
      })
    })
  }

  // Private Methods

  async _maybeMigrate () {
    if (!this._migrationRoot) return
    const isMigrated = await this._db.get(MIGRATION_KEY)
    if (isMigrated) return

    const basePath = p.resolve(this._migrationRoot)
    const nameIterator = readdir.iterator(basePath, {
      basePath,
      deep: true,
      filter: /.*name$/
    })
    const b = this._db.batch()

    for await (const namePath of nameIterator) {
      let name = await fs.readFile(namePath)
      if (!varint.decode(name)) continue
      name = name.slice(varint.decode.bytes).toString('utf-8')

      const keyPath = p.join(p.dirname(namePath), 'key')
      const publicKey = await loadKey(keyPath)
      if (!publicKey) continue

      const discoveryKey = hypercoreCrypto.discoveryKey(publicKey)
      await this._putCoreBatch({
        name,
        publicKey,
        discoveryKey
      }, b)
    }

    return b.flush()

    async function loadKey (path) {
      try {
        const key = await fs.readFile(path)
        return key
      } catch (err) {
        if (err.code !== 'ENOENT') throw err
        return null
      }
    }
  }

  async _putCoreBatch (keys, batch) {
    const record = {
      name: keys.name,
      publicKey: toString(keys.publicKey),
      discoveryKey: toString(keys.discoveryKey)
    }
    const b = batch || this._db.batch()
    await b.put(KEYS_NAMESPACE + this._db.sep + record.publicKey, record)
    await b.put(DKEYS_NAMESPACE + this._db.sep + record.discoveryKey, record)
    if (!batch) await b.flush()
  }

  // Public Methods

  async getName (keys) {
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
