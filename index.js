const { NanoresourcePromise: Nanoresource } = require('nanoresource-promise/emitter')
const raf = require('random-access-file')

const Replicator = require('./lib/replicator')
const Index = require('./lib/index')
const Loader = require('./lib/loader')
const errors = require('./lib/errors')

module.exports = class Corestore extends Nanoresource {
  constructor (storage, opts = {}) {
    super()

    if (typeof storage === 'string') storage = defaultStorage(storage)
    if (typeof storage !== 'function') throw new errors.InvalidStorageError()
    this.storage = storage

    this._namespace = opts._namespace || ['default']
    this._db = opts._db || new Index(this.storage, opts)
    this._loader = opts._loader || new Loader(this.storage, this._db, opts)
    this._replicator = opts._replicator || new Replicator(this.loader, opts)

    this._loader.on('core', core => {
      this._replicator.inject(core)
    })
    this._loader.on('error', err => this.emit('error', err))

    // Eagerly open.
    this.open()
  }

  // Nanoresource Methods

  async _open () {
    await this._db.open()
    return this._loader.open()
  }

  async _close () {
    await this._replicator.close()
    await this._loader.close()
    return this._db.close()
  }

  // Private Methods

  _validateGetOptions (opts) {
    if (typeof opts === 'object') {
      if (!Buffer.isBuffer(opts) && !opts.key && !opts.name) throw new errors.InvalidOptionsError()
      else opts = { key: opts }
    } else {
      opts = { key: Buffer.from(opts, 'hex') }
    }
    if (opts.key && opts.key.length !== 64) throw new errors.InvalidKeyError()
    return opts
  }

  // Public Methods

  get (opts = {}) {
    opts = this._validateGetOptions(opts)
    if (!this.opened) this.open().catch(err => this.emit('error', err))
    return this._loader.get(this._namespace, opts)
  }

  namespace (name) {
    if (!name) throw new Error('A name must be provided as the first argument.')
    if (Buffer.isBuffer(name)) name = name.toString('hex')
    return new Corestore(this.storage, {
      namespace: [...this._namespace, name],
      _db: this._db,
      _loader: this._loader,
      _replicator: this._replicator
    })
  }

  replicate (isInitiator, opts) {
    return this._replicator.replicate(isInitiator, opts)
  }

  // Backup/Restore

  async backup () {
    if (!this.opened) await this.open()
    const allNames = await this._db.getAllNames()
    return {
      masterKey: this._masterKey.toString('hex'),
      names: [...allNames]
    }
  }

  restore (manifest) {
    return this._db.restore(manifest)
  }

  static async restore (manifest, target) {
    if (!manifest || !manifest.masterKey) throw new Error('Malformed manifest.')
    const store = new this(target, {
      masterKey: Buffer.from(manifest.masterKey, 'hex')
    })
    await store.restore(manifest)
    return store
  }
}

function defaultStorage (dir) {
  return function (name) {
    try {
      var lock = name.endsWith('/bitfield') ? require('fd-lock') : null
    } catch (err) {}
    return raf(name, { directory: dir, lock: lock })
  }
}
