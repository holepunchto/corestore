// TODO: Extract this into a standalone module
const fs = require('fs').promises
const p = require('path')
const os = require('os')

const raf = require('random-access-file')
const sodium = require('sodium-universal')
const blake2b = require('blake2b-universal')

const SYSTEM_PROFILE_STORAGE = p.join(os.homedir(), '.hyperspace', 'profiles')
const DEFAULT_PROFILE_NAME = 'default'
const PROFILE_FILENAME = 'profile'

const DEFAULT_TOKEN = Buffer.alloc(0)
const NAMESPACE = Buffer.from('@hyperspace/key-manager')

module.exports = class KeyManager {
  constructor (profile, opts = {}) {
    this.profile = profile
  }

  _sign (keyPair, message) {
    if (!keyPair._secretKey) throw new Error('Invalid key pair')
    const signature = Buffer.allocUnsafe(sodium.crypto_sign_BYTES)
    sodium.crypto_sign_detached(signature, message, keyPair._secretKey)
    return signature
  }

  createSecret (name, token) {
    return deriveSeed(this.profile, token, name)
  }

  createHypercoreKeyPair (name, token) {
    const keyPair = {
      publicKey: Buffer.allocUnsafe(sodium.crypto_sign_PUBLICKEYBYTES),
      _secretKey: Buffer.alloc(sodium.crypto_sign_SECRETKEYBYTES),
      sign: (msg) => this._sign(keyPair, msg)
    }

    sodium.crypto_sign_seed_keypair(keyPair.publicKey, keyPair._secretKey, this.createSecret(name, token))

    return keyPair
  }

  static createToken () {
    return randomBytes(32)
  }

  static async forProfile (name, opts = {}) {
    if (typeof name === 'object') {
      opts = name
      name = null
    }
    const profilePath = p.join(opts.dir || SYSTEM_PROFILE_STORAGE, name || DEFAULT_PROFILE_NAME)

    let { profile, err } = await fs.readFile(profilePath).then(profile => ({ profile }), err => ({ err }))
    if (err) {
      if (err.code !== 'ENOENT') throw err
      profile = randomBytes(32)
      await fs.mkdir(p.dirname(profilePath), { recursive: true })
      await fs.writeFile(profilePath, profile)
    }

    return new this(profile, opts)
  }

  static async fromStorage (storage, opts = {}) {
    if (typeof storage === 'string') {
      const root = storage
      storage = path => raf(p.join(root, path))
    }
    if (typeof storage !== 'object' && typeof storage !== 'function') throw new Error('Storage must be a random-access-storage instance or a Function')
    const profileStorage = typeof storage === 'function' ? storage(opts.name || PROFILE_FILENAME) : storage

    const profile = await new Promise((resolve, reject) => {
      profileStorage.stat((err, st) => {
        if (err && err.code !== 'ENOENT') return reject(err)
        if (err || st.size < 32 || opts.overwrite) {
          const key = randomBytes(32)
          return profileStorage.write(0, key, err => {
            if (err) return reject(err)
            profileStorage.close(err => {
              if (err) return reject(err)
              return resolve(key)
            })
          })
        }
        profileStorage.read(0, 32, (err, key) => {
          if (err) return reject(err)
          profileStorage.close(err => {
            if (err) return reject(err)
            return resolve(key)
          })
        })
      })
    })

    return new this(profile, opts)
  }
}

function deriveSeed (profile, token, name, output) {
  if (token && token.length < 32) throw new Error('Token must be a Buffer with length >= 32')
  if (!name || typeof name !== 'string') throw new Error('name must be a String')
  if (!output) output = Buffer.alloc(32)

  blake2b.batch(output, [
    NAMESPACE,
    token || DEFAULT_TOKEN,
    Buffer.from(Buffer.byteLength(name, 'ascii') + '\n' + name, 'ascii')
  ], profile)

  return output
}

function randomBytes (n) {
  const buf = Buffer.allocUnsafe(n)
  sodium.randombytes_buf(buf)
  return buf
}
