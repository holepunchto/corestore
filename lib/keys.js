// TODO: Extract this into a standalone module

const sodium = require('sodium-universal')
const blake2b = require('blake2b-universal')
const b4a = require('b4a')

const DEFAULT_TOKEN = b4a.alloc(0)
const NAMESPACE = b4a.from('@hyperspace/key-manager')

module.exports = class KeyManager {
  constructor (storage, profile, opts = {}) {
    this.storage = storage
    this.profile = profile
  }

  _sign (keyPair, message) {
    if (!keyPair._secretKey) throw new Error('Invalid key pair')
    const signature = b4a.allocUnsafe(sodium.crypto_sign_BYTES)
    sodium.crypto_sign_detached(signature, message, keyPair._secretKey)
    return signature
  }

  createSecret (name, token) {
    return deriveSeed(this.profile, token, name)
  }

  createHypercoreKeyPair (name, token) {
    const keyPair = {
      publicKey: b4a.allocUnsafe(sodium.crypto_sign_PUBLICKEYBYTES),
      _secretKey: b4a.alloc(sodium.crypto_sign_SECRETKEYBYTES),
      auth: {
        sign: (msg) => this._sign(keyPair, msg),
        verify: (signable, signature) => {
          return sodium.crypto_sign_detached(signature, signable, keyPair.publicKey)
        }
      }
    }

    sodium.crypto_sign_seed_keypair(keyPair.publicKey, keyPair._secretKey, this.createSecret(name, token))

    return keyPair
  }

  createNetworkIdentity (name, token) {
    const keyPair = {
      publicKey: b4a.alloc(32),
      secretKey: b4a.alloc(64)
    }

    sodium.crypto_sign_seed_keypair(keyPair.publicKey, keyPair.secretKey, this.createSecret(name, token))

    return keyPair
  }

  close () {
    return new Promise((resolve, reject) => {
      this.storage.close(err => {
        if (err) return reject(err)
        return resolve()
      })
    })
  }

  static createToken () {
    return randomBytes(32)
  }

  static async fromStorage (storage, opts = {}) {
    const profileStorage = storage(opts.name || 'default')

    const profile = await new Promise((resolve, reject) => {
      profileStorage.stat((err, st) => {
        if (err && err.code !== 'ENOENT') return reject(err)
        if (err || st.size < 32 || opts.overwrite) {
          const key = randomBytes(32)
          return profileStorage.write(0, key, err => {
            if (err) return reject(err)
            return resolve(key)
          })
        }
        profileStorage.read(0, 32, (err, key) => {
          if (err) return reject(err)
          return resolve(key)
        })
      })
    })

    return new this(profileStorage, profile, opts)
  }
}

function deriveSeed (profile, token, name, output) {
  if (token && token.length < 32) throw new Error('Token must be a Buffer with length >= 32')
  if (!name || typeof name !== 'string') throw new Error('name must be a String')
  if (!output) output = b4a.alloc(32)

  blake2b.batch(output, [
    NAMESPACE,
    token || DEFAULT_TOKEN,
    b4a.from(b4a.byteLength(name, 'ascii') + '\n' + name, 'ascii')
  ], profile)

  return output
}

function randomBytes (n) {
  const buf = b4a.allocUnsafe(n)
  sodium.randombytes_buf(buf)
  return buf
}
