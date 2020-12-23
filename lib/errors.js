const CustomError = require('custom-error-class')

class InvalidKeyError extends CustomError {
  constructor () {
    super('Invalid hypercore key')
    this.invalidKey = true
  }
}

class InvalidOptionsError extends CustomError {
  constructor () {
    super('Invalid get options')
    this.invalidOptions = true
  }
}

class InvalidStorageError extends CustomError {
  constructor () {
    super('Storage should be a function or a string')
    this.invalidStorage = true
  }
}

class InvalidNamespaceError extends CustomError {
  constructor () {
    super('Invalid namespace')
    this.invalidNamespace = true
  }
}

class MalformedManifestError extends CustomError {
  constructor () {
    super('Invalid backup/restore manifest')
    this.invalidManifest = true
  }
}

module.exports = {
  InvalidStorageError,
  InvalidKeyError,
  InvalidOptionsError,
  InvalidNamespaceError,
  MalformedManifestError
}
