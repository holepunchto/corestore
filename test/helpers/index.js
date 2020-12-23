const fs = require('fs')

async function cleanup (dirs) {
  return Promise.all(dirs.map(dir => new Promise((resolve, reject) => {
    fs.rmdir(dir, { recursive: true }, err => {
      if (err) return reject(err)
      return resolve()
    })
  })))
}

function delay (ms, cb) {
  return new Promise(resolve => {
    setTimeout(() => {
      if (cb) cb()
      resolve()
    }, ms)
  })
}

module.exports = {
  delay,
  cleanup
}
