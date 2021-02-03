const fs = require('fs').promises

async function cleanup (dirs) {
  return Promise.allSettled(dirs.map(dir => fs.rmdir(dir, { recursive: true })))
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
