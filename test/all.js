const p = require('path')
const ram = require('random-access-memory')
const raf = require('random-access-file')
const datEncoding = require('dat-encoding')
const rimraf = require('rimraf')
const test = require('tape')

const corestore = require('..')

test('ram-based corestore, different get options', async t => {
  const store1 = corestore(ram)
  const core1 = store1.default()
  var core2, core3, core4, core5

  await runAll([
    cb => core1.ready(cb),
    cb => core1.append('hello', cb),
    cb => {
      // Buffer arg
      core2 = store1.get(core1.key)
      return core2.ready(cb)
    },
    cb => {
      // Object arg
      core3 = store1.get({ key: core1.key })
      return core3.ready(cb)
    },
    cb => {
      // Discovery key option
      core4 = store1.get({ discoveryKey: core1.discoveryKey })
      return core4.ready(cb)
    },
    cb => {
      // String option
      core5 = store1.get({ key: datEncoding.encode(core1.key) })
      return core5.ready(cb)
    }
  ])

  t.same(core1, core2)
  t.same(core1, core3)
  t.same(core1, core4)
  t.same(core1, core5)
  t.end()
})

test('ram-based corestore, simple replication', async t => {
  const store1 = corestore(ram)
  const store2 = corestore(ram)
  const core1 = store1.default()
  const core2 = store1.get()
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => core2.ready(cb),
    cb => {
      core3 = store2.default(core1.key)
      return core3.ready(cb)
    },
    cb => {
      core4 = store2.get({ key: core2.key })
      return core4.ready(cb)
    },
    cb => core1.append('hello', cb),
    cb => core1.append('world', cb),
    cb => core2.append('cat', cb),
    cb => core2.append('dog', cb),
    cb => {
      const stream = store1.replicate()
      stream.pipe(store2.replicate()).pipe(stream)
      stream.on('end', cb)
    }
  ])

  await validateCore(t, core3, [Buffer.from('hello'), Buffer.from('world')])
  await validateCore(t, core4, [Buffer.from('cat'), Buffer.from('dog')])
  t.end()
})

test('raf-based corestore, simple replication', async t => {
  const store1 = corestore(path => raf(p.join('store1', path)))
  const store2 = corestore(path => raf(p.join('store2', path)))
  const core1 = store1.default()
  const core2 = store1.get()
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => core2.ready(cb),
    cb => {
      core3 = store2.default({ key: core1.key })
      return core3.ready(cb)
    },
    cb => {
      core4 = store2.get({ key: core2.key })
      return core4.ready(cb)
    },
    cb => core1.append('hello', cb),
    cb => core1.append('world', cb),
    cb => core2.append('cat', cb),
    cb => core2.append('dog', cb),
    cb => {
      const stream = store1.replicate()
      stream.pipe(store2.replicate()).pipe(stream)
      stream.on('end', cb)
    }
  ])

  await validateCore(t, core3, [Buffer.from('hello'), Buffer.from('world')])
  await validateCore(t, core4, [Buffer.from('cat'), Buffer.from('dog')])
  await cleanup(['store1', 'store2'])
  t.end()
})

test('raf-based corestore, close and reopen', async t => {
  var store = corestore(path => raf(p.join('store', path)))
  var core = store.default()

  await runAll([
    cb => core.ready(cb),
    cb => core.append('hello', cb),
    cb => store.close(cb),
    cb => {
      t.true(core.closed)
      return process.nextTick(cb, null)
    },
    cb => {
      store =  corestore(path => raf(p.join('store', path)))
      core = store.default()
      return core.ready(cb)
    }
  ])

  await validateCore(t, core, [Buffer.from('hello')])
  await cleanup(['store1'])
  t.end()
})

test('live replication with an additional core', async t => {
  const store1 = corestore(ram)
  const store2 = corestore(ram)
  const core1 = store1.default()
  var core2 = null
  var core3 = null
  var core4 = null

  await runAll([
    cb => core1.ready(cb),
    cb => {
      core3 = store2.default({ key: core1.key })
      return core3.ready(cb)
    },
    cb => {
      const stream = store1.replicate({ live: true })
      stream.pipe(store2.replicate({ live: true })).pipe(stream)
      return cb(null)
    },
    cb => {
      core2 = store1.get()
      return core2.ready(cb)
    },
    cb => {
      core4 = store2.get(core2.key)
      return core4.ready(cb)
    },
    cb => core2.append('hello', cb),
    cb => core2.append('world', cb),
    cb => delay(500, cb)
  ])

  await validateCore(t, core4, [Buffer.from('hello'), Buffer.from('world')])
  t.end()
})

function runAll (ops) {
  return new Promise((resolve, reject) => {
    runNext(ops.shift())
    function runNext (op) {
      op(err => {
        if (err) return reject(err)
        let next = ops.shift()
        if (!next) return resolve()
        return runNext(next)
      })
    }
  })
}

function validateCore(t, core, values) {
  const ops = values.map((v, idx) => cb => {
    core.get(idx, (err, value) => {
      t.error(err, 'no error')
      t.same(value, values[idx])
      return cb(null)
    })
  })
  return runAll(ops)
}

async function cleanup (dirs) {
  return Promise.all(dirs.map(dir => new Promise((resolve, reject) => {
    rimraf(dir, err => {
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
