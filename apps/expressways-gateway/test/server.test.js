const assert = require('node:assert/strict')
const { after, before, test } = require('node:test')
const { app, constantTimeEqual, nonNegativeOffset, validTaskId } = require('../server')

let server
let baseUrl

before(async () => {
  server = app.listen(0, '127.0.0.1')
  await new Promise((resolve, reject) => {
    server.once('listening', resolve)
    server.once('error', reject)
  })
  const address = server.address()
  baseUrl = `http://127.0.0.1:${address.port}`
})

after(async () => {
  await new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()))
  })
})

test('validation helpers reject ambiguous values', () => {
  assert.equal(validTaskId('task-123'), true)
  assert.equal(validTaskId('../escape'), false)
  assert.equal(nonNegativeOffset('12'), 12)
  assert.equal(nonNegativeOffset('-1'), null)
  assert.equal(nonNegativeOffset('not-a-number'), null)
  assert.equal(constantTimeEqual('secret', 'secret'), true)
  assert.equal(constantTimeEqual('secret', 'different'), false)
})

test('health response carries defensive headers', async () => {
  const response = await fetch(`${baseUrl}/health`)
  assert.equal(response.status, 200)
  assert.equal(response.headers.get('x-content-type-options'), 'nosniff')
  assert.match(response.headers.get('content-security-policy'), /frame-ancestors 'none'/)
})

test('chat validates input before invoking the broker CLI', async () => {
  const response = await fetch(`${baseUrl}/chat`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ prompt: 'hello', temperature: 99 }),
  })
  assert.equal(response.status, 400)
  assert.deepEqual(await response.json(), { error: 'temperature must be between 0 and 2' })
})

test('result lookup rejects invalid offsets before broker access', async () => {
  const response = await fetch(`${baseUrl}/results/task-1?offset=-1`)
  assert.equal(response.status, 400)
})
