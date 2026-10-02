const express = require('express')
const { spawn } = require('node:child_process')
const { randomUUID, timingSafeEqual } = require('node:crypto')
const fs = require('node:fs/promises')
const path = require('node:path')

const app = express()

function boundedInteger(name, fallback, minimum, maximum) {
  const value = Number(process.env[name] || fallback)
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`)
  }
  return value
}

const GATEWAY_HOST = process.env.HOST || '127.0.0.1'
const GATEWAY_PORT = boundedInteger('PORT', 8899, 1, 65535)
const TRANSPORT = process.env.EXPRESSWAYS_TRANSPORT || 'tcp'
const ADDRESS = process.env.EXPRESSWAYS_ADDRESS || '127.0.0.1:7766'
const TOKEN_FILE = process.env.EXPRESSWAYS_TOKEN_FILE || path.resolve(__dirname, '../../var/auth/developer.token')
const TASKS_TOPIC = process.env.EXPRESSWAYS_TASKS_TOPIC || 'tasks'
const TASK_EVENTS_TOPIC = process.env.EXPRESSWAYS_TASK_EVENTS_TOPIC || 'task_events'
const RESULTS_TOPIC = process.env.EXPRESSWAYS_RESULTS_TOPIC || 'ollama_results'
const POLL_INTERVAL_MS = boundedInteger('POLL_INTERVAL_MS', 1000, 100, 60000)
const CONSUME_BATCH_LIMIT = boundedInteger('CONSUME_BATCH_LIMIT', 100, 1, 1000)
const REQUEST_TIMEOUT_MS = boundedInteger('REQUEST_TIMEOUT_MS', 15000, 100, 300000)
const MAX_CONNECTIONS = boundedInteger('MAX_CONNECTIONS', 64, 1, 1024)
const MAX_CHILD_OUTPUT_BYTES = boundedInteger('MAX_CHILD_OUTPUT_BYTES', 1048576, 1024, 16777216)
const MAX_ARTIFACT_BYTES = boundedInteger('MAX_ARTIFACT_BYTES', 1048576, 1024, 16777216)
const ACCESS_BEARER = process.env.GATEWAY_ACCESS_BEARER || ''
const RESULT_DIR = process.env.OLLAMA_RESULT_DIR || path.resolve(__dirname, '../../var/agent/ollama-results')
const CTL_BIN = process.env.EXPRESSWAYSCTL_BIN || path.resolve(__dirname, '../../target/debug/expresswaysctl')

const loopbackHosts = new Set(['127.0.0.1', '::1', 'localhost'])
if (!loopbackHosts.has(GATEWAY_HOST) && ACCESS_BEARER.length === 0) {
  throw new Error('GATEWAY_ACCESS_BEARER is required for a non-loopback HOST')
}

function constantTimeEqual(left, right) {
  const leftBytes = Buffer.from(left)
  const rightBytes = Buffer.from(right)
  if (leftBytes.length !== rightBytes.length) {
    return false
  }
  return timingSafeEqual(leftBytes, rightBytes)
}

let activeConnections = 0
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff')
  res.setHeader('Referrer-Policy', 'no-referrer')
  res.setHeader('Content-Security-Policy', "default-src 'self'; connect-src 'self'; img-src 'self' data:; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")

  if (ACCESS_BEARER.length > 0) {
    const authorization = req.get('authorization') || ''
    const provided = authorization.startsWith('Bearer ') ? authorization.slice(7) : ''
    if (!constantTimeEqual(provided, ACCESS_BEARER)) {
      res.status(401).json({ error: 'bearer authentication required' })
      return
    }
  }

  if (activeConnections >= MAX_CONNECTIONS) {
    res.status(503).json({ error: 'gateway connection limit reached' })
    return
  }
  activeConnections += 1
  let released = false
  const release = () => {
    if (!released) {
      released = true
      activeConnections -= 1
    }
  }
  res.once('finish', release)
  res.once('close', release)
  next()
})
app.use(express.json({ limit: '1mb', strict: true }))
app.use(express.static(path.join(__dirname, 'public'), { dotfiles: 'deny', index: 'index.html' }))

function buildCtlArgs(commandArgs) {
  return [
    '--transport',
    TRANSPORT,
    '--address',
    ADDRESS,
    ...commandArgs,
  ]
}

function runCtl(commandArgs) {
  return new Promise((resolve, reject) => {
    const args = buildCtlArgs(commandArgs)
    const child = spawn(CTL_BIN, args, {
      cwd: path.resolve(__dirname, '../..'),
      env: {
        PATH: process.env.PATH || '',
        LANG: process.env.LANG || 'C.UTF-8',
        LC_ALL: process.env.LC_ALL || '',
        RUST_LOG: process.env.RUST_LOG || 'warn',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    })

    let stdout = ''
    let stderr = ''
    let outputBytes = 0
    let settled = false
    const finish = (callback) => {
      if (!settled) {
        settled = true
        clearTimeout(timer)
        callback()
      }
    }
    const timer = setTimeout(() => {
      child.kill('SIGKILL')
      finish(() => reject(new Error('expresswaysctl timed out')))
    }, REQUEST_TIMEOUT_MS)

    child.stdout.on('data', (chunk) => {
      outputBytes += chunk.length
      if (outputBytes > MAX_CHILD_OUTPUT_BYTES) {
        child.kill('SIGKILL')
        finish(() => reject(new Error('expresswaysctl output exceeded its limit')))
        return
      }
      stdout += chunk.toString()
    })

    child.stderr.on('data', (chunk) => {
      outputBytes += chunk.length
      if (outputBytes > MAX_CHILD_OUTPUT_BYTES) {
        child.kill('SIGKILL')
        finish(() => reject(new Error('expresswaysctl output exceeded its limit')))
        return
      }
      stderr += chunk.toString()
    })

    child.on('error', (error) => {
      finish(() => reject(new Error(`failed to run expresswaysctl: ${error.message}`)))
    })

    child.on('close', (code) => {
      if (code !== 0) {
        finish(() => reject(new Error(`expresswaysctl exited with code ${code}`)))
        return
      }

      finish(() => resolve(stdout.trim()))
    })
  })
}

function validTaskId(taskId) {
  return typeof taskId === 'string' && /^[A-Za-z0-9._:-]{1,256}$/.test(taskId)
}

function nonNegativeOffset(value) {
  const parsed = Number(value || 0)
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null
}

function reportInternalError(context, error) {
  console.error(JSON.stringify({ event: 'gateway_error', context, message: error.message }))
}

function parseJsonOutput(output) {
  const trimmed = output.trim()
  if (!trimmed) {
    return null
  }

  try {
    return JSON.parse(trimmed)
  } catch {
    const firstBrace = trimmed.indexOf('{')
    if (firstBrace === -1) {
      return null
    }
    return JSON.parse(trimmed.slice(firstBrace))
  }
}

async function consumeTaskEvents(offset) {
  const output = await runCtl([
    'consume',
    '--token-file',
    TOKEN_FILE,
    '--topic',
    TASK_EVENTS_TOPIC,
    '--offset',
    String(offset),
    '--limit',
    String(CONSUME_BATCH_LIMIT),
  ])

  const json = parseJsonOutput(output)
  if (!json || json.type !== 'messages' || !Array.isArray(json.messages)) {
    return { nextOffset: offset, events: [] }
  }

  const events = []
  for (const message of json.messages) {
    try {
      const event = JSON.parse(message.payload)
      events.push({
        offset: message.offset,
        event,
      })
    } catch {
      // Ignore malformed event payloads in this minimal gateway.
    }
  }

  return {
    nextOffset: nonNegativeOffset(json.next_offset) ?? offset,
    events,
  }
}

async function consumeResultMessages(offset) {
  const output = await runCtl([
    'consume',
    '--token-file',
    TOKEN_FILE,
    '--topic',
    RESULTS_TOPIC,
    '--offset',
    String(offset),
    '--limit',
    String(CONSUME_BATCH_LIMIT),
  ])

  const json = parseJsonOutput(output)
  if (!json || json.type !== 'messages' || !Array.isArray(json.messages)) {
    return { nextOffset: offset, results: [] }
  }

  const results = []
  for (const message of json.messages) {
    try {
      const result = JSON.parse(message.payload)
      results.push({
        offset: message.offset,
        result,
      })
    } catch {
      // Ignore malformed result payloads in this minimal gateway.
    }
  }

  return {
    nextOffset: nonNegativeOffset(json.next_offset) ?? offset,
    results,
  }
}

async function findLatestResultForTask(taskId, startOffset = 0, maxBatches = 50) {
  let offset = Number(startOffset) || 0
  let latest = null

  for (let i = 0; i < maxBatches; i += 1) {
    const { results, nextOffset } = await consumeResultMessages(offset)

    for (const item of results) {
      if (!item.result || item.result.task_id !== taskId) {
        continue
      }
      latest = item
    }

    if (results.length === 0 || nextOffset <= offset) {
      break
    }

    offset = nextOffset
  }

  return latest
}

async function tryReadTaskArtifact(taskId) {
  if (!validTaskId(taskId)) {
    return null
  }
  const artifactPath = path.join(RESULT_DIR, `${taskId}.ollama.json`)
  try {
    const metadata = await fs.lstat(artifactPath)
    if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.size > MAX_ARTIFACT_BYTES) {
      return null
    }
    const raw = await fs.readFile(artifactPath, 'utf8')
    return { artifact: JSON.parse(raw) }
  } catch {
    return null
  }
}

function writeSse(res, event, data) {
  res.write(`event: ${event}\n`)
  res.write(`data: ${JSON.stringify(data)}\n\n`)
}

app.get('/health', (_req, res) => {
  res.json({ status: 'ok' })
})

app.get('/results/:taskId', async (req, res) => {
  const taskId = req.params.taskId
  const includeArtifact = req.query.includeArtifact !== 'false'
  const startOffset = nonNegativeOffset(req.query.offset)

  if (!validTaskId(taskId)) {
    res.status(400).json({ error: 'taskId is invalid' })
    return
  }
  if (startOffset === null) {
    res.status(400).json({ error: 'offset must be a non-negative integer' })
    return
  }

  try {
    const latest = await findLatestResultForTask(taskId, startOffset)

    if (latest) {
      const response = {
        taskId,
        source: 'results_topic',
        offset: latest.offset,
        result: latest.result,
      }

      if (includeArtifact) {
        const artifact = await tryReadTaskArtifact(taskId)
        if (artifact) {
          response.artifact = artifact.artifact
        }
      }

      res.json(response)
      return
    }

    if (includeArtifact) {
      const artifact = await tryReadTaskArtifact(taskId)
      if (artifact) {
        res.json({
          taskId,
          source: 'artifact_file',
          artifact: artifact.artifact,
        })
        return
      }
    }

    res.status(404).json({
      error: 'result not found',
      taskId,
      hint: 'Result may not be ready yet, or was not published to results topic.',
    })
  } catch (error) {
    reportInternalError('results', error)
    res.status(500).json({ error: 'result lookup failed' })
  }
})

app.post('/chat', async (req, res) => {
  const { prompt, model, system, temperature, maxTokens } = req.body || {}

  if (typeof prompt !== 'string' || prompt.trim().length === 0 || Buffer.byteLength(prompt) > 65536) {
    res.status(400).json({ error: 'prompt must contain 1..65536 bytes' })
    return
  }
  if (model !== undefined && (typeof model !== 'string' || model.length === 0 || model.length > 256)) {
    res.status(400).json({ error: 'model must contain 1..256 characters' })
    return
  }
  if (system !== undefined && (typeof system !== 'string' || Buffer.byteLength(system) > 65536)) {
    res.status(400).json({ error: 'system must be a string no larger than 65536 bytes' })
    return
  }
  if (temperature !== undefined && (!Number.isFinite(temperature) || temperature < 0 || temperature > 2)) {
    res.status(400).json({ error: 'temperature must be between 0 and 2' })
    return
  }
  if (maxTokens !== undefined && (!Number.isSafeInteger(maxTokens) || maxTokens < 1 || maxTokens > 1000000)) {
    res.status(400).json({ error: 'maxTokens must be an integer between 1 and 1000000' })
    return
  }

  const taskId = randomUUID()
  const payload = {
    prompt,
    model,
    system,
    temperature,
    max_tokens: Number.isFinite(maxTokens) ? maxTokens : undefined,
  }

  try {
    await runCtl([
      'submit-task',
      '--token-file',
      TOKEN_FILE,
      '--topic',
      TASKS_TOPIC,
      '--task-id',
      taskId,
      '--task-type',
      'ollama_chat',
      '--skill',
      'chat',
      '--payload-json',
      JSON.stringify(payload),
    ])
  } catch (error) {
    reportInternalError('submit', error)
    res.status(500).json({ error: 'task submission failed' })
    return
  }

  res.status(202).json({
    taskId,
    eventsUrl: `/events/${encodeURIComponent(taskId)}`,
  })
})

app.get('/events/:taskId', async (req, res) => {
  const taskId = req.params.taskId
  let nextOffset = nonNegativeOffset(req.query.offset)
  let resultsOffset = nonNegativeOffset(req.query.resultsOffset)

  if (!validTaskId(taskId)) {
    res.status(400).json({ error: 'taskId is invalid' })
    return
  }
  if (nextOffset === null || resultsOffset === null) {
    res.status(400).json({ error: 'offsets must be non-negative integers' })
    return
  }

  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')
  res.flushHeaders()

  writeSse(res, 'ready', { taskId, nextOffset, resultsOffset })

  let closed = false
  req.on('close', () => {
    closed = true
  })

  while (!closed) {
    try {
      const { events, nextOffset: consumedNextOffset } = await consumeTaskEvents(nextOffset)
      nextOffset = consumedNextOffset

      const { results, nextOffset: consumedResultsOffset } = await consumeResultMessages(resultsOffset)
      resultsOffset = consumedResultsOffset

      for (const item of results) {
        if (!item.result || item.result.task_id !== taskId) {
          continue
        }
        writeSse(res, 'result', {
          offset: item.offset,
          taskId,
          result: item.result,
        })
      }

      for (const item of events) {
        const taskEvent = item.event
        if (!taskEvent || taskEvent.task_id !== taskId) {
          continue
        }

        writeSse(res, 'task_event', {
          offset: item.offset,
          taskId,
          event: taskEvent,
        })

        if (['completed', 'failed', 'canceled', 'timed_out', 'exhausted'].includes(taskEvent.status)) {
          const finalResults = await consumeResultMessages(resultsOffset)
          resultsOffset = finalResults.nextOffset
          for (const resultItem of finalResults.results) {
            if (!resultItem.result || resultItem.result.task_id !== taskId) {
              continue
            }
            writeSse(res, 'result', {
              offset: resultItem.offset,
              taskId,
              result: resultItem.result,
            })
          }

          const artifactResult = await tryReadTaskArtifact(taskId)
          if (artifactResult) {
            writeSse(res, 'result', {
              taskId,
              artifact: artifactResult.artifact,
            })
          }

          writeSse(res, 'end', { taskId, status: taskEvent.status })
          res.end()
          return
        }
      }
    } catch (error) {
      reportInternalError('events', error)
      writeSse(res, 'error', { taskId, message: 'event stream failed' })
      res.end()
      return
    }

    writeSse(res, 'keepalive', {
      taskId,
      nextOffset,
      resultsOffset,
      at: new Date().toISOString(),
    })
    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS))
  }
})

function startGateway() {
  return app.listen(GATEWAY_PORT, GATEWAY_HOST, () => {
    console.log(
      JSON.stringify({
        event: 'gateway_started',
        host: GATEWAY_HOST,
        port: GATEWAY_PORT,
        transport: TRANSPORT,
        address: ADDRESS,
        tasksTopic: TASKS_TOPIC,
        taskEventsTopic: TASK_EVENTS_TOPIC,
        resultsTopic: RESULTS_TOPIC,
        resultDir: RESULT_DIR,
        ctlBin: CTL_BIN,
      })
    )
  })
}

if (require.main === module) {
  startGateway()
}

module.exports = { app, constantTimeEqual, nonNegativeOffset, startGateway, validTaskId }
