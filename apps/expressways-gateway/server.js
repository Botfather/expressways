const express = require('express')
const { spawn } = require('node:child_process')
const { randomUUID } = require('node:crypto')
const fs = require('node:fs/promises')
const path = require('node:path')

const app = express()
app.use(express.json({ limit: '1mb' }))
app.use(express.static(path.join(__dirname, 'public')))

const GATEWAY_PORT = Number(process.env.PORT || 8899)
const TRANSPORT = process.env.EXPRESSWAYS_TRANSPORT || 'tcp'
const ADDRESS = process.env.EXPRESSWAYS_ADDRESS || '127.0.0.1:7766'
const TOKEN_FILE = process.env.EXPRESSWAYS_TOKEN_FILE || path.resolve(__dirname, '../../var/auth/developer.token')
const TASKS_TOPIC = process.env.EXPRESSWAYS_TASKS_TOPIC || 'tasks'
const TASK_EVENTS_TOPIC = process.env.EXPRESSWAYS_TASK_EVENTS_TOPIC || 'task_events'
const RESULTS_TOPIC = process.env.EXPRESSWAYS_RESULTS_TOPIC || 'ollama_results'
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 1000)
const CONSUME_BATCH_LIMIT = Number(process.env.CONSUME_BATCH_LIMIT || 100)
const RESULT_DIR = process.env.OLLAMA_RESULT_DIR || path.resolve(__dirname, '../../var/agent/ollama-results')
const CTL_BIN = process.env.EXPRESSWAYSCTL_BIN || path.resolve(__dirname, '../../target/debug/expresswaysctl')

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
      env: process.env,
      stdio: ['ignore', 'pipe', 'pipe'],
    })

    let stdout = ''
    let stderr = ''

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString()
    })

    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString()
    })

    child.on('error', (error) => {
      reject(new Error(`failed to run expresswaysctl: ${error.message}`))
    })

    child.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`expresswaysctl exited with code ${code}: ${stderr || stdout}`))
        return
      }

      resolve(stdout.trim())
    })
  })
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
    nextOffset: Number(json.next_offset || offset),
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
    nextOffset: Number(json.next_offset || offset),
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
  const artifactPath = path.join(RESULT_DIR, `${taskId}.ollama.json`)
  try {
    const raw = await fs.readFile(artifactPath, 'utf8')
    return { artifactPath, artifact: JSON.parse(raw) }
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
  const startOffset = Number(req.query.offset || 0)

  if (!taskId) {
    res.status(400).json({ error: 'taskId is required' })
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
          response.artifactPath = artifact.artifactPath
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
          artifactPath: artifact.artifactPath,
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
    res.status(500).json({ error: error.message })
  }
})

app.post('/chat', async (req, res) => {
  const { prompt, model, system, temperature, maxTokens } = req.body || {}

  if (typeof prompt !== 'string' || prompt.trim().length === 0) {
    res.status(400).json({ error: 'prompt is required' })
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
    res.status(500).json({ error: error.message })
    return
  }

  res.status(202).json({
    taskId,
    eventsUrl: `/events/${encodeURIComponent(taskId)}`,
  })
})

app.get('/events/:taskId', async (req, res) => {
  const taskId = req.params.taskId
  let nextOffset = Number(req.query.offset || 0)
  let resultsOffset = Number(req.query.resultsOffset || 0)

  if (!taskId) {
    res.status(400).json({ error: 'taskId is required' })
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
              artifactPath: artifactResult.artifactPath,
              artifact: artifactResult.artifact,
            })
          }

          writeSse(res, 'end', { taskId, status: taskEvent.status })
          res.end()
          return
        }
      }
    } catch (error) {
      writeSse(res, 'error', { taskId, message: error.message })
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

app.listen(GATEWAY_PORT, () => {
  console.log(
    JSON.stringify({
      event: 'gateway_started',
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
