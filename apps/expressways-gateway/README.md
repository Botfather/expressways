# Expressways Gateway (Browser + SSE)

Minimal Node gateway that lets a browser submit chat tasks into Expressways and receive task updates over Server-Sent Events.

## What it does

- `POST /chat` submits an `ollama_chat` task to the `tasks` topic.
- `GET /events/:taskId` streams matching `task_events` updates for that task.
- Streams dedicated `ollama_results` topic messages as `result` SSE events.
- When the task reaches a terminal status, the gateway attempts to read the Ollama artifact from `./var/agent/ollama-results/<task-id>.ollama.json` and emits it as a `result` SSE event.

## Browser demo

Open `http://127.0.0.1:8899/` for a minimal in-browser chat UI that submits prompts and streams task/result updates live.

## Prerequisites

- Expressways broker running.
- `expresswaysctl` built at `./target/debug/expresswaysctl`.
- Capability token file at `./var/auth/developer.token`.
- Ollama worker binary running (`expressways-agent-ollama`).

## Install and run

```bash
cd apps/expressways-gateway
npm install
npm start
```

Default gateway URL: `http://127.0.0.1:8899`

## API

### Submit prompt

```bash
curl -sS -X POST http://127.0.0.1:8899/chat \
  -H 'content-type: application/json' \
  -d '{"prompt":"Explain the CAP theorem in one paragraph.","model":"llama3.2"}'
```

Response:

```json
{
  "taskId": "...",
  "eventsUrl": "/events/..."
}
```

### Stream task events

```bash
curl -N http://127.0.0.1:8899/events/<task-id>
```

SSE event types:

- `ready`
- `task_event`
- `keepalive`
- `result`
- `end`
- `error`

### Fetch latest result by task ID

Use this endpoint when a browser tab reloads and you want the latest known result without reopening the SSE stream history.

```bash
curl -sS http://127.0.0.1:8899/results/<task-id>
```

Optional query parameters:

- `offset` (default `0`) to begin scanning the results topic from a known offset.
- `includeArtifact` (`true` by default, set `false` to skip artifact file lookup).

## Environment variables

- `PORT` (default `8899`)
- `EXPRESSWAYS_TRANSPORT` (default `tcp`)
- `EXPRESSWAYS_ADDRESS` (default `127.0.0.1:7766`)
- `EXPRESSWAYS_TOKEN_FILE` (default `../../var/auth/developer.token`)
- `EXPRESSWAYS_TASKS_TOPIC` (default `tasks`)
- `EXPRESSWAYS_TASK_EVENTS_TOPIC` (default `task_events`)
- `EXPRESSWAYS_RESULTS_TOPIC` (default `ollama_results`)
- `OLLAMA_RESULT_DIR` (default `../../var/agent/ollama-results`)
- `EXPRESSWAYSCTL_BIN` (default `../../target/debug/expresswaysctl`)
- `POLL_INTERVAL_MS` (default `1000`)
- `CONSUME_BATCH_LIMIT` (default `100`)

## One-command local stack

From repository root:

```bash
make run-ollama-stack
```

This runs broker + orchestrator + Ollama agent + gateway and serves the browser demo at `http://127.0.0.1:8899/`.
