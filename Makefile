.PHONY: benchmark generate-token help prepare-local-dirs run-expressways run-orchestrator run-dashboard run-stack run-ollama-agent run-gateway run-ollama-stack

EXPRESSWAYS_CONFIG ?= configs/expressways.example.toml
BROKER_ADDRESS ?= 127.0.0.1:7766
TOKEN_FILE ?= ./var/auth/developer.token
STATE_PATH ?= ./var/orchestrator/state.json
TASKS_TOPIC ?= tasks
TASK_EVENTS_TOPIC ?= task_events
DASHBOARD_LISTEN ?= 127.0.0.1:8787
STARTUP_DELAY_SECONDS ?= 1
OLLAMA_AGENT_ID ?= ollama-chat-agent
OLLAMA_MODEL ?= llama3.2
OLLAMA_URL ?= http://127.0.0.1:11434
OLLAMA_RESULTS_TOPIC ?= ollama_results
GATEWAY_PORT ?= 8899
ORCHESTRATOR_RETRY_DELAY_MS ?= 500
ORCHESTRATOR_CONSUME_LIMIT ?= 25
ORCHESTRATOR_POLL_INTERVAL_MS ?= 1000

help:
	@echo "Available targets:"
	@echo "  make help              Show this command summary"
	@echo "  make run-expressways   Start the local Expressways broker"
	@echo "  make run-orchestrator  Start the local task supervisor"
	@echo "  make run-dashboard     Start the local dashboard server"
	@echo "  make run-stack         Start broker, supervisor, and dashboard together"
	@echo "  make run-ollama-agent  Start the Ollama AgentWorker bridge"
	@echo "  make run-gateway       Start the browser SSE gateway"
	@echo "  make run-ollama-stack  Start broker, orchestrator, Ollama worker, and gateway"
	@echo "  make benchmark         Run the benchmark suite"

prepare-local-dirs:
	@mkdir -p ./tmp ./var/auth ./var/agent ./var/benchmarks ./var/orchestrator

run-expressways: prepare-local-dirs
	cargo run -p expressways-server -- --config $(EXPRESSWAYS_CONFIG)

run-orchestrator: prepare-local-dirs
	cargo run -p expressways-orchestrator -- --transport tcp --address $(BROKER_ADDRESS) supervise --token-file $(TOKEN_FILE) --state-path $(STATE_PATH) --retry-delay-ms $(ORCHESTRATOR_RETRY_DELAY_MS) --tasks-topic $(TASKS_TOPIC) --task-events-topic $(TASK_EVENTS_TOPIC) --consume-limit $(ORCHESTRATOR_CONSUME_LIMIT) --poll-interval-ms $(ORCHESTRATOR_POLL_INTERVAL_MS)

run-dashboard: prepare-local-dirs
	cargo run -p expressways-orchestrator -- --transport tcp --address $(BROKER_ADDRESS) serve-dashboard --token-file $(TOKEN_FILE) --state-path $(STATE_PATH) --listen $(DASHBOARD_LISTEN) --task-events-topic $(TASK_EVENTS_TOPIC)

run-ollama-agent: prepare-local-dirs
	cargo run -p expressways-client --bin expressways-agent-ollama -- --transport tcp --address $(BROKER_ADDRESS) --token-file $(TOKEN_FILE) --agent-id $(OLLAMA_AGENT_ID) --default-model $(OLLAMA_MODEL) --ollama-url $(OLLAMA_URL) --task-events-topic $(TASK_EVENTS_TOPIC) --results-topic $(OLLAMA_RESULTS_TOPIC)

run-gateway: prepare-local-dirs
	@cd apps/expressways-gateway && npm install
	@cd apps/expressways-gateway && PORT=$(GATEWAY_PORT) EXPRESSWAYS_TRANSPORT=tcp EXPRESSWAYS_ADDRESS=$(BROKER_ADDRESS) EXPRESSWAYS_TOKEN_FILE=$(TOKEN_FILE) EXPRESSWAYS_TASK_EVENTS_TOPIC=$(TASK_EVENTS_TOPIC) EXPRESSWAYS_RESULTS_TOPIC=$(OLLAMA_RESULTS_TOPIC) npm start

run-stack: prepare-local-dirs
	@set -eu; \
	server_pid=""; \
	orchestrator_pid=""; \
	trap 'if [ -n "$$orchestrator_pid" ]; then kill "$$orchestrator_pid" 2>/dev/null || true; fi; if [ -n "$$server_pid" ]; then kill "$$server_pid" 2>/dev/null || true; fi' INT TERM EXIT; \
	cargo run -p expressways-server -- --config $(EXPRESSWAYS_CONFIG) & \
	server_pid=$$!; \
	sleep $(STARTUP_DELAY_SECONDS); \
	cargo run -p expressways-orchestrator -- --transport tcp --address $(BROKER_ADDRESS) supervise --token-file $(TOKEN_FILE) --state-path $(STATE_PATH) --retry-delay-ms $(ORCHESTRATOR_RETRY_DELAY_MS) --tasks-topic $(TASKS_TOPIC) --task-events-topic $(TASK_EVENTS_TOPIC) --consume-limit $(ORCHESTRATOR_CONSUME_LIMIT) --poll-interval-ms $(ORCHESTRATOR_POLL_INTERVAL_MS) & \
	orchestrator_pid=$$!; \
	sleep $(STARTUP_DELAY_SECONDS); \
	cargo run -p expressways-orchestrator -- --transport tcp --address $(BROKER_ADDRESS) serve-dashboard --token-file $(TOKEN_FILE) --state-path $(STATE_PATH) --listen $(DASHBOARD_LISTEN) --task-events-topic $(TASK_EVENTS_TOPIC)

run-ollama-stack: prepare-local-dirs
	@set -eu; \
	server_pid=""; \
	orchestrator_pid=""; \
	agent_pid=""; \
	trap 'if [ -n "$$agent_pid" ]; then kill "$$agent_pid" 2>/dev/null || true; fi; if [ -n "$$orchestrator_pid" ]; then kill "$$orchestrator_pid" 2>/dev/null || true; fi; if [ -n "$$server_pid" ]; then kill "$$server_pid" 2>/dev/null || true; fi' INT TERM EXIT; \
	cargo run -p expressways-server -- --config $(EXPRESSWAYS_CONFIG) & \
	server_pid=$$!; \
	sleep $(STARTUP_DELAY_SECONDS); \
	cargo run -p expressways-orchestrator -- --transport tcp --address $(BROKER_ADDRESS) supervise --token-file $(TOKEN_FILE) --state-path $(STATE_PATH) --retry-delay-ms $(ORCHESTRATOR_RETRY_DELAY_MS) --tasks-topic $(TASKS_TOPIC) --task-events-topic $(TASK_EVENTS_TOPIC) --consume-limit $(ORCHESTRATOR_CONSUME_LIMIT) --poll-interval-ms $(ORCHESTRATOR_POLL_INTERVAL_MS) & \
	orchestrator_pid=$$!; \
	sleep $(STARTUP_DELAY_SECONDS); \
	cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address $(BROKER_ADDRESS) create-topic --token-file $(TOKEN_FILE) --topic $(OLLAMA_RESULTS_TOPIC) >/dev/null 2>&1 || true; \
	cargo run -p expressways-client --bin expressways-agent-ollama -- --transport tcp --address $(BROKER_ADDRESS) --token-file $(TOKEN_FILE) --agent-id $(OLLAMA_AGENT_ID) --default-model $(OLLAMA_MODEL) --ollama-url $(OLLAMA_URL) --task-events-topic $(TASK_EVENTS_TOPIC) --results-topic $(OLLAMA_RESULTS_TOPIC) & \
	agent_pid=$$!; \
	sleep $(STARTUP_DELAY_SECONDS); \
	cd apps/expressways-gateway && npm install >/dev/null && PORT=$(GATEWAY_PORT) EXPRESSWAYS_TRANSPORT=tcp EXPRESSWAYS_ADDRESS=$(BROKER_ADDRESS) EXPRESSWAYS_TOKEN_FILE=$(TOKEN_FILE) EXPRESSWAYS_TASK_EVENTS_TOPIC=$(TASK_EVENTS_TOPIC) EXPRESSWAYS_RESULTS_TOPIC=$(OLLAMA_RESULTS_TOPIC) npm start

benchmark: prepare-local-dirs
	@echo "Running benchmarks..."
	cargo run -p expressways-bench -- suite \
		--spawn-server \
		--broker-iterations 100 \
		--warmup-iterations 20 \
		--payload-bytes 512 \
		--message-count 20000 \
		--read-batch 250 \
		--output ./var/benchmarks/latest.json

generate-token:
	@echo "Generating a new token..."
	@mkdir -p ./tmp ./var/auth ./var/agent ./var/benchmarks ./var/orchestrator
	@if [ ! -f ./var/auth/issuer.private ] || [ ! -f ./var/auth/issuer.public ]; then \
		echo "Issuer keypair missing. Generating local dev keypair..."; \
		cargo run -p expressways-client --bin expresswaysctl -- generate-keypair --key-id dev --private-key ./var/auth/issuer.private --public-key ./var/auth/issuer.public; \
	fi
	cargo run -p expressways-client --bin expresswaysctl -- issue-token --key-id dev --private-key ./var/auth/issuer.private --principal local:developer --audience expressways --scope system:broker:health --scope 'system:broker:admin' --scope 'topic:*:admin,publish,consume' --scope 'artifact:*:publish,consume,admin' --scope 'registry:agents*:admin' --output ./var/auth/developer.token
