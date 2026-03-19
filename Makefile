.PHONY: benchmark help prepare-local-dirs run-expressways run-orchestrator run-dashboard run-stack

EXPRESSWAYS_CONFIG ?= configs/expressways.example.toml
BROKER_ADDRESS ?= 127.0.0.1:7766
TOKEN_FILE ?= ./var/auth/developer.token
STATE_PATH ?= ./var/orchestrator/state.json
TASKS_TOPIC ?= tasks
TASK_EVENTS_TOPIC ?= task_events
DASHBOARD_LISTEN ?= 127.0.0.1:8787
STARTUP_DELAY_SECONDS ?= 1

help:
	@echo "Available targets:"
	@echo "  make help              Show this command summary"
	@echo "  make run-expressways   Start the local Expressways broker"
	@echo "  make run-orchestrator  Start the local task supervisor"
	@echo "  make run-dashboard     Start the local dashboard server"
	@echo "  make run-stack         Start broker, supervisor, and dashboard together"
	@echo "  make benchmark         Run the benchmark suite"

prepare-local-dirs:
	@mkdir -p ./tmp ./var/auth ./var/agent ./var/benchmarks ./var/orchestrator

run-expressways: prepare-local-dirs
	cargo run -p expressways-server -- --config $(EXPRESSWAYS_CONFIG)

run-orchestrator: prepare-local-dirs
	cargo run -p expressways-orchestrator -- --transport tcp --address $(BROKER_ADDRESS) supervise --token-file $(TOKEN_FILE) --state-path $(STATE_PATH) --tasks-topic $(TASKS_TOPIC) --task-events-topic $(TASK_EVENTS_TOPIC)

run-dashboard: prepare-local-dirs
	cargo run -p expressways-orchestrator -- --transport tcp --address $(BROKER_ADDRESS) serve-dashboard --token-file $(TOKEN_FILE) --state-path $(STATE_PATH) --listen $(DASHBOARD_LISTEN) --task-events-topic $(TASK_EVENTS_TOPIC)

run-stack: prepare-local-dirs
	@set -eu; \
	server_pid=""; \
	orchestrator_pid=""; \
	trap 'if [ -n "$$orchestrator_pid" ]; then kill "$$orchestrator_pid" 2>/dev/null || true; fi; if [ -n "$$server_pid" ]; then kill "$$server_pid" 2>/dev/null || true; fi' INT TERM EXIT; \
	cargo run -p expressways-server -- --config $(EXPRESSWAYS_CONFIG) & \
	server_pid=$$!; \
	sleep $(STARTUP_DELAY_SECONDS); \
	cargo run -p expressways-orchestrator -- --transport tcp --address $(BROKER_ADDRESS) supervise --token-file $(TOKEN_FILE) --state-path $(STATE_PATH) --tasks-topic $(TASKS_TOPIC) --task-events-topic $(TASK_EVENTS_TOPIC) & \
	orchestrator_pid=$$!; \
	sleep $(STARTUP_DELAY_SECONDS); \
	cargo run -p expressways-orchestrator -- --transport tcp --address $(BROKER_ADDRESS) serve-dashboard --token-file $(TOKEN_FILE) --state-path $(STATE_PATH) --listen $(DASHBOARD_LISTEN) --task-events-topic $(TASK_EVENTS_TOPIC)

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
	cargo run -p expressways-client --bin expresswaysctl -- issue-token --key-id dev --private-key ./var/auth/issuer.private --principal local:developer --audience expressways --scope system:broker:health --scope 'system:broker:admin' --scope 'topic:*:admin,publish,consume' --scope 'registry:agents*:admin' --output ./var/auth/developer.token
