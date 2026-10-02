.PHONY: backup-runtime benchmark bootstrap-local export-support-bundle generate-admin-token generate-token help prepare-local-dirs rehearse-clean-machine rehearse-config-rollback-reliability rehearse-dr-restore-clean-env rehearse-key-rotation rehearse-m3-live-suite rehearse-reliability-denials rehearse-rollback restore-runtime run-expressways run-orchestrator run-dashboard run-stack run-ollama-agent run-gateway run-ollama-stack summarize-pilot-runs summarize-rollback-reliability-trend validate-support-bundle verify-first-run

EXPRESSWAYS_CONFIG ?= configs/expressways.example.toml
BROKER_ADDRESS ?= 127.0.0.1:7766
TOKEN_FILE ?= ./var/auth/developer.token
ADMIN_CONFIG ?= $(EXPRESSWAYS_CONFIG)
ADMIN_TOKEN_FILE ?= ./var/auth/admin.token
ADMIN_KEY_ID ?= dev
ADMIN_PRINCIPAL ?= local:developer
BOOTSTRAP_VERIFY_TOPIC ?= bootstrap.verify
BOOTSTRAP_VERIFY_PAYLOAD ?= bootstrap-ok
SUPPORT_BUNDLE_OUTPUT ?= ./var/agent/support-bundle.json
SUPPORT_BUNDLE_COVERAGE_OUTPUT ?= ./var/agent/support-bundle-coverage.json
BACKUP_OUTPUT_DIR ?= ./var/agent/backups
BACKUP_SIGNING_PRIVATE_KEY ?= ./var/auth/issuer.private
BACKUP_VERIFICATION_PUBLIC_KEY ?= ./var/auth/issuer.public
RESTORE_BACKUP_DIR ?=
STATE_PATH ?= ./var/orchestrator/state.json
CONFIG_AUDIT_LOG ?= ./var/agent/config-audit/entries.jsonl
TASKS_TOPIC ?= tasks
TASK_EVENTS_TOPIC ?= task_events
DASHBOARD_LISTEN ?= 127.0.0.1:8787
DASHBOARD_ACCESS_ARGS ?=
STARTUP_DELAY_SECONDS ?= 1
OLLAMA_AGENT_ID ?= ollama-chat-agent
OLLAMA_MODEL ?= llama3.2
OLLAMA_URL ?= http://127.0.0.1:11434
OLLAMA_RESULTS_TOPIC ?= ollama_results
GATEWAY_PORT ?= 8899
ORCHESTRATOR_RETRY_DELAY_MS ?= 500
ORCHESTRATOR_CONSUME_LIMIT ?= 25
ORCHESTRATOR_POLL_INTERVAL_MS ?= 1000
VERIFY_RETRY_ATTEMPTS ?= 20
VERIFY_RETRY_DELAY_SECONDS ?= 1

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
	@echo "  make bootstrap-local   Generate local issuer and guarded admin token"
	@echo "  make generate-admin-token Generate a local admin-scope capability token (default principal: $(ADMIN_PRINCIPAL))"
	@echo "  make verify-first-run  Verify health, metrics, publish, and consume using admin token"
	@echo "  make export-support-bundle Export config/audit/config-audit/log diagnostics and broker snapshot JSON bundle"
	@echo "  make validate-support-bundle Validate top-10 incident diagnostic coverage for a support bundle"
	@echo "  make backup-runtime    Capture runtime backup bundle (config + state) under $(BACKUP_OUTPUT_DIR)"
	@echo "  make restore-runtime   Restore runtime state from RESTORE_BACKUP_DIR (must be set)"
	@echo "  make rehearse-clean-machine Run timed clean-machine bootstrap rehearsal and write evidence report"
	@echo "  make rehearse-rollback Run failed-upgrade rollback rehearsal and write pass/fail report"
	@echo "  make rehearse-config-rollback-reliability Run config-console rollback reliability rehearsal and emit trend report"
	@echo "  make rehearse-reliability-denials Run degraded/storage-pressure/auth-policy denial rehearsal coverage suite"
	@echo "  make rehearse-dr-restore-clean-env Run isolated backup/restore rehearsal and verify broker health after restore"
	@echo "  make rehearse-key-rotation Run isolated issuer key-rotation rehearsal and verify overlap/cutover/revocation behavior"
	@echo "  make rehearse-m3-live-suite Run clean-machine + rollback + denial rehearsals and validate support-bundle coverage"
	@echo "  make summarize-rollback-reliability-trend Build rolling trend summary for config rollback reliability reports"
	@echo "  make summarize-pilot-runs Build duration/pass-rate summary from pilot rehearsal reports"
	@echo "  make generate-token    Alias for generate-admin-token"
	@echo "  make benchmark         Run the benchmark suite"

prepare-local-dirs:
	@mkdir -p ./tmp ./var/auth ./var/agent ./var/benchmarks ./var/orchestrator

run-expressways: prepare-local-dirs
	cargo run -p expressways-server -- --config $(EXPRESSWAYS_CONFIG)

run-orchestrator: prepare-local-dirs
	cargo run -p expressways-orchestrator -- --transport tcp --address $(BROKER_ADDRESS) supervise --token-file $(TOKEN_FILE) --state-path $(STATE_PATH) --retry-delay-ms $(ORCHESTRATOR_RETRY_DELAY_MS) --tasks-topic $(TASKS_TOPIC) --task-events-topic $(TASK_EVENTS_TOPIC) --consume-limit $(ORCHESTRATOR_CONSUME_LIMIT) --poll-interval-ms $(ORCHESTRATOR_POLL_INTERVAL_MS)

run-dashboard: prepare-local-dirs
	cargo run -p expressways-orchestrator -- --transport tcp --address $(BROKER_ADDRESS) serve-dashboard --token-file $(TOKEN_FILE) --state-path $(STATE_PATH) --listen $(DASHBOARD_LISTEN) --task-events-topic $(TASK_EVENTS_TOPIC) $(DASHBOARD_ACCESS_ARGS)

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
	cargo run -p expressways-orchestrator -- --transport tcp --address $(BROKER_ADDRESS) serve-dashboard --token-file $(TOKEN_FILE) --state-path $(STATE_PATH) --listen $(DASHBOARD_LISTEN) --task-events-topic $(TASK_EVENTS_TOPIC) $(DASHBOARD_ACCESS_ARGS)

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
	cargo build --release -p expressways-server
	cargo run --release -p expressways-bench -- suite \
		--spawn-server \
		--server-bin target/release/expressways-server \
		--broker-iterations 100 \
		--warmup-iterations 20 \
		--payload-bytes 512 \
		--message-count 20000 \
		--read-batch 250 \
		--output ./var/benchmarks/latest.json

bootstrap-local: generate-admin-token
	@echo "Bootstrap complete."
	@echo "Admin token: $(ADMIN_TOKEN_FILE)"
	@echo "Next: start broker with 'make run-expressways', then run 'make verify-first-run'."

generate-admin-token:
	@echo "Generating a new admin token..."
	@mkdir -p ./tmp ./var/auth ./var/agent ./var/benchmarks ./var/orchestrator
	@if [ ! -f ./var/auth/issuer.private ] || [ ! -f ./var/auth/issuer.public ]; then \
		echo "Issuer keypair missing. Generating local dev keypair..."; \
		cargo run -p expressways-client --bin expresswaysctl -- generate-keypair --key-id $(ADMIN_KEY_ID) --private-key ./var/auth/issuer.private --public-key ./var/auth/issuer.public; \
	fi
	@echo "Validating principal $(ADMIN_PRINCIPAL) against $(ADMIN_CONFIG)..."
	@cargo run -p expressways-client --bin expresswaysctl -- validate-principal --config $(ADMIN_CONFIG) --principal $(ADMIN_PRINCIPAL) --key-id $(ADMIN_KEY_ID) >/dev/null
	cargo run -p expressways-client --bin expresswaysctl -- issue-token --key-id $(ADMIN_KEY_ID) --private-key ./var/auth/issuer.private --principal $(ADMIN_PRINCIPAL) --audience expressways --scope system:broker:health --scope 'system:broker:admin' --scope 'topic:*:admin,publish,consume' --scope 'artifact:*:publish,consume,admin' --scope 'registry:agents*:admin' --output $(ADMIN_TOKEN_FILE)

verify-first-run:
	@if [ ! -f $(ADMIN_TOKEN_FILE) ]; then \
		echo "Missing admin token at $(ADMIN_TOKEN_FILE). Run 'make generate-admin-token' first."; \
		exit 1; \
	fi
	@echo "Running first-run verification against $(BROKER_ADDRESS)..."
	@attempt=1; \
	while true; do \
		if cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address $(BROKER_ADDRESS) health --token-file $(ADMIN_TOKEN_FILE) >/dev/null 2>&1; then \
			break; \
		fi; \
		if [ "$$attempt" -ge "$(VERIFY_RETRY_ATTEMPTS)" ]; then \
			echo "Broker health check failed after $(VERIFY_RETRY_ATTEMPTS) attempts."; \
			cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address $(BROKER_ADDRESS) health --token-file $(ADMIN_TOKEN_FILE); \
			exit 1; \
		fi; \
		echo "Health probe attempt $$attempt/$(VERIFY_RETRY_ATTEMPTS) failed; retrying in $(VERIFY_RETRY_DELAY_SECONDS)s..."; \
		sleep $(VERIFY_RETRY_DELAY_SECONDS); \
		attempt=$$((attempt + 1)); \
	done
	cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address $(BROKER_ADDRESS) health --token-file $(ADMIN_TOKEN_FILE)
	cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address $(BROKER_ADDRESS) metrics --token-file $(ADMIN_TOKEN_FILE)
	cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address $(BROKER_ADDRESS) create-topic --token-file $(ADMIN_TOKEN_FILE) --topic $(BOOTSTRAP_VERIFY_TOPIC) >/dev/null 2>&1 || true
	cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address $(BROKER_ADDRESS) publish --token-file $(ADMIN_TOKEN_FILE) --topic $(BOOTSTRAP_VERIFY_TOPIC) --payload $(BOOTSTRAP_VERIFY_PAYLOAD) --classification internal
	cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address $(BROKER_ADDRESS) consume --token-file $(ADMIN_TOKEN_FILE) --topic $(BOOTSTRAP_VERIFY_TOPIC) --offset 0 --limit 1

export-support-bundle:
	cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address $(BROKER_ADDRESS) export-support-bundle --token-file $(ADMIN_TOKEN_FILE) --config $(EXPRESSWAYS_CONFIG) --audit-log ./var/audit/audit.jsonl --config-audit-log ./var/agent/config-audit/entries.jsonl --logs-dir ./var/agent/service-control/logs --output $(SUPPORT_BUNDLE_OUTPUT)

validate-support-bundle:
	cargo run -p expressways-client --bin expresswaysctl -- validate-support-bundle --bundle $(SUPPORT_BUNDLE_OUTPUT) --output $(SUPPORT_BUNDLE_COVERAGE_OUTPUT)

backup-runtime:
	cargo run -p expressways-client --bin expresswaysctl -- backup-runtime --config $(EXPRESSWAYS_CONFIG) --output-dir $(BACKUP_OUTPUT_DIR) --config-audit-log $(CONFIG_AUDIT_LOG) --orchestrator-state $(STATE_PATH) --signing-private-key $(BACKUP_SIGNING_PRIVATE_KEY) --signing-key-id $(ADMIN_KEY_ID)

restore-runtime:
	@if [ -z "$(RESTORE_BACKUP_DIR)" ]; then \
		echo "RESTORE_BACKUP_DIR is required. Example: make restore-runtime RESTORE_BACKUP_DIR=./var/agent/backups/expressways-backup-<timestamp>"; \
		exit 1; \
	fi
	cargo run -p expressways-client --bin expresswaysctl -- restore-runtime --backup-dir $(RESTORE_BACKUP_DIR) --config $(EXPRESSWAYS_CONFIG) --config-audit-log $(CONFIG_AUDIT_LOG) --orchestrator-state $(STATE_PATH) --verification-public-key $(BACKUP_VERIFICATION_PUBLIC_KEY) --overwrite

rehearse-clean-machine:
	bash scripts/rehearsal-clean-machine.sh

rehearse-rollback:
	bash scripts/rehearsal-rollback.sh

rehearse-config-rollback-reliability:
	bash scripts/rehearsal-config-rollback-reliability.sh

rehearse-reliability-denials:
	bash scripts/rehearsal-reliability-denials.sh

rehearse-dr-restore-clean-env:
	bash scripts/rehearsal-dr-restore-clean-env.sh

rehearse-key-rotation:
	bash scripts/rehearsal-key-rotation.sh

rehearse-m3-live-suite:
	bash scripts/rehearsal-m3-live-suite.sh

summarize-pilot-runs:
	bash scripts/pilot-run-duration-summary.sh

summarize-rollback-reliability-trend:
	bash scripts/rollback-reliability-trend-summary.sh

generate-token: generate-admin-token
