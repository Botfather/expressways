.PHONY: benchmark

benchmark:
	@echo "Running benchmarks..."
	cargo run -p expressways-bench -- suite \
		--spawn-server \
		--broker-iterations 100 \
		--warmup-iterations 20 \
		--payload-bytes 512 \
		--message-count 2000 \
		--read-batch 250 \
		--output ./var/benchmarks/latest.json

