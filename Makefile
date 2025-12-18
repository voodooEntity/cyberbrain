 ## Convenience targets for running only the scheduler tests

.PHONY: test-scheduler test-scheduler-race test-scheduler-short diagrams

# Run only the tests under src/system/scheduler_test
test-scheduler:
	go test -v ./src/system/scheduler_test

# Same as above, but with the race detector enabled
test-scheduler-race:
	go test -v -race ./src/system/scheduler_test

# Faster run with a default timeout safeguard
test-scheduler-short:
	go test -v -timeout=60s ./src/system/scheduler_test

# Render all diagrams (private repo convenience; requires Docker)
diagrams:
	bash scripts/mermaid_render.sh
