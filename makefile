.PHONY: test test-unit test-integration test-stress test-bench coverage coverage-text bench bench-all

# Run all tests
test:
	@echo "Running all tests..."
	@go test ./... ./tests/...

# Run unit tests only
test-unit:
	@echo "Running unit tests..."
	@go test ./tests/unit/... ./pkg/... ./internal/...

# Run integration tests
test-integration:
	@echo "Running integration tests..."
	@go test ./tests/integration/...

# Run stress tests with race detection
test-stress:
	@echo "Running stress tests with race detection..."
	@go test -race -timeout=30s ./tests/stress/...

# Run benchmark tests
test-bench:
	@echo "Running benchmark tests..."
	@go test -bench=. ./tests/benchmark/...

# Generate and view coverage report
coverage:
	@echo "Generating coverage profile..."
	@go test -coverprofile=coverage.out -covermode=atomic ./... ./tests/...
	@echo "Opening HTML coverage report..."
	@go tool cover -html=coverage.out

# Output function-level coverage to terminal
coverage-text:
	@echo "Generating coverage profile..."
	@go test -coverprofile=coverage.out -covermode=atomic ./... ./tests/...
	@echo "\nFunction coverage:"
	@go tool cover -func=coverage.out

# Legacy benchmark support (pkg-level benchmarks)
bench:
	@echo "Running pkg-level benchmarks..."
	@go test -bench=BenchmarkMemStore -benchmem -benchtime=500ms -timeout=5m ./pkg/store/memstore > bench_results.txt 2>&1 || true
	@go test -bench=BenchmarkFileStore -benchmem -benchtime=500ms -timeout=5m ./pkg/store/filestore >> bench_results.txt 2>&1 || true
	@go test -bench=BenchmarkLRU -benchmem -benchtime=500ms -timeout=5m ./pkg/policy >> bench_results.txt 2>&1 || true
	@echo "Pkg benchmarks completed - results saved to bench_results.txt"

# Run organized benchmark tests
bench-organized:
	@echo "Running organized benchmark tests..."
	@go test -bench=. -benchmem -benchtime=1s -timeout=10m ./tests/benchmark/... > organized_bench_results.txt 2>&1 || true
	@echo "Organized benchmarks completed - results saved to organized_bench_results.txt"

# Run all benchmarks (legacy + organized)
bench-all: bench bench-organized
	@echo "All benchmarks completed"