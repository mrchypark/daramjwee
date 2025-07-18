.PHONY: test coverage coverage-text bench


# Generate and view coverage report
coverage:
	@echo "Generating coverage profile..."
	@go test -coverprofile=coverage.out -covermode=atomic ./...
	@echo "Opening HTML coverage report..."
	@go tool cover -html=coverage.out

# Output function-level coverage to terminal
coverage-text:
	@echo "Generating coverage profile..."
	@go test -coverprofile=coverage.out -covermode=atomic ./...
	@echo "\nFunction coverage:"
	@go tool cover -func=coverage.out

bench:
	@echo "Running benchmarks..."
	@go test -bench=BenchmarkMemStore -benchmem -benchtime=500ms -timeout=5m ./pkg/store/memstore > bench_results.txt 2>&1 || true
	@go test -bench=BenchmarkFileStore -benchmem -benchtime=500ms -timeout=5m ./pkg/store/filestore >> bench_results.txt 2>&1 || true
	@go test -bench=BenchmarkLRU -benchmem -benchtime=500ms -timeout=5m ./pkg/policy >> bench_results.txt 2>&1 || true
	@echo "Benchmarks completed - results saved to bench_results.txt"

# Run N-tier specific benchmarks
bench-ntier:
	@echo "Running N-tier performance benchmarks..."
	@go test -bench=BenchmarkNTierVs2Tier -benchmem -benchtime=1s -timeout=10m . > ntier_bench_results.txt 2>&1 || true
	@go test -bench=BenchmarkTierPromotion -benchmem -benchtime=1s -timeout=10m . >> ntier_bench_results.txt 2>&1 || true
	@go test -bench=BenchmarkNTierConcurrentAccess -benchmem -benchtime=1s -timeout=10m . >> ntier_bench_results.txt 2>&1 || true
	@go test -bench=BenchmarkMemoryAllocation -benchmem -benchtime=1s -timeout=10m . >> ntier_bench_results.txt 2>&1 || true
	@go test -bench=BenchmarkSetOperations -benchmem -benchtime=1s -timeout=10m . >> ntier_bench_results.txt 2>&1 || true
	@go test -bench=BenchmarkDeleteOperations -benchmem -benchtime=1s -timeout=10m . >> ntier_bench_results.txt 2>&1 || true
	@go test -bench=BenchmarkGCPressure -benchmem -benchtime=1s -timeout=10m . >> ntier_bench_results.txt 2>&1 || true
	@go test -bench=BenchmarkThroughput -benchmem -benchtime=2s -timeout=10m . >> ntier_bench_results.txt 2>&1 || true
	@go test -bench=BenchmarkScalability -benchmem -benchtime=1s -timeout=10m . >> ntier_bench_results.txt 2>&1 || true
	@echo "N-tier benchmarks completed - results saved to ntier_bench_results.txt"

# Run all benchmarks including N-tier
bench-all: bench bench-ntier
	@echo "All benchmarks completed"