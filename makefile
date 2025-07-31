# Makefile for daramjwee project

.PHONY: test test-local test-ci test-race test-coverage test-bench clean help

# Default target
help:
	@echo "Available targets:"
	@echo "  test-local    - Run local tests (full test suite)"
	@echo "  test-ci       - Run CI tests (optimized for CI environment)"
	@echo "  test-race     - Run tests with race detection"
	@echo "  test-coverage - Run tests with coverage report"
	@echo "  test-bench    - Run benchmark tests"
	@echo "  test-short    - Run short tests only"
	@echo "  clean         - Clean test cache and build artifacts"

# Local testing (full test suite)
test-local:
	@echo "Running local tests (full test suite)..."
	go test -v -timeout 120s ./...

# CI testing (optimized for CI environment)
test-ci:
	@echo "Running CI tests (optimized for CI environment)..."
	go test -v -tags=ci -timeout 60s ./...

# Race condition testing
test-race:
	@echo "Running tests with race detection..."
	go test -race -v -timeout 120s ./...

# Race condition testing for CI
test-race-ci:
	@echo "Running CI tests with race detection..."
	go test -race -v -tags=ci -timeout 60s ./...

# Coverage testing
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -coverprofile=coverage.out -timeout 120s ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Coverage testing for CI
test-coverage-ci:
	@echo "Running CI tests with coverage..."
	go test -v -tags=ci -coverprofile=coverage.out -timeout 60s ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Benchmark testing
test-bench:
	@echo "Running benchmark tests..."
	go test -bench=. -benchmem -timeout 300s ./...

# Short tests only
test-short:
	@echo "Running short tests..."
	go test -short -v -timeout 30s ./...

# Memory efficiency tests only (local)
test-memory:
	@echo "Running memory efficiency tests (local)..."
	go test -v -run "TestMemory" -timeout 60s

# Memory efficiency tests only (CI)
test-memory-ci:
	@echo "Running memory efficiency tests (CI)..."
	go test -v -tags=ci -run "TestMemory" -timeout 30s

# Adaptive buffer pool tests only (local)
test-adaptive:
	@echo "Running adaptive buffer pool tests (local)..."
	go test -v -run "TestAdaptiveBufferPool" -timeout 60s

# Adaptive buffer pool tests only (CI)
test-adaptive-ci:
	@echo "Running adaptive buffer pool tests (CI)..."
	go test -v -tags=ci -run "TestAdaptiveBufferPool" -timeout 30s

# Performance tests only (local)
test-performance:
	@echo "Running performance tests (local)..."
	go test -v -run "TestPerformance" -timeout 120s

# Performance tests only (CI)
test-performance-ci:
	@echo "Running performance tests (CI)..."
	go test -v -tags=ci -run "TestPerformance" -timeout 60s

# Clean up
clean:
	@echo "Cleaning up..."
	go clean -testcache
	go clean -cache
	rm -f coverage.out coverage.html
	rm -f bench_results.txt

# Continuous integration target
ci: test-race-ci test-coverage-ci
	@echo "CI tests completed successfully"

# Development target
dev: test-local test-race
	@echo "Development tests completed successfully"

# Quick test for development
quick:
	@echo "Running quick tests..."
	go test -short -v -timeout 15s ./...

# Test specific package
test-pkg:
	@if [ -z "$(PKG)" ]; then echo "Usage: make test-pkg PKG=<package>"; exit 1; fi
	@echo "Testing package: $(PKG)"
	go test -v -timeout 60s ./$(PKG)

# Test with specific pattern
test-pattern:
	@if [ -z "$(PATTERN)" ]; then echo "Usage: make test-pattern PATTERN=<pattern>"; exit 1; fi
	@echo "Testing with pattern: $(PATTERN)"
	go test -v -run "$(PATTERN)" -timeout 60s ./...

# Test with specific pattern for CI
test-pattern-ci:
	@if [ -z "$(PATTERN)" ]; then echo "Usage: make test-pattern-ci PATTERN=<pattern>"; exit 1; fi
	@echo "Testing with pattern (CI): $(PATTERN)"
	go test -v -tags=ci -run "$(PATTERN)" -timeout 30s ./...

# Verbose test output
test-verbose:
	@echo "Running tests with verbose output..."
	go test -v -x -timeout 120s ./...

# Test with CPU profiling
test-profile:
	@echo "Running tests with CPU profiling..."
	go test -v -cpuprofile=cpu.prof -timeout 120s ./...
	@echo "CPU profile generated: cpu.prof"

# Test with memory profiling
test-memprofile:
	@echo "Running tests with memory profiling..."
	go test -v -memprofile=mem.prof -timeout 120s ./...
	@echo "Memory profile generated: mem.prof"

# Show test coverage in terminal
coverage-text:
	@echo "Running tests with text coverage..."
	go test -v -coverprofile=coverage.out -timeout 120s ./...
	go tool cover -func=coverage.out

# Show test coverage in terminal for CI
coverage-text-ci:
	@echo "Running CI tests with text coverage..."
	go test -v -tags=ci -coverprofile=coverage.out -timeout 60s ./...
	go tool cover -func=coverage.out