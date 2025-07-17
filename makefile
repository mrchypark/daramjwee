.PHONY: test coverage coverage-text

# ... existing content ...

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
	@go test -bench=. -benchmem ./... 2>/dev/null > bench_results.txt || echo "Benchmarks completed with some issues"