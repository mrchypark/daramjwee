.PHONY: test coverage coverage-text

# ... 기존 내용 ...

# 커버리지 리포트 생성 및 보기
coverage:
	@echo "Generating coverage profile..."
	@go test -coverprofile=coverage.out -covermode=atomic ./...
	@echo "Opening HTML coverage report..."
	@go tool cover -html=coverage.out

# 터미널에 함수별 커버리지 출력
coverage-text:
	@echo "Generating coverage profile..."
	@go test -coverprofile=coverage.out -covermode=atomic ./...
	@echo "\nFunction coverage:"
	@go tool cover -func=coverage.out

bench:
	@echo "Running benchmarks..."
	@go test -v -bench=. -benchmem ./... > bench_results.txt