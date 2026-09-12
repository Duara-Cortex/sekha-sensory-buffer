.PHONY: all build build-arm64 test clean run bench

BINARY_NAME=sekha-sensory-buffer
BENCHMARK_NAME=sekha-benchmark
NODE3_HOST=192.168.8.183
NODE3_USER=admin

all: build

build:
	@mkdir -p bin
	go build -ldflags="-s -w" -o bin/$(BINARY_NAME) ./cmd/server
	go build -ldflags="-s -w" -o bin/$(BENCHMARK_NAME) ./cmd/benchmark
	@echo "Build complete: bin/$(BINARY_NAME) and bin/$(BENCHMARK_NAME)"

build-arm64:
	@mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o bin/$(BINARY_NAME)-linux-arm64 ./cmd/server
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o bin/$(BENCHMARK_NAME)-linux-arm64 ./cmd/benchmark
	@echo "Cross-compiled ARM64 binaries in bin/"

test:
	go test -v -race ./...

bench:
	./bin/$(BENCHMARK_NAME) -url http://127.0.0.1:8081/api/v1/sensory/ingest -stats-url http://127.0.0.1:8081/api/v1/sensory/stats -lines-per-sec 10000 -duration 10s -batch-size 50

run:
	go run ./cmd/server -port 8081 -capacity-mb 64

clean:
	rm -rf bin/
