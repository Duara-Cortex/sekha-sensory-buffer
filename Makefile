.PHONY: all build build-arm64 test clean run bench

BINARY_NAME=sekha-sensory-buffer
BENCHMARK_NAME=sekha-benchmark
VALIDATE_NAME=sekha-validate
STRESS_NAME=sekha-stress
NODE3_HOST=192.168.8.183
NODE3_USER=admin

all: build

build:
	@mkdir -p bin
	go build -ldflags="-s -w" -o bin/$(BINARY_NAME) ./cmd/server
	go build -ldflags="-s -w" -o bin/$(BENCHMARK_NAME) ./cmd/benchmark
	go build -ldflags="-s -w" -o bin/$(VALIDATE_NAME) ./cmd/validate_classifier
	go build -ldflags="-s -w" -o bin/$(STRESS_NAME) ./cmd/stress_test
	@echo "Build complete: bin/$(BINARY_NAME), bin/$(BENCHMARK_NAME), bin/$(VALIDATE_NAME), and bin/$(STRESS_NAME)"

build-arm64:
	@mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o bin/$(BINARY_NAME)-linux-arm64 ./cmd/server
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o bin/$(BENCHMARK_NAME)-linux-arm64 ./cmd/benchmark
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o bin/$(VALIDATE_NAME)-linux-arm64 ./cmd/validate_classifier
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o bin/$(STRESS_NAME)-linux-arm64 ./cmd/stress_test
	@echo "Cross-compiled ARM64 binaries in bin/"

install: build
	sudo systemctl stop sekha-sensory-buffer.service 2>/dev/null || true
	sudo cp bin/$(BINARY_NAME) /usr/local/bin/
	sudo cp systemd/sekha-sensory-buffer.service /etc/systemd/system/
	sudo systemctl daemon-reload
	sudo systemctl restart sekha-sensory-buffer.service
	@echo "Daemon updated and restarted: sekha-sensory-buffer.service"

test:
	go test -v -race ./...

validate:
	./bin/$(VALIDATE_NAME)

stress:
	./bin/$(STRESS_NAME) -base-url http://127.0.0.1:8081 -stage-duration 15s

bench:
	./bin/$(BENCHMARK_NAME) -url http://127.0.0.1:8081/api/v1/sensory/ingest -stats-url http://127.0.0.1:8081/api/v1/sensory/stats -lines-per-sec 10000 -duration 10s -batch-size 50

run:
	go run ./cmd/server -port 8081 -capacity-mb 64

clean:
	rm -rf bin/
