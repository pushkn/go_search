.PHONY: run build test bench up up-rebuild down clean load load-attack bench-baseline bench-stress bench-attack bench-report

run:
	go run ./cmd/server

build:
	go build -o bin/server ./cmd/server

test:
	go test ./... -v -race

bench:
	go test ./... -bench=. -benchmem -run=^$$

up:
	docker compose -f deployments/docker-compose.yml up -d

up-rebuild:
	docker compose -f deployments/docker-compose.yml up -d --build

down:
	docker compose -f deployments/docker-compose.yml down

load:
	go run ./scripts/load_producer -rps=1000 -duration=30s

load-attack:
	go run ./scripts/load_producer -rps=2000 -duration=60s -bot-share=0.3

bench-baseline:
	@mkdir -p docs/benchmarks
	vegeta attack -keepalive -format=http -targets=scripts/vegeta/targets.http -rate=5000 -duration=30s | \
		tee docs/benchmarks/baseline-5k.bin | vegeta report
	vegeta report -type=hist[0,1ms,5ms,10ms,50ms,100ms,500ms] < docs/benchmarks/baseline-5k.bin

bench-10k:
	@mkdir -p docs/benchmarks
	vegeta attack -keepalive -format=http -targets=scripts/vegeta/targets.http -rate=10000 -duration=30s | \
		tee docs/benchmarks/stress-10k.bin | vegeta report
	vegeta report -type=hist[0,1ms,5ms,10ms,50ms,100ms,500ms] < docs/benchmarks/stress-10k.bin

bench-attack:
	@mkdir -p docs/benchmarks
	@echo "starting kafka load with bot share 0.3"
	go run ./scripts/load_producer -rps=2000 -duration=60s -bot-share=0.3 & \
	  LOAD_PID=$$!; \
	  sleep 10; \
	  vegeta attack -keepalive -format=http -targets=scripts/vegeta/targets.http -rate=5000 -duration=30s | \
	    tee docs/benchmarks/attack-5k.bin | vegeta report; \
	  vegeta report -type=hist[0,1ms,5ms,10ms,50ms,100ms,500ms] < docs/benchmarks/attack-5k.bin; \
	  wait $$LOAD_PID

bench-report:
	vegeta plot < docs/benchmarks/baseline-5k.bin > docs/benchmarks/baseline-5k.html
	vegeta plot < docs/benchmarks/stress-10k.bin > docs/benchmarks/stress-10k.html
	vegeta plot < docs/benchmarks/attack-5k.bin > docs/benchmarks/attack-5k.html

clean:
	rm -rf bin/
