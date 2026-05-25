.PHONY: run build test bench up up-rebuild down clean

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

clean:
	rm -rf bin/
