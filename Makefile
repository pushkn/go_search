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

clean:
rm -rf bin/
