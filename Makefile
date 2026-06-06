.PHONY: help build run dev test vet tidy docker docker-up docker-down docker-logs

help:
	@echo "Targets:"
	@echo "  build       - go build ./..."
	@echo "  run         - run the server (expects env loaded; see .env.example)"
	@echo "  dev         - air-driven live reload"
	@echo "  test        - go test ./..."
	@echo "  vet         - go vet ./..."
	@echo "  tidy        - go mod tidy"
	@echo "  docker-up   - docker compose up -d (foreman + postgres)"
	@echo "  docker-down - docker compose down"
	@echo "  docker-logs - docker compose logs -f foreman"

build:
	go build ./...

run:
	go run .

dev:
	air -c .air.toml

test:
	go test ./...

vet:
	go vet ./...

tidy:
	go mod tidy

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f foreman
