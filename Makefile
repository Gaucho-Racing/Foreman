.PHONY: help build run dev test vet tidy docker-up docker-down docker-logs release

help:
	@echo "Targets:"
	@echo "  build       - go build ./..."
	@echo "  run         - run the server (expects env loaded; see .env.example)"
	@echo "  dev         - air-driven live reload"
	@echo "  test        - go test ./... (needs Docker for testcontainers)"
	@echo "  vet         - go vet ./..."
	@echo "  tidy        - go mod tidy"
	@echo "  docker-up   - docker compose up -d (foreman + postgres)"
	@echo "  docker-down - docker compose down"
	@echo "  docker-logs - docker compose logs -f foreman"
	@echo "  release V=  - cut release v\$$V (e.g. make release V=0.2.0)"

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

# `make release V=0.2.0` — non-interactive. Omit V to be prompted.
release:
	./scripts/release.sh $(V)
