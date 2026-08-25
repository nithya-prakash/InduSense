SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: setup up down logs ps restart clean \
        lint test unit-test integration-test contract-test e2e-test \
        seed simulate load-test demo fmt vet migrate-up migrate-down

MIGRATE_IMAGE := migrate/migrate:v4.17.1
POSTGRES_DSN := postgres://indusense:indusense_dev_password@postgres:5432/indusense?sslmode=disable

## migrate-up: apply all pending Postgres migrations
migrate-up:
	docker run --rm --network indusense_indusense-net \
		-v "$$(pwd)/migrations:/migrations" $(MIGRATE_IMAGE) \
		-path=/migrations -database "$(POSTGRES_DSN)" up

## migrate-down: roll back the most recent migration
migrate-down:
	docker run --rm --network indusense_indusense-net \
		-v "$$(pwd)/migrations:/migrations" $(MIGRATE_IMAGE) \
		-path=/migrations -database "$(POSTGRES_DSN)" down 1

## setup: copy .env.example to .env if missing
setup:
	@test -f .env || cp .env.example .env
	@echo "Environment file ready at .env"

## up: start all infrastructure + application services
up:
	$(COMPOSE) up -d --build
	@echo "Waiting for services to become healthy..."
	@$(COMPOSE) ps

## down: stop and remove all containers (volumes preserved)
down:
	$(COMPOSE) down

## clean: stop containers and remove volumes (DESTRUCTIVE — wipes data)
clean:
	$(COMPOSE) down -v

## restart: down then up
restart: down up

## logs: tail logs for all services
logs:
	$(COMPOSE) logs -f --tail=200

## ps: show container status
ps:
	$(COMPOSE) ps

## fmt: format all Go code
fmt:
	go fmt ./...

## vet: static analysis
vet:
	go vet ./...

## lint: fmt + vet (golangci-lint added in a later phase)
lint: fmt vet

## unit-test: run Go unit tests (no external services required)
unit-test:
	go test ./... -short -race -count=1

## test: alias for unit-test
test: unit-test

## integration-test: run integration tests against real infra (Testcontainers)
integration-test:
	go test ./tests/integration/... -race -count=1

## contract-test: run schema/contract tests between services
contract-test:
	go test ./tests/contract/... -race -count=1

## e2e-test: run the full end-to-end pipeline test
e2e-test:
	go test ./tests/e2e/... -race -count=1 -timeout=5m

## seed: seed organizations/factories/machines/devices/sensors into Postgres
seed:
	SEED_POSTGRES_DSN="postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable" go run ./scripts/seed

## simulate: run the sensor simulator against MQTT
simulate:
	@echo "NOT YET IMPLEMENTED — added in Phase 3 (Sensor Simulation)"

## load-test: run k6 load tests
load-test:
	@echo "NOT YET IMPLEMENTED — added in Phase 15 (Load Testing)"

## demo: full scripted demo (infra -> migrate -> seed -> simulate -> dashboard)
demo:
	@echo "NOT YET IMPLEMENTED — assembled once all phases are complete"
