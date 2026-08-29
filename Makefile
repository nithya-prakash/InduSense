SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: setup up down logs ps restart clean \
        lint test unit-test integration-test contract-test e2e-test \
        seed simulate simulate-docker load-test demo fmt vet migrate-up migrate-down

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

INFRA_SERVICES := postgres redis mosquitto kafka kafka-init-topics kafka-ui influxdb prometheus grafana jaeger

## up: start infra, apply migrations, then start the application services
#
# Two-phase on purpose, not one `docker compose up -d --build`: none of
# the app services have a restart policy, and anomaly-detector/
# alert-service both run an eager Postgres query at startup (not a lazy
# connection). Against a genuinely fresh, empty database, both die on
# their very first attempt, before migrate-up has had a chance to create
# any tables — and because nothing restarts them, `docker compose up`
# alone leaves them Exited forever, not crash-looping-then-recovering the
# way Kubernetes would. Bringing up infra first, migrating, then bringing
# up the rest means the app services' first-ever query finds real tables.
up:
	$(COMPOSE) up -d --build --wait --wait-timeout 180 $(INFRA_SERVICES)
	$(MAKE) migrate-up
	$(COMPOSE) up -d --build --wait --wait-timeout 180
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
#
# Restarts anomaly-detector and alert-service afterward: both load a
# Postgres-derived cache once at startup (anomaly-detector's device/sensor
# catalog, alert-service's rule cache) and only refresh it periodically —
# every 300s and 60s by default. Against a freshly-seeded database, both
# services started with an empty cache, so newly-seeded devices/rules
# would otherwise be invisible to rule-based anomaly detection for up to
# 5 minutes. A restart reloads the cache immediately instead of waiting.
#
# The brief sleep afterward is a pragmatic, honest tradeoff, not a real
# guarantee: neither service's /ready endpoint checks Kafka consumer-group
# state (it never needed to before this), so there's no signal to poll for
# "the group has finished rebalancing" — this was discovered by a CI run
# that restarted both services and then immediately published test
# traffic, landing inside that rebalance window and timing out even
# though the pipeline was working correctly, just not fully stood up yet.
seed:
	$(COMPOSE) --profile seed up --build seed
	$(COMPOSE) restart anomaly-detector alert-service
	sleep 5

## simulate: run the sensor simulator as a container against the compose network
#
# Always container-based, even for "local" use: the simulator depends on
# psycopg/paho-mqtt, which the host's local Python toolchain can't
# reliably install (see the Python-rewrite build notes). There is no
# bare-host equivalent anymore the way `go run ./simulator` was.
simulate:
	$(COMPOSE) --profile simulate up --build simulator

## simulate-docker: alias for `simulate` (kept for muscle memory / old docs)
simulate-docker: simulate

## load-test: run all k6 load tests against the running stack (requires `make seed`)
load-test:
	k6 run load-tests/dashboard-read-load.js
	k6 run load-tests/auth-rate-limit.js
	k6 run load-tests/websocket-scale.js

## demo: full scripted demo (infra -> migrate -> seed -> simulate -> dashboard)
demo:
	@echo "NOT YET IMPLEMENTED — assembled once all phases are complete"
