SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: setup up down logs ps restart clean \
        lint test unit-test integration-test contract-test e2e-test build-tests-image \
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

TESTS_IMAGE := indusense-tests
NETWORK := indusense_indusense-net
TEST_ENV := -e API_BASE_URL=http://api:8080 -e REDIS_HOST=redis -e REDIS_PORT=6379 \
            -e ALERT_POSTGRES_DSN="postgres://indusense:indusense_dev_password@postgres:5432/indusense?sslmode=disable" \
            -e SIM_MQTT_BROKER_URL=tcp://mosquitto:1883

## fmt/vet: no-ops kept for muscle memory -- there's no Go left to format
# or vet. Python-side static checks (ruff/mypy) aren't wired up yet; add
# them here when they are.
fmt:
	@echo "no Go left to format (see build-tests-image for the Python test image instead)"

vet:
	@echo "no Go left to vet (see build-tests-image for the Python test image instead)"

## lint: fmt + vet (currently no-ops, see above)
lint: fmt vet

## build-tests-image: build the shared Python test image (every service's
## combined dependencies + pytest) used by every target below
build-tests-image:
	docker build -q -t $(TESTS_IMAGE) -f tests/Dockerfile .

## unit-test: run each Python service's own pytest suite (services/*/tests,
## shared/tests, simulator/tests, scripts/seed/tests), no external
## services required. Run one at a time, not combined into a single pytest
## invocation: several services declare same-named local modules
## (config.py, main.py, ...) that would collide in one process's module
## cache if imported together -- separate invocations are what keeps each
## service's tests isolated to its own modules, the way separate Go
## packages were.
unit-test: build-tests-image
	@for dir in shared services/ingestion services/stream-processor services/anomaly-detector services/alert-service services/api simulator scripts/seed; do \
		echo "=== $$dir ==="; \
		docker run --rm --entrypoint python3 $(TESTS_IMAGE) -m pytest $$dir/tests -v || exit 1; \
	done

## test: alias for unit-test
test: unit-test

## integration-test: run integration tests against the real running stack
integration-test: build-tests-image
	docker run --rm --network $(NETWORK) --entrypoint python3 $(TEST_ENV) $(TESTS_IMAGE) -m pytest tests/integration -v

## contract-test: run schema/contract tests between services (no live
## stack needed -- pure wire-format checks against shared.events)
contract-test: build-tests-image
	docker run --rm --entrypoint python3 $(TESTS_IMAGE) -m pytest tests/contract -v

## e2e-test: run the full end-to-end pipeline test against the real running stack
e2e-test: build-tests-image
	docker run --rm --network $(NETWORK) --entrypoint python3 $(TEST_ENV) $(TESTS_IMAGE) -m pytest tests/e2e -v

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
