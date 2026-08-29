# 16. CI/CD

[.github/workflows/ci.yml](../../.github/workflows/ci.yml) runs on every
push and pull request to `main`: parallel lint/check jobs (a Python
compile-check — `python -m compileall` across every service, no ruff/mypy
wired up yet; a `pip-audit` job matrixed over every service's
`requirements.txt`; the frontend's `eslint`/`next build`; `helm lint` on the
Kubernetes + Helm chart; `actionlint` on the workflow file itself), then a
`unit-test` job (`make unit-test` — each service's own pytest suite,
per-directory), then a `test` job that spins up the entire real stack via
`make up` and runs `make contract-test && make integration-test &&
make e2e-test` against it — real infra, not a CI-specific mocked
substitute. On a push to `main` only, once every job has passed,
`publish-images` builds and pushes eight Dockerfiles (five services,
migrate, seed, frontend — the simulator is deliberately excluded, since
it's dev/demo tooling never deployed anywhere real) to GHCR, tagged
`latest` and the commit SHA. This is the honest scope for "CD" here: there's
no live remote environment for this portfolio project to actually deploy
the Helm chart into.

This job graph replaced an earlier Go-era version (`gofmt`/`go vet`/`go
build`, a `govulncheck` job, and `go test ./... -race -count=1` as the real
test step) during the [Python rewrite](17-python-rewrite.md) — see that
phase for what changed and why.

**Running the local bring-up sequence for real, from a completely fresh
Postgres, surfaced two genuine bugs** (from the original Go-era build,
still true of the current setup since they're about compose bring-up
order, not language):

1. **`make up` alone couldn't stand up a fresh clone.** `anomaly-detector`
   and `alert-service` both run an eager Postgres query at startup, and
   neither has a restart policy in docker-compose.yml. Against a truly
   empty database, both crashed on their first attempt and, unlike
   Kubernetes' crash-loop-and-retry, just stayed `Exited` forever. `make up`
   now brings up infra first (`--wait`), then runs migrations, then the app
   services — a two-phase bring-up mirroring the real dependency graph
   Compose has no native way to express.
2. **A freshly-seeded database's new devices/rules were invisible to
   alerting for minutes, not seconds** — `anomaly-detector` and
   `alert-service` each cache Postgres-derived state at startup and only
   refresh periodically (every 300s / 60s by default). `make seed` now
   restarts both services afterward to reload their caches immediately,
   and the e2e test's anomaly-to-alert deadline was widened to give both
   services' Kafka consumer groups time to rebalance after the restart.

### Operational hygiene (post-audit fixes)

A pre-GitHub audit found several small but real operational gaps, all
fixed and verified against the live stack: `kafka-ui` pinned to a specific
tag (was `:latest`); every service given an explicit `mem_limit` in
docker-compose.yml (previously none at all); Grafana admin credentials
moved out of hardcoded values into `${VAR:-default}` env vars, matching
every other credential in the file; an empty, untracked, never-referenced
`infrastructure/kubernetes/` directory removed; and Dependabot
([.github/dependabot.yml](../../.github/dependabot.yml)) added for every
ecosystem in the repo (weekly, grouped per ecosystem) — originally `gomod`
plus the frontend's `npm`/Docker/GitHub Actions entries, now one `pip`
entry per Python directory instead of `gomod`, following the rewrite.

**Not implemented here**: an actual live GitHub Actions run against a
pushed remote at the time these jobs were first written (verified instead
by running the exact same command sequence locally, twice, from a
completely fresh state); a CD step that deploys anywhere real; Trivy image
scanning and CodeQL; ruff/mypy as real linters (the compile-check above is
a syntax-only substitute).
