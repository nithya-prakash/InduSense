# A general-purpose Python dev/test image for the whole repo -- the
# Python-rewrite's replacement for what one `go.mod` + `go test ./...`
# used to give for free. Bundles every service's dependencies (see
# requirements.txt) plus pytest, and copies in every Python source
# directory, so it can run pytest against any of them: a single service's
# own tests/ (e.g. `services/ingestion/tests`), `shared/tests`, or this
# directory's cross-service suites (contract/integration/e2e).
FROM python:3.12-slim AS build
WORKDIR /src
COPY tests/requirements.txt ./requirements.txt
RUN python -m venv /venv && /venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.12-slim
WORKDIR /app
COPY --from=build /venv /venv
COPY shared/ ./shared/
COPY services/ ./services/
COPY simulator/ ./simulator/
COPY scripts/ ./scripts/
COPY tests/ ./tests/
ENV PATH="/venv/bin:${PATH}" \
    PYTHONPATH="/app" \
    PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "pytest"]
CMD ["tests/contract", "tests/integration", "tests/e2e", "-v"]
