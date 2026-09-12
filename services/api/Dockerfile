FROM python:3.14-slim AS build
WORKDIR /src
COPY services/api/requirements.txt ./requirements.txt
RUN python -m venv /venv && /venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.14-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -u 10001 -m appuser
COPY --from=build /venv /venv
WORKDIR /app
COPY shared/ ./shared/
COPY services/api/ ./services/api/
ENV PATH="/venv/bin:${PATH}" \
    PYTHONPATH="/app" \
    PYTHONUNBUFFERED=1
USER appuser
WORKDIR /app/services/api
EXPOSE 8080
ENTRYPOINT ["python", "main.py"]
