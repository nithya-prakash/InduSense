FROM python:3.14-slim AS build
WORKDIR /src
COPY simulator/requirements.txt ./requirements.txt
RUN python -m venv /venv && /venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.14-slim
RUN useradd -u 10001 -m appuser
COPY --from=build /venv /venv
WORKDIR /app
COPY shared/ ./shared/
COPY simulator/ ./simulator/
ENV PATH="/venv/bin:${PATH}" \
    PYTHONPATH="/app" \
    PYTHONUNBUFFERED=1
USER appuser
WORKDIR /app/simulator
ENTRYPOINT ["python", "main.py"]
