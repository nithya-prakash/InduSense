# A migrations SQL directory doesn't belong in a ConfigMap (1MiB limit,
# and it decouples the schema from the image that applies it) — this bakes
# migrations/ into the same migrate/migrate binary the Makefile already
# uses via `docker run`, so the Kubernetes migration Job needs no source
# checkout, just this image.
FROM migrate/migrate:v4.19.1
COPY migrations /migrations
ENTRYPOINT ["migrate", "-path=/migrations"]
