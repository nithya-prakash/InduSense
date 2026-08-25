FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ ./pkg/
COPY scripts/seed/ ./scripts/seed/
RUN CGO_ENABLED=0 go build -o /out/seed ./scripts/seed

FROM alpine:3.20
RUN adduser -D -u 10001 appuser
USER appuser
COPY --from=build /out/seed /usr/local/bin/seed
ENTRYPOINT ["/usr/local/bin/seed"]
