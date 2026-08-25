FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ ./pkg/
COPY services/stream-processor/ ./services/stream-processor/
RUN CGO_ENABLED=0 go build -o /out/stream-processor ./services/stream-processor

FROM alpine:3.20
RUN adduser -D -u 10001 appuser
USER appuser
COPY --from=build /out/stream-processor /usr/local/bin/stream-processor
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/stream-processor"]
