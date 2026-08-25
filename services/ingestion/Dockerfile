FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY services/ingestion/ ./services/ingestion/
RUN CGO_ENABLED=0 go build -o /out/ingestion ./services/ingestion

FROM alpine:3.20
RUN adduser -D -u 10001 appuser
USER appuser
COPY --from=build /out/ingestion /usr/local/bin/ingestion
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/ingestion"]
