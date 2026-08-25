FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ ./pkg/
COPY services/alert-service/ ./services/alert-service/
RUN CGO_ENABLED=0 go build -o /out/alert-service ./services/alert-service

FROM alpine:3.20
RUN adduser -D -u 10001 appuser
USER appuser
COPY --from=build /out/alert-service /usr/local/bin/alert-service
EXPOSE 8084
ENTRYPOINT ["/usr/local/bin/alert-service"]
