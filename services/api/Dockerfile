FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ ./pkg/
COPY services/api/ ./services/api/
RUN CGO_ENABLED=0 go build -o /out/api ./services/api

FROM alpine:3.20
RUN adduser -D -u 10001 appuser
USER appuser
COPY --from=build /out/api /usr/local/bin/api
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/api"]
