FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ ./pkg/
COPY services/anomaly-detector/ ./services/anomaly-detector/
RUN CGO_ENABLED=0 go build -o /out/anomaly-detector ./services/anomaly-detector

FROM alpine:3.20
RUN adduser -D -u 10001 appuser
USER appuser
COPY --from=build /out/anomaly-detector /usr/local/bin/anomaly-detector
EXPOSE 8083
ENTRYPOINT ["/usr/local/bin/anomaly-detector"]
