// Package logging provides the platform's structured JSON logging via
// log/slog. Every record carries the fields the observability spec
// requires — timestamp, service, level, message — plus event_id/device_id/
// organization_id/trace_id supplied by call sites and by the active span,
// so a log line and the Jaeger trace it happened inside share an ID a
// reader can pivot between.
package logging

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// Init builds the service-wide structured logger: JSON to stdout, slog's
// default "time"/"msg" keys renamed to the spec's "timestamp"/"message",
// and "service" attached to every record.
func Init(serviceName string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.TimeKey:
				a.Key = "timestamp"
			case slog.MessageKey:
				a.Key = "message"
			}
			return a
		},
	})
	return slog.New(handler).With("service", serviceName)
}

// WithContext enriches logger with the active span's trace ID, if the
// context carries one. Call sites that have both a context and an event to
// log should route through this rather than logging with the bare logger.
func WithContext(ctx context.Context, logger *slog.Logger) *slog.Logger {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() {
		return logger
	}
	return logger.With("trace_id", sc.TraceID().String())
}
