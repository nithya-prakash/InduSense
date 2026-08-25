// Package tracing wires each service into a single OpenTelemetry
// TracerProvider exporting to Jaeger over OTLP/HTTP, and provides a
// kafka.Header carrier so trace context survives the hop across Kafka —
// otel's built-in propagators only know how to inject/extract HTTP-style
// text map carriers, so producers/consumers need an adapter.
package tracing

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	kafka "github.com/segmentio/kafka-go"
)

// Init configures the global TracerProvider and propagator for the calling
// service. It reads OTEL_EXPORTER_OTLP_ENDPOINT (defaulting to
// http://localhost:4318, matching .env.example) and never fails startup:
// if the exporter can't be built, tracing is disabled and a no-op shutdown
// is returned, since a missing collector must never take down a service
// whose job is ingesting real telemetry.
func Init(ctx context.Context, serviceName string) (shutdown func(context.Context) error, err error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4318"
	}

	// WithEndpoint (host:port, no scheme) appends the default "/v1/traces"
	// path; WithEndpointURL takes the URL's path verbatim and does NOT
	// default it, which silently 404s against Jaeger's OTLP receiver if
	// OTEL_EXPORTER_OTLP_ENDPOINT (an OTel-standard base URL, no path) is
	// passed straight through.
	host := endpoint
	if u, parseErr := url.Parse(endpoint); parseErr == nil && u.Host != "" {
		host = u.Host
	}

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(host),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return func(context.Context) error { return nil }, fmt.Errorf("tracing: build otlp exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return func(context.Context) error { return nil }, fmt.Errorf("tracing: build resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// Tracer returns the named tracer from the global provider — a thin
// convenience so call sites don't import otel directly.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

// kafkaHeaderCarrier adapts a *[]kafka.Header to propagation.TextMapCarrier
// so the standard W3C traceparent propagator can inject into, and extract
// from, Kafka message headers exactly as it would HTTP headers.
type kafkaHeaderCarrier struct {
	headers *[]kafka.Header
}

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaHeaderCarrier) Set(key, value string) {
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}
	return keys
}

// InjectKafka writes the span context carried by ctx into headers, so a
// consumer on the other side of the broker can continue the same trace.
func InjectKafka(ctx context.Context, headers *[]kafka.Header) {
	otel.GetTextMapPropagator().Inject(ctx, kafkaHeaderCarrier{headers: headers})
}

// ExtractKafka reads any propagated span context out of a consumed
// message's headers and returns a context a consumer can start child spans
// from. If no trace context is present, it returns ctx unchanged and the
// consumer's span simply becomes a new trace root.
func ExtractKafka(ctx context.Context, headers []kafka.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, kafkaHeaderCarrier{headers: &headers})
}
