// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"

	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	errOTLPEndpoint = errors.New("must be an https URL, or an http URL on a loopback host")
	errOTLPHeaders  = errors.New("must be comma-separated key=value pairs")
)

const (
	serviceName     = "event-gateway"
	shutdownTimeout = 10 * time.Second
)

type telemetry struct {
	logger   *slog.Logger
	tracer   trace.Tracer
	ingest   metric.Int64Counter
	shutdown func(context.Context) error
}

func openTelemetry(ctx context.Context, out io.Writer) (telemetry, error) {
	handler := slog.NewJSONHandler(out, nil)

	enabled, err := checkOTLPEnv()
	if err != nil {
		return telemetry{}, err
	}

	if !enabled {
		return newTelemetry(handler, otel.GetTracerProvider(), otel.GetMeterProvider(), func(context.Context) error {
			return nil
		}), nil
	}

	// The SDK's default handler prints the error text, which can carry exporter
	// settings such as header values.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {
		slog.New(handler).ErrorContext(ctx, "OTLP export failed")
	}))

	traceExporter, traceErr := otlptracehttp.New(ctx)
	metricExporter, metricErr := otlpmetrichttp.New(ctx)
	logExporter, logErr := otlploghttp.New(ctx)

	err = errors.Join(traceErr, metricErr, logErr)
	if err != nil {
		return telemetry{}, fmt.Errorf("configure OTLP exporters: %w", err)
	}

	res := resource.NewSchemaless(attribute.String("service.name", serviceName))
	tracer := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(traceExporter))
	meter := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
	)
	logger := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
	)
	both := slog.NewMultiHandler(handler, otelslog.NewHandler(serviceName, otelslog.WithLoggerProvider(logger)))

	var (
		once        sync.Once
		shutdownErr error
	)

	return newTelemetry(both, tracer, meter, func(ctx context.Context) error {
		once.Do(func() {
			shutdownErr = errors.Join(tracer.Shutdown(ctx), meter.Shutdown(ctx), logger.Shutdown(ctx))
		})

		return shutdownErr
	}), nil
}

// checkOTLPEnv reports whether any OTLP endpoint is set, and rejects settings
// that could send or print a credential. Errors name the variable, never its
// value.
func checkOTLPEnv() (bool, error) {
	var errs []error

	enabled := false

	for _, signal := range []string{"", "TRACES_", "METRICS_", "LOGS_"} {
		name := "OTEL_EXPORTER_OTLP_" + signal + "ENDPOINT"

		endpoint := os.Getenv(name)
		if endpoint != "" {
			enabled = true

			if !secureEndpoint(endpoint) {
				errs = append(errs, fmt.Errorf("%s %w", name, errOTLPEndpoint))
			}
		}

		name = "OTEL_EXPORTER_OTLP_" + signal + "HEADERS"
		if !validHeaders(os.Getenv(name)) {
			errs = append(errs, fmt.Errorf("%s %w", name, errOTLPHeaders))
		}
	}

	return enabled, errors.Join(errs...)
}

func secureEndpoint(endpoint string) bool {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return false
	}

	if parsed.Scheme == "https" {
		return true
	}

	host := parsed.Hostname()
	if parsed.Scheme != "http" {
		return false
	}

	addr, err := netip.ParseAddr(host)

	return host == "localhost" || (err == nil && addr.IsLoopback())
}

// validHeaders rejects HTTP header syntax such as "Authorization: Bearer ...",
// which the SDK would print instead of sending.
func validHeaders(headers string) bool {
	if headers == "" {
		return true
	}

	for pair := range strings.SplitSeq(headers, ",") {
		key, _, found := strings.Cut(pair, "=")

		key = strings.TrimSpace(key)
		if !found || key == "" || strings.ContainsAny(key, " \t:") {
			return false
		}
	}

	return true
}

func newTelemetry(
	handler slog.Handler,
	tracer trace.TracerProvider,
	meter metric.MeterProvider,
	shutdown func(context.Context) error,
) telemetry {
	//nolint:errcheck // The counter name is a valid, fixed instrument name.
	ingest, _ := meter.Meter(serviceName).Int64Counter("event_gateway_ingest_total")

	return telemetry{
		logger:   slog.New(handler),
		tracer:   tracer.Tracer(serviceName),
		ingest:   ingest,
		shutdown: shutdown,
	}
}

func (t telemetry) close(ctx context.Context) {
	shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()

	err := t.shutdown(shutdown)
	if err != nil {
		t.logger.ErrorContext(shutdown, "OTLP shutdown failed")
	}
}

func (t telemetry) record(ctx context.Context, span trace.Span, event string, status int) {
	attrs := []attribute.KeyValue{
		attribute.String("event", event),
		attribute.Int("http.response.status_code", status),
	}
	span.SetAttributes(attrs...)
	t.ingest.Add(ctx, 1, metric.WithAttributes(attrs...))
}
