// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

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

const serviceName = "event-gateway"

type telemetry struct {
	logger   *slog.Logger
	tracer   trace.Tracer
	ingest   metric.Int64Counter
	shutdown func(context.Context) error
}

func openTelemetry(ctx context.Context, out io.Writer) (telemetry, error) {
	handler := slog.NewJSONHandler(out, nil)
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return newTelemetry(handler, otel.GetTracerProvider(), otel.GetMeterProvider(), func(context.Context) error {
			return nil
		}), nil
	}

	traceExporter, traceErr := otlptracehttp.New(ctx)
	metricExporter, metricErr := otlpmetrichttp.New(ctx)
	logExporter, logErr := otlploghttp.New(ctx)

	err := errors.Join(traceErr, metricErr, logErr)
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

	return newTelemetry(both, tracer, meter, func(ctx context.Context) error {
		return errors.Join(tracer.Shutdown(ctx), meter.Shutdown(ctx), logger.Shutdown(ctx))
	}), nil
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
