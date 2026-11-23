// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const serviceName = "event-gateway"

// telemetry carries the logger, tracer and counter the handlers record to.
// The caller supplies the providers, so nothing here reads the environment.
type telemetry struct {
	logger *slog.Logger
	tracer trace.Tracer
	ingest metric.Int64Counter
}

func newTelemetry(handler slog.Handler, tracer trace.TracerProvider, meter metric.MeterProvider) telemetry {
	//nolint:errcheck // The counter name is a valid, fixed instrument name.
	ingest, _ := meter.Meter(serviceName).Int64Counter("event_gateway_ingest_total")

	return telemetry{
		logger: slog.New(handler),
		tracer: tracer.Tracer(serviceName),
		ingest: ingest,
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
