// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const (
	serviceName      = "event-gateway"
	logFieldCap      = 64
	outcomeAccepted  = "accepted"
	outcomeDuplicate = "duplicate"
	outcomeRejected  = "rejected"
)

var nonPrintable = regexp.MustCompile(`[^\x20-\x7e]`)

// telemetry carries the logger, tracer and counters the gateway records to.
// The caller supplies the providers, so nothing here reads the environment.
// A nil provider falls back to a no-op one.
type telemetry struct {
	logger *slog.Logger
	tracer trace.Tracer
	ingest metric.Int64Counter

	// parked counts successful park publishes. Failed publishes increment parkFailed instead.
	parked metric.Int64Counter
	// parkFailed counts failed park publishes, which do not increment parked.
	parkFailed metric.Int64Counter
}

func newTelemetry(handler slog.Handler, tracer trace.TracerProvider, meter metric.MeterProvider) telemetry {
	if tracer == nil {
		tracer = tracenoop.NewTracerProvider()
	}

	if meter == nil {
		meter = metricnoop.NewMeterProvider()
	}

	signals := meter.Meter(serviceName)
	//nolint:errcheck // The counter names are valid, fixed instrument names.
	ingest, _ := signals.Int64Counter("event_gateway_ingest_total")
	//nolint:errcheck // The counter name is a valid, fixed instrument name.
	parked, _ := signals.Int64Counter("event_gateway_parked_total")
	//nolint:errcheck // The counter name is a valid, fixed instrument name.
	parkFailed, _ := signals.Int64Counter("event_gateway_park_publish_failed_total")

	return telemetry{
		logger:     slog.New(handler),
		tracer:     tracer.Tracer(serviceName),
		ingest:     ingest,
		parked:     parked,
		parkFailed: parkFailed,
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

func (t telemetry) logWebhook(ctx context.Context, meta envelope, status int, duplicate bool) {
	outcome, reason := outcomeAccepted, ""
	if status != http.StatusAccepted {
		outcome, reason = outcomeRejected, rejectReason(status)
	} else if duplicate {
		outcome = outcomeDuplicate
	}

	attrs := []slog.Attr{
		slog.String("provider", meta.Provider),
		slog.String("event", sanitizeLog(meta.Event)),
		slog.String("delivery", sanitizeLog(meta.Delivery)),
	}
	if meta.Repository != nil {
		attrs = append(attrs, slog.String("repository", sanitizeLog(*meta.Repository)))
	}

	attrs = append(attrs, slog.Int("status", status), slog.String("outcome", outcome))
	if reason != "" {
		attrs = append(attrs, slog.String("reason", reason))
	}

	t.logger.LogAttrs(ctx, slog.LevelInfo, "webhook received", attrs...)
}

func sanitizeLog(value string) string {
	return nonPrintable.ReplaceAllLiteralString(value[:min(len(value), logFieldCap)], "_")
}

func rejectReason(status int) string {
	switch status {
	case http.StatusUnauthorized:
		return "bad_signature"
	case http.StatusRequestEntityTooLarge:
		return "size_cap"
	case http.StatusConflict:
		return "body_conflict"
	case http.StatusServiceUnavailable:
		return "store_unavailable"
	default:
		return "bad_body"
	}
}
