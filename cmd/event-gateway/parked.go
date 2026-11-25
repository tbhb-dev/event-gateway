// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	parkedStream   = "PARKED"
	parkedPrefix   = "parked."
	parkedMaxAge   = 30 * 24 * time.Hour
	parkTimeout    = 10 * time.Second
	parkedErrorCap = 512
	parkedListSize = 100
)

// RuleError identifies the rule that caused a delivery error.
// Err must be safe to persist and must omit the envelope and payload.
type RuleError struct {
	Err  error
	Rule string
}

func (e RuleError) Error() string    { return e.Err.Error() }
func (e RuleError) Unwrap() error    { return e.Err }
func (e RuleError) ruleName() string { return e.Rule }

//nolint:tagliatelle // The record uses the gateway protocol's timestamp naming.
type parkedRecord struct {
	Target   string `json:"target"`
	Rule     string `json:"rule"`
	Provider string `json:"provider"`
	Event    string `json:"event"`
	Delivery string `json:"delivery"`
	Error    string `json:"error"`
	ParkedAt string `json:"parked_at"`
	Seq      uint64 `json:"seq"`
}

//nolint:ireturn // JetStream exposes streams through its Stream interface.
func openParkedStream(ctx context.Context, jet jetstream.JetStream) (jetstream.Stream, error) {
	//nolint:exhaustruct_v5 // The stream uses JetStream defaults for the other options.
	stream, err := jet.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     parkedStream,
		Subjects: []string{parkedPrefix + ">"},
		Storage:  jetstream.FileStorage,
		Replicas: 1,
		MaxAge:   parkedMaxAge,
	})
	if err != nil {
		return nil, fmt.Errorf("configure PARKED stream: %w", err)
	}

	return stream, nil
}

// parkHook takes validated target names and delivery errors safe to persist.
func parkHook(
	jet jetstream.JetStream, signals telemetry, timeout time.Duration,
) func(context.Context, string, Event, error) {
	return func(ctx context.Context, target string, event Event, deliveryErr error) {
		record := newParkedRecord(target, event, deliveryErr)
		//nolint:errcheck // The record contains only strings and a sequence.
		data, _ := json.Marshal(record)

		// The runner has already acknowledged the event, even during shutdown.
		publish, cancel := context.WithTimeout(context.WithoutCancel(ctx), cmp.Or(timeout, parkTimeout))
		defer cancel()

		_, err := jet.Publish(publish, parkedPrefix+target, data, jetstream.WithExpectStream(parkedStream))

		counter, level, message := signals.parked, slog.LevelWarn, "event parked"
		if err != nil {
			counter, level, message = signals.parkFailed, slog.LevelError, "park publish failed"
		}

		counter.Add(ctx, 1, metric.WithAttributes(attribute.String("target", target)))
		signals.logger.LogAttrs(ctx, level, message, slog.String("target", target), slog.String("rule", record.Rule),
			slog.Uint64("seq", event.Seq), slog.String("provider", record.Provider), slog.String("event", record.Event))
	}
}

func newParkedRecord(target string, event Event, deliveryErr error) parkedRecord {
	var metadata struct {
		envelope

		Payload json.RawMessage `json:"payload"`
	}

	if json.Unmarshal(event.Envelope, &metadata) != nil {
		metadata.Provider, metadata.Event, metadata.Delivery = "", "", ""
	}

	var rule interface{ ruleName() string }

	var name string
	if errors.As(deliveryErr, &rule) {
		name = rule.ruleName()
	}

	message := deliveryErr.Error()

	for _, sensitive := range [][]byte{event.Envelope, metadata.Payload} {
		if len(sensitive) > 0 {
			message = strings.ReplaceAll(message, string(sensitive), "[redacted]")
		}
	}

	return parkedRecord{
		Target: target, Rule: name, Seq: event.Seq, Provider: metadata.Provider,
		Event: metadata.Event, Delivery: metadata.Delivery,
		Error:    strings.ToValidUTF8(message[:min(len(message), parkedErrorCap)], ""),
		ParkedAt: time.Now().UTC().Format(time.RFC3339),
	}
}

//nolint:wrapcheck // Report pull batch errors directly to the caller.
func listParked(ctx context.Context, stream jetstream.Stream, target string, limit int) ([]parkedRecord, error) {
	if limit < 0 {
		return []parkedRecord{}, nil
	}

	//nolint:exhaustruct_v5 // The ordered consumer reads from the oldest retained record.
	consumer, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{parkedPrefix + cmp.Or(target, ">")},
		MaxResetAttempts:  1,
		InactiveThreshold: shutdownTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("create parked consumer: %w", err)
	}

	defer func() {
		//nolint:errcheck // JetStream expires the consumer if deletion fails.
		_ = stream.DeleteConsumer(ctx, consumer.CachedInfo().Name)
	}()

	batch, err := consumer.FetchNoWait(cmp.Or(limit, parkedListSize))
	if err != nil {
		return nil, fmt.Errorf("fetch parked records: %w", err)
	}

	records := []parkedRecord{}

	for message := range batch.Messages() {
		var record parkedRecord

		decodeErr := json.Unmarshal(message.Data(), &record)
		if decodeErr != nil {
			return nil, fmt.Errorf("decode parked record: %w", decodeErr)
		}

		records = append(records, record)
	}

	return records, batch.Error()
}

//nolint:wrapcheck // Return the target's original delivery error.
func retryParked(ctx context.Context, store eventStore, target Target, seq uint64) error {
	message, err := store.stream.GetMsg(ctx, seq)
	if err != nil {
		return fmt.Errorf("read event sequence %d from EVENTS: %w", seq, err)
	}

	return target.Deliver(ctx, Event{Seq: message.Sequence, Envelope: message.Data})
}
