// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//nolint:exhaustruct_v5 // Fixtures use zero defaults for fields unrelated to the case.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const (
	parkedPayloadFixture  = "private-payload-fixture"
	parkedPayloadJSON     = `{"value":"` + parkedPayloadFixture + `"}`
	parkedFixtureProvider = "github"
)

var errLongParked = errors.New(strings.Repeat("x", 512) + "overflow")

//nolint:ireturn // Return the actual JetStream stream to the tests.
func testParkedStream(t *testing.T, store eventStore) jetstream.Stream {
	t.Helper()

	stream, err := openParkedStream(t.Context(), store.jet)
	checkError(t, err)

	return stream
}

func TestParkedStreamConfig(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	store := testStore(t, retention)
	before := store.stream.CachedInfo().Config
	info := testParkedStream(t, store).CachedInfo()
	checkEqual(t, slices.Equal(info.Config.Subjects, []string{"parked.>"}), true)
	checkEqual(t, info.Config.Storage, jetstream.FileStorage)
	checkEqual(t, info.Config.Replicas, 1)
	checkEqual(t, info.Config.MaxAge, 30*24*time.Hour)

	after, err := store.stream.Info(ctx)
	checkError(t, err)
	checkEqual(t, reflect.DeepEqual(after.Config, before), true)

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	_, err = openParkedStream(canceled, store.jet)
	checkEqual(t, errors.Is(err, context.Canceled), true)
}

func TestParkedRecord(t *testing.T) {
	t.Parallel()

	const delivery = "delivery-1"

	ctx := t.Context()
	store := testStore(t, retention)
	stream := testParkedStream(t, store)
	req := signedRequest(ctx, issuesEvent, delivery, parkedPayloadJSON)
	testWebhook(t, store, defaultBodyCap).ServeHTTP(httptest.NewRecorder(), req)

	stored, readErr := store.stream.GetMsg(ctx, 1)
	checkError(t, readErr)

	valid := Event{Envelope: stored.Data, Seq: stored.Sequence}
	bodyErr := fmt.Errorf("%w: %s %s", ErrPermanent, stored.Data, parkedPayloadJSON)
	hook := parkHook(store.jet, newTelemetry(slog.DiscardHandler, nil, nil), 0)

	for _, test := range []struct {
		err   error
		name  string
		want  parkedRecord
		event Event
	}{
		{
			name: "wrapped-rule", event: valid,
			err: fmt.Errorf("delivery: %w", RuleError{Rule: "select", Err: bodyErr}),
			want: parkedRecord{
				Rule: "select", Provider: parkedFixtureProvider, Event: issuesEvent, Delivery: delivery,
				Error: "delivery: permanent: [redacted] [redacted]",
			},
		},
		{
			name:  "undecodable-envelope",
			event: Event{Envelope: []byte(`{"provider":"github","event":5,"delivery":"delivery-1"}`), Seq: 2},
			err:   errLongParked,
			want:  parkedRecord{Error: strings.Repeat("x", 512)},
		},
		{
			name:  "short-payload",
			event: Event{Envelope: []byte(`{"payload":0}`)},
			err:   fmt.Errorf("%w: status 400", ErrPermanent),
			want:  parkedRecord{Error: "permanent: status 400"},
		},
		{
			name:  "eight-byte-payload",
			event: Event{Envelope: []byte(`{"payload":12345678}`)},
			err:   fmt.Errorf("%w: 12345678 12345678", ErrPermanent),
			want:  parkedRecord{Error: "permanent: [redacted] [redacted]"},
		},
		{
			name: "typed-nil-error", event: valid,
			err: (*json.SyntaxError)(nil),
			want: parkedRecord{
				Provider: parkedFixtureProvider, Event: issuesEvent, Delivery: delivery, Error: ErrPermanent.Error(),
			},
		},
		{
			name: "empty-rule-error", event: valid,
			err: RuleError{},
			want: parkedRecord{
				Provider: parkedFixtureProvider, Event: issuesEvent, Delivery: delivery, Error: ErrPermanent.Error(),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			subject := "parked." + test.name
			hook(t.Context(), test.name, test.event, test.err)

			msg, err := stream.GetLastMsgForSubject(t.Context(), subject)
			checkError(t, err)
			checkEqual(t, msg.Subject, subject)
			checkEqual(t, bytes.Contains(msg.Data, []byte(parkedPayloadFixture)), false)

			var record parkedRecord
			checkError(t, json.Unmarshal(msg.Data, &record))
			parkedAt, err := time.Parse(time.RFC3339, record.ParkedAt)
			checkError(t, err)
			checkEqual(t, parkedAt.Location(), time.UTC)

			test.want.ParkedAt, test.want.Seq, test.want.Target = record.ParkedAt, test.event.Seq, test.name
			checkEqual(t, record, test.want)
		})
	}
}

func TestParkedTelemetry(t *testing.T) {
	t.Parallel()

	const timeout = 50 * time.Millisecond

	ctx := t.Context()
	store := testStore(t, retention)
	testParkedStream(t, store)

	var logs bytes.Buffer

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	signals := newTelemetry(slog.NewJSONHandler(&logs, nil), nil, meter)
	envelopeJSON := `{"provider":"github","event":"issues","payload":"` + parkedPayloadFixture + `"}`
	event := Event{Envelope: []byte(envelopeJSON), Seq: 1}
	failure := RuleError{Rule: "match", Err: ErrPermanent}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	parkHook(store.jet, signals, 0)(canceled, "sample", event, failure)
	checkError(t, store.jet.DeleteStream(ctx, parkedStream))

	before := time.Now()

	parkHook(store.jet, signals, timeout)(ctx, "sample", event, failure)

	checkEqual(t, time.Since(before) < timeout+time.Second, true)
	checkEqual(t, strings.Contains(logs.String(), parkedPayloadFixture), false)

	checkEqual(t, strings.Count(logs.String(), "\n"), 2)
	checkEqual(t, strings.Contains(logs.String(), `"msg":"event parked"`), true)
	checkEqual(t, strings.Contains(logs.String(), `"msg":"park publish failed"`), true)

	var collected metricdata.ResourceMetrics
	checkError(t, reader.Collect(ctx, &collected))

	counts := map[string]int64{}

	for _, instrument := range collected.ScopeMetrics[0].Metrics {
		data, ok := instrument.Data.(metricdata.Sum[int64])
		checkEqual(t, ok, true)
		checkEqual(t, len(data.DataPoints), 1)
		point := data.DataPoints[0]
		target, ok := point.Attributes.Value("target")
		checkEqual(t, ok, true)
		checkEqual(t, target.AsString(), "sample")

		counts[instrument.Name] = point.Value
	}

	checkEqual(t, counts["event_gateway_parked_total"], int64(1))
	checkEqual(t, counts["event_gateway_park_publish_failed_total"], int64(1))
}

func TestListParked(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	store := testStore(t, retention)
	stream := testParkedStream(t, store)
	empty, listErr := listParked(ctx, stream, "", 0)
	checkError(t, listErr)
	checkEqual(t, len(empty), 0)

	const firstTarget = "one"

	hook := parkHook(store.jet, newTelemetry(slog.DiscardHandler, nil, nil), 0)
	for i, target := range []string{firstTarget, "two", firstTarget} {
		hook(ctx, target, Event{Seq: uint64(i + 1)}, ErrPermanent)
	}

	for _, test := range []struct {
		target string
		want   []uint64
		limit  int
	}{
		{target: firstTarget, want: []uint64{1, 3}},
		{want: []uint64{1, 2, 3}},
		{limit: 2, want: []uint64{1, 2}},
	} {
		records, err := listParked(ctx, stream, test.target, test.limit)
		checkError(t, err)

		checkEqual(t, len(records), len(test.want))

		for i, record := range records {
			checkEqual(t, record.Seq, test.want[i])
		}
	}

	_, err := listParked(ctx, stream, "", -1)
	checkEqual(t, errors.Is(err, errParkedLimit), true)
	_, err = listParked(ctx, parkedPullStream{Stream: stream}, "", 0)
	checkEqual(t, errors.Is(err, errRetryDelivery), true)
	_, err = store.jet.Publish(ctx, "parked.one", []byte("invalid JSON"))
	checkError(t, err)
	_, err = listParked(ctx, stream, "", 0)

	var syntax *json.SyntaxError

	checkEqual(t, errors.As(err, &syntax), true)
	checkError(t, store.jet.DeleteStream(ctx, parkedStream))
	_, err = listParked(ctx, stream, "", 0)
	checkEqual(t, errors.Is(err, jetstream.ErrStreamNotFound), true)
}

type failedParkedPull struct{ jetstream.Consumer }

//nolint:ireturn // Match the JetStream consumer interface.
func (failedParkedPull) FetchNoWait(int) (jetstream.MessageBatch, error) {
	return nil, errRetryDelivery
}

type parkedPullStream struct{ jetstream.Stream }

//nolint:ireturn // Match the JetStream stream interface.
func (stream parkedPullStream) OrderedConsumer(
	ctx context.Context, config jetstream.OrderedConsumerConfig,
) (jetstream.Consumer, error) {
	consumer, err := stream.Stream.OrderedConsumer(ctx, config)

	return failedParkedPull{Consumer: consumer}, err //nolint:wrapcheck // Preserve the real consumer's error.
}

func TestRetryParked(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	store := testStore(t, retention)
	stream := testParkedStream(t, store)
	publishRunner(t, store, issuesSubject)
	publishRunner(t, store, issuesSubject)
	checkError(t, store.stream.DeleteMsg(ctx, 1))
	parkHook(store.jet, newTelemetry(slog.DiscardHandler, nil, nil), 0)(ctx, "retry", Event{Seq: 2}, ErrPermanent)

	for _, test := range []struct {
		deliveryErr error
		want        error
		seq         uint64
		calls       int
		panics      bool
	}{
		{seq: 2, calls: 1},
		{seq: 2, calls: 1, deliveryErr: errRetryDelivery, want: errRetryDelivery},
		{seq: 2, calls: 1, panics: true, want: ErrPermanent},
		{seq: 1, want: jetstream.ErrMsgNotFound},
		{seq: 3, want: jetstream.ErrMsgNotFound},
	} {
		calls := 0
		target := fakeTarget{name: "retry", deliver: func(_ context.Context, event Event) error {
			calls++

			checkEqual(t, event.Seq, test.seq)
			checkEqual(t, string(event.Envelope), testEnvelope)

			if test.panics {
				panic(testEnvelope)
			}

			return test.deliveryErr
		}}
		err := retryParked(ctx, store, target, test.seq)
		checkEqual(t, errors.Is(err, test.want), true)
		checkEqual(t, calls, test.calls)
	}

	info, err := stream.Info(ctx)
	checkError(t, err)
	checkEqual(t, info.State.Msgs, uint64(1))
}
