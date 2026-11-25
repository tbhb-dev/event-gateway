// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//nolint:exhaustruct_v5 // Fixtures use zero defaults for fields unrelated to the case.
package main

// cspell:words eventfields

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"go.tbhb.dev/event-gateway/internal/eventfields"
	"go.tbhb.dev/event-gateway/internal/routing"
)

const ruleMatch = `"match":{"provider":"github","event":"pull_request",` +
	`"repository":["example/repo"],"action":["opened"]}`

const (
	fakeType       = "fake"
	firstRule      = "first"
	fixtureTargets = `{"targets":[{"name":"receiver","type":"` + fakeType + `","start":"all","rules":[` +
		`{"name":"` + firstRule + `",` + ruleMatch + `,"action":{}},{"name":"second",` + ruleMatch + `,"action":{}}]}]}`
	fixtureNewTarget = `{"targets":[{"name":"receiver","type":"` + fakeType + `","start":"new","rules":[` +
		`{"name":"` + firstRule + `",` + ruleMatch + `,"action":{}}]}]}`
)

const ruleEnvelope = `{"provider":"github","event":"pull_request","delivery":"fixture","repository":"Example/Repo",
	"payload":{"action":"opened"}}`

type actorFunc func(routing.Rule, eventfields.Fields, []byte) error

func (act actorFunc) Act(_ context.Context, rule routing.Rule, fields eventfields.Fields, envelope []byte) error {
	return act(rule, fields, envelope)
}

type actorError bool

func (actorError) Error() string       { return "action failed" }
func (err actorError) Permanent() bool { return bool(err) }

func writeTargets(t *testing.T, data string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "targets.json")
	checkError(t, os.WriteFile(path, []byte(data), 0o600))

	return path
}

func TestServeTargets(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		want       error
		name, data string
	}{
		{name: "unknown type", data: fixtureTargets, want: errUnknownTargetType},
		{name: "invalid file", data: `{"targets":[],"private-fixture":true}`, want: errTargetsFile},
		{name: "invalid JSON", data: "private-fixture", want: errTargetsFile},
		{name: "invalid array", data: `{"targets":[null]}`, want: errTargetsFile},
		{name: "null targets", data: `{"targets":null}`, want: errTargetsFile},
		{name: "missing file", want: os.ErrNotExist},
		{name: "empty array", data: `{ "target\u0073": [ ] }`, want: errStoreDir},
		{name: "empty object", data: "{}", want: errStoreDir},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "missing.json")

			if test.data != "" {
				path = writeTargets(t, test.data)
			}

			args := []string{"-targets", path, "-github-secret=" + fixtureKey, storeDirFlag}
			err := serve(t.Context(), io.Discard, slog.DiscardHandler, args)
			checkEqual(t, errors.Is(err, test.want), true)
			checkEqual(t, strings.Contains(err.Error(), "private-fixture"), false)
		})
	}
}

//nolint:paralleltest // Serve uses the process-wide target registry.
func TestServeStartsConfiguredRunner(t *testing.T) {
	targetTypes[fakeType] = func(routing.Target) (actor, error) { return actorFunc(nil), nil }

	t.Cleanup(func() { delete(targetTypes, fakeType) })

	t.Chdir(shortSocketDir(t))

	for name, data := range map[string]string{"registered": fixtureNewTarget, "empty": `{"targets":[]}`} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			args := []string{
				"-targets=" + writeTargets(t, data), "-listen=127.0.0.1",
				storeDirFlag + dir, "-github-secret=" + fixtureKey, "-parked-socket=s",
			}
			err := serve(t.Context(), io.Discard, slog.DiscardHandler, args)
			_, listenErr := errors.AsType[*net.AddrError](err)
			checkEqual(t, listenErr, true)

			store, stop, err := openStore(t.Context(), storeOptions(dir), retention)
			checkError(t, err)
			t.Cleanup(stop)

			_, parkedErr := store.jet.Stream(t.Context(), parkedStream)
			_, consumerErr := store.stream.Consumer(t.Context(), "receiver")

			if name == "empty" {
				checkEqual(t, errors.Is(parkedErr, jetstream.ErrStreamNotFound), true)
				checkEqual(t, errors.Is(consumerErr, jetstream.ErrConsumerNotFound), true)
			} else {
				checkError(t, parkedErr)
				checkError(t, consumerErr)
			}
		})
	}
}

func TestTargetStartPositions(t *testing.T) {
	t.Parallel()

	const timestamp = "2026-01-01T00:00:00Z"

	since, parseErr := time.Parse(time.RFC3339, timestamp)
	checkError(t, parseErr)

	for start, want := range map[string]startPosition{"new": {}, "all": {all: true}, timestamp: {at: &since}} {
		path := writeTargets(t, strings.Replace(fixtureTargets, `"all"`, `"`+start+`"`, 1))

		var cfg serveConfig
		checkError(t, cfg.parse(io.Discard, []string{"-targets", path}))
		targets, err := loadTargets(cfg.targets, map[string]func(routing.Target) (actor, error){
			fakeType: func(routing.Target) (actor, error) { return actorFunc(nil), nil },
		})
		checkError(t, err)
		checkEqual(t, len(targets), 1)
		checkEqual(t, targets[0].Name(), "receiver")
		checkEqual(t, slices.Equal(targets[0].Subjects, []string{pullSubject}), true)
		checkEqual(t, reflect.DeepEqual(targets[0].Start, want), true)
	}

	_, err := loadTargets(writeTargets(t, fixtureTargets), map[string]func(routing.Target) (actor, error){
		fakeType: func(routing.Target) (actor, error) { return nil, errRetryDelivery },
	})
	checkEqual(t, errors.Is(err, errRetryDelivery), true)
}

func TestRuleTargetDelivery(t *testing.T) {
	t.Parallel()

	config, loadErr := routing.Load([]byte(fixtureTargets))
	checkError(t, loadErr)

	for _, test := range []struct {
		name, envelope string
		failure        error
		calls          []string
		permanent      bool
	}{
		{name: "match", envelope: ruleEnvelope, calls: []string{firstRule, "second"}},
		{name: "no match", envelope: strings.Replace(ruleEnvelope, "opened", "closed", 1)},
		{name: "retry", envelope: ruleEnvelope, failure: errRetryDelivery, calls: []string{firstRule}},
		{name: "permanent", envelope: ruleEnvelope, failure: actorError(true), calls: []string{firstRule}, permanent: true},
		{name: "temporary", envelope: ruleEnvelope, failure: actorError(false), calls: []string{firstRule}},
		{name: "sentinel", envelope: ruleEnvelope, failure: ErrPermanent, calls: []string{firstRule}},
		{name: "malformed", envelope: "private envelope fixture", permanent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var calls []string

			act := actorFunc(func(rule routing.Rule, fields eventfields.Fields, envelope []byte) error {
				checkEqual(t, string(envelope), test.envelope)
				checkEqual(t, fields.Seq, uint64(42))
				checkEqual(t, fields.Repository, "Example/Repo")

				calls = append(calls, rule.Name)

				return test.failure
			})
			target := ruleTarget{actor: act, config: config.Targets[0]}
			err := target.Deliver(t.Context(), Event{Seq: 42, Envelope: []byte(test.envelope)})
			checkEqual(t, slices.Equal(calls, test.calls), true)
			checkEqual(t, errors.Is(err, ErrPermanent), test.permanent)
			checkEqual(t, errors.Is(err, errRetryDelivery), errors.Is(test.failure, errRetryDelivery))
			checkEqual(t, err != nil, test.failure != nil || test.permanent)

			if test.failure != nil {
				rule, ok := errors.AsType[RuleError](err)
				checkEqual(t, ok, true)
				checkEqual(t, rule.Rule, firstRule)
			}

			if err != nil {
				checkEqual(t, strings.Contains(err.Error(), test.envelope), false)
			}
		})
	}
}

func TestRunWithTargets(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	store := testStore(t, retention)
	calls := make(chan uint64, 2)
	targets, loadErr := loadTargets(writeTargets(t, fixtureTargets), map[string]func(routing.Target) (actor, error){
		fakeType: func(routing.Target) (actor, error) {
			return actorFunc(func(_ routing.Rule, fields eventfields.Fields, _ []byte) error {
				calls <- fields.Seq

				return actorError(true)
			}), nil
		},
	})
	checkError(t, loadErr)

	for _, data := range []string{strings.Replace(ruleEnvelope, "opened", "closed", 1), ruleEnvelope} {
		_, err := store.jet.Publish(ctx, pullSubject, []byte(data))
		checkError(t, err)
	}

	signals := newTelemetry(slog.DiscardHandler, nil, nil)

	done := make(chan error, 1)
	go func() {
		done <- runWithTargets(ctx, store, signals, targets, func(ctx context.Context) error {
			<-ctx.Done()

			return nil
		})
	}()

	checkEqual(t, receiveRunner(t, calls), uint64(2))
	cancel()

	checkError(t, receiveRunner(t, done))
	checkEqual(t, len(calls), 0)

	stream, err := store.jet.Stream(t.Context(), parkedStream)
	checkError(t, err)
	records, err := listParked(t.Context(), stream, "", 0)
	checkError(t, err)
	checkEqual(t, len(records), 1)
	checkEqual(t, records[0].Target, "receiver")
	checkEqual(t, records[0].Rule, firstRule)
	checkEqual(t, records[0].Seq, uint64(2))
}

func TestRunWithNewTarget(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	store := testStore(t, retention)
	calls := make(chan Event, 2)
	targets, err := loadTargets(writeTargets(t, fixtureNewTarget), map[string]func(routing.Target) (actor, error){
		fakeType: func(routing.Target) (actor, error) {
			return actorFunc(func(_ routing.Rule, fields eventfields.Fields, envelope []byte) error {
				calls <- Event{Seq: fields.Seq, Envelope: envelope}

				return nil
			}), nil
		},
	})
	checkError(t, err)

	signals := newTelemetry(slog.DiscardHandler, nil, nil)
	err = runWithTargets(ctx, store, signals, targets, func(ctx context.Context) error {
		consumer, consumerErr := store.stream.Consumer(ctx, targets[0].Name())
		checkError(t, consumerErr)
		checkEqual(t, consumer.CachedInfo().Config.DeliverPolicy, jetstream.DeliverNewPolicy)

		ack, publishErr := store.jet.Publish(ctx, pullSubject, []byte(ruleEnvelope))
		checkError(t, publishErr)

		got := receiveRunner(t, calls)
		checkEqual(t, got.Seq, ack.Sequence)
		checkEqual(t, string(got.Envelope), ruleEnvelope)
		cancel()

		return nil
	})
	checkError(t, err)
	checkEqual(t, len(calls), 0)
}

func TestRunWithTargetsErrors(t *testing.T) {
	t.Parallel()

	store := testStore(t, retention)
	signals := newTelemetry(slog.DiscardHandler, nil, nil)
	targets := []runnerTarget{{Target: fakeTarget{name: "."}}}
	called := false
	http := func(context.Context) error {
		called = true

		return nil
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err := runWithTargets(ctx, store, signals, targets, http)
	checkEqual(t, errors.Is(err, jetstream.ErrInvalidConsumerName), true)
	checkEqual(t, called, false)

	cancel()

	err = runWithTargets(ctx, store, signals, targets, http)
	checkEqual(t, errors.Is(err, context.Canceled), true)
	checkEqual(t, called, false)
}

type cancelTarget struct {
	Target

	cancel context.CancelFunc
}

func (target cancelTarget) Name() string {
	target.cancel()

	return target.Target.Name()
}

func TestRunWithTargetsStartupCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	store := testStore(t, retention)
	signals := newTelemetry(slog.DiscardHandler, nil, nil)
	targets := []runnerTarget{{Target: cancelTarget{Target: fakeTarget{name: "receiver"}, cancel: cancel}}}
	called := false
	err := runWithTargets(ctx, store, signals, targets, func(context.Context) error {
		called = true

		return nil
	})
	checkError(t, err)
	checkEqual(t, ctx.Err(), context.Canceled)
	checkEqual(t, called, false)
}
