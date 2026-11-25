// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

// cspell:words eventfields

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"go.tbhb.dev/event-gateway/internal/eventfields"
	"go.tbhb.dev/event-gateway/internal/filter"
	"go.tbhb.dev/event-gateway/internal/routing"
)

//nolint:gochecknoglobals // Serve uses the registry of supported target types.
var targetTypes = map[string]func(routing.Target) (actor, error){}

var (
	errTargetsFile       = errors.New("invalid targets file")
	errUnknownTargetType = errors.New("unknown target type")
)

type actor interface {
	// Act performs one rule's action for one event. The error must not contain
	// event payload or envelope text. An error with a `Permanent() bool` method
	// that returns true parks the event; any other error is retried.
	Act(ctx context.Context, rule routing.Rule, fields eventfields.Fields, envelope []byte) error
}

type permanentActorError interface {
	error
	Permanent() bool
}

// retryActorError keeps the actor's message and drops the runner's sentinel.
type retryActorError struct{ error }

type ruleTarget struct {
	actor  actor
	config routing.Target
}

func (target ruleTarget) Name() string { return target.config.Name }

func (target ruleTarget) Deliver(ctx context.Context, event Event) error {
	fields, err := eventfields.FromEnvelope(event.Seq, event.Envelope)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPermanent, err)
	}

	payload := sessionJSON[sessionObject](event.Envelope)["payload"]
	matchFields := filter.Extract(fields.Event, payload)

	for _, rule := range target.config.Rules {
		if !rule.Matches(fields.Provider, fields.Event, fields.Repository, matchFields) {
			continue
		}

		err = target.actor.Act(ctx, rule, fields, event.Envelope)
		if err != nil {
			permanent, ok := errors.AsType[permanentActorError](err)
			if ok && permanent.Permanent() {
				err = fmt.Errorf("%w: %w", ErrPermanent, err)
			} else if errors.Is(err, ErrPermanent) {
				err = retryActorError{err}
			}

			return RuleError{Rule: rule.Name, Err: err}
		}
	}

	return nil
}

func loadTargets(path string, registry map[string]func(routing.Target) (actor, error)) ([]runnerTarget, error) {
	if path == "" {
		return nil, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read targets file: %w", err)
	}

	config, err := routing.Load(data)
	if err != nil {
		if emptyTargetConfig(data) {
			return nil, nil
		}

		return nil, errTargetsFile
	}

	targets := make([]runnerTarget, 0, len(config.Targets))
	for _, target := range config.Targets {
		create, ok := registry[target.Type]
		if !ok {
			return nil, fmt.Errorf("%w %q", errUnknownTargetType, target.Type)
		}

		actions, createErr := create(target)
		if createErr != nil {
			return nil, fmt.Errorf("configure target %q: %w", target.Name, createErr)
		}

		start := startPosition{all: target.Start.Kind == routing.All, at: nil}
		if target.Start.Kind == routing.Time {
			start.at = &target.Start.Time
		}

		targets = append(targets, runnerTarget{
			Target: ruleTarget{actor: actions, config: target},
			Start:  start, Subjects: target.Subjects(),
		})
	}

	return targets, nil
}

// The routing parser requires targets. Serve also accepts an empty config.
func emptyTargetConfig(data []byte) bool {
	object := sessionJSON[sessionObject](data)
	if object == nil {
		return false
	}

	if len(object) == 0 {
		return true
	}

	entries := sessionJSON[[]json.RawMessage](object["targets"])

	return len(object) == 1 && entries != nil && len(entries) == 0
}

func runWithTargets(
	ctx context.Context,
	store eventStore,
	signals telemetry,
	targets []runnerTarget,
	serveHTTP func(context.Context) error,
) error {
	if len(targets) == 0 {
		return serveHTTP(ctx)
	}

	_, err := openParkedStream(ctx, store.jet)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	//nolint:exhaustruct_v5 // The runner uses the default retry base and a fresh gap counter.
	run := runner{
		stream:      store.stream,
		targets:     targets,
		onPermanent: parkHook(store.jet, signals, 0),
		ready:       make(chan struct{}),
	}
	stopped := make(chan error, 1)

	go func() {
		stopped <- run.run(ctx)

		cancel()
	}()

	select {
	case runErr := <-stopped:
		return runErr
	case <-run.ready:
	}

	// Cancellation can arrive as the runner signals readiness.
	select {
	case <-ctx.Done():
		return <-stopped
	default:
	}

	httpErr := serveHTTP(ctx)

	cancel()

	return errors.Join(httpErr, <-stopped)
}
