// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	maxPayload    = 6 << 20
	natsReadyWait = 5 * time.Second
	expiryRetry   = 100 * time.Millisecond
	streamName    = "EVENTS"
	bodyHashKey   = "Event-Body-SHA256"
)

var (
	errNATSNotReady = errors.New("embedded NATS did not become ready")
	errBodyConflict = errors.New("delivery ID already has a different body")
)

type eventStore struct {
	jet    jetstream.JetStream
	stream jetstream.Stream
}

func storeOptions(cfg serveConfig) *server.Options {
	//nolint:exhaustruct_v5 // The embedded server uses NATS defaults for the other options.
	return &server.Options{
		JetStream:          true,
		StoreDir:           cfg.storeDir,
		DontListen:         true,
		SyncAlways:         true,
		MaxPayload:         maxPayload,
		JetStreamMaxMemory: cfg.maxMemory,
		JetStreamMaxStore:  cfg.maxStore,
	}
}

func openStore(ctx context.Context, options *server.Options, maxAge time.Duration) (eventStore, func(), error) {
	embedded, err := server.NewServer(options)
	if err != nil {
		return eventStore{}, nil, fmt.Errorf("configure embedded NATS: %w", err)
	}

	go embedded.Start()

	if !embedded.ReadyForConnections(natsReadyWait) {
		embedded.Shutdown()

		return eventStore{}, nil, errNATSNotReady
	}

	return connectStore(ctx, embedded, maxAge)
}

func connectStore(ctx context.Context, embedded *server.Server, maxAge time.Duration) (eventStore, func(), error) {
	conn, err := nats.Connect(nats.DefaultURL, nats.InProcessServer(embedded))
	if err != nil {
		embedded.Shutdown()

		return eventStore{}, nil, fmt.Errorf("connect to embedded NATS: %w", err)
	}

	closeStore := func() {
		conn.Close()
		embedded.Shutdown()
	}

	store, err := openStream(ctx, conn, maxAge)
	if err != nil {
		closeStore()

		return eventStore{}, nil, err
	}

	return store, closeStore, nil
}

func openStream(ctx context.Context, conn *nats.Conn, maxAge time.Duration) (eventStore, error) {
	//nolint:errcheck // JetStream New only returns errors from options.
	jet, _ := jetstream.New(conn)

	//nolint:exhaustruct_v5 // The stream uses JetStream defaults for the other options.
	stream, err := jet.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       streamName,
		Subjects:   []string{"events.>"},
		Storage:    jetstream.FileStorage,
		Replicas:   1,
		MaxAge:     maxAge,
		Duplicates: maxAge,
	})
	if err != nil {
		return eventStore{}, fmt.Errorf("configure EVENTS stream: %w", err)
	}

	return eventStore{jet: jet, stream: stream}, nil
}

func (s eventStore) publish(ctx context.Context, event envelope, hash string) error {
	msg := nats.NewMsg("events.github." + event.Event)
	msg.Data = event.encode()
	msg.Header.Set(nats.MsgIdHdr, "github:"+event.Delivery)
	msg.Header.Set(bodyHashKey, hash)

	err := s.publishChecked(ctx, msg, hash)
	if !errors.Is(err, jetstream.ErrMsgNotFound) {
		return err
	}

	// Retention and duplicate eviction run on separate JetStream timers.
	wait := time.NewTimer(expiryRetry)
	defer wait.Stop()

	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for duplicate expiry: %w", ctx.Err())
	case <-wait.C:
		return s.publishChecked(ctx, msg, hash)
	}
}

func (s eventStore) publishChecked(ctx context.Context, msg *nats.Msg, hash string) error {
	ack, err := s.jet.PublishMsg(ctx, msg, jetstream.WithExpectStream(streamName))
	if err != nil {
		return fmt.Errorf("publish event: %w", err)
	}

	if !ack.Duplicate {
		return nil
	}

	original, err := s.stream.GetMsg(ctx, ack.Sequence)
	if err != nil {
		return fmt.Errorf("read original event: %w", err)
	}

	if original.Header.Get(bodyHashKey) != hash {
		return errBodyConflict
	}

	return nil
}
