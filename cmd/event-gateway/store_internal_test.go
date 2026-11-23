// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

func TestEventExpiry(t *testing.T) {
	t.Parallel()

	const (
		maxAge   = 150 * time.Millisecond
		delivery = "expiry"
	)

	ctx := t.Context()
	store := testStore(t, maxAge)
	hook := testWebhook(t, store, defaultBodyCap)

	expectStatus(t, hook.ServeHTTP, signedRequest(ctx, issuesEvent, delivery, "{}"), http.StatusAccepted)

	deadline := time.Now().Add(5 * time.Second)

	for {
		info, err := store.stream.Info(ctx)
		checkError(t, err)

		if info.State.Msgs == 0 {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("event did not expire")
		}

		time.Sleep(10 * time.Millisecond)
	}

	_, err := store.stream.GetMsg(ctx, 1)
	checkEqual(t, errors.Is(err, jetstream.ErrMsgNotFound), true)

	expectStatus(
		t,
		hook.ServeHTTP,
		signedRequest(ctx, issuesEvent, delivery, `{"action":"opened"}`),
		http.StatusAccepted,
	)

	msg, err := store.stream.GetMsg(ctx, 2)
	checkError(t, err)
	checkEqual(t, msg.Sequence, uint64(2))
}

func TestStoreStartupErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := serveConfig{storeDir: dir} //nolint:exhaustruct_v5 // Other server options use defaults.

	options := storeOptions(cfg)
	options.MaxPending = 1

	_, _, err := openStore(t.Context(), options, retention)
	if err == nil {
		t.Fatal("NATS accepted invalid startup options")
	}

	_, _, err = openStore(t.Context(), storeOptions(cfg), time.Nanosecond)
	if err == nil {
		t.Fatal("JetStream accepted an invalid duplicate window")
	}

	srv, err := server.NewServer(storeOptions(cfg))
	checkError(t, err)
	srv.Shutdown()

	_, _, err = connectStore(t.Context(), srv, retention)
	if err == nil {
		t.Fatal("connected to a stopped server")
	}
}

func TestServeRejectsInvalidStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	file := filepath.Join(dir, "storage")
	checkError(t, os.WriteFile(file, []byte(fixtureKey), 0o600))

	err := serve(t.Context(), io.Discard, []string{"-github-secret-file", file, "-store-dir", file})
	checkEqual(t, errors.Is(err, errNATSNotReady), true)
}
