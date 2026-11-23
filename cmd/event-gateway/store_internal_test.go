// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

func checkError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}

func checkEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()

	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func testStore(t *testing.T, age time.Duration) eventStore {
	t.Helper()

	store, stop, err := openStore(t.Context(), storeOptions(t.TempDir()), age)
	checkError(t, err)
	t.Cleanup(stop)

	return store
}

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

	options := storeOptions(dir)
	options.MaxPending = 1

	_, _, err := openStore(t.Context(), options, retention)
	if err == nil {
		t.Fatal("NATS accepted invalid startup options")
	}

	_, _, err = openStore(t.Context(), storeOptions(dir), time.Nanosecond)
	if err == nil {
		t.Fatal("JetStream accepted an invalid duplicate window")
	}

	srv, err := server.NewServer(storeOptions(dir))
	checkError(t, err)
	srv.Shutdown()

	_, _, err = connectStore(t.Context(), srv, retention)
	if err == nil {
		t.Fatal("connected to a stopped server")
	}
}

func TestStoreRejectsInvalidDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	file := filepath.Join(dir, "storage")
	checkError(t, os.WriteFile(file, []byte("data"), 0o600))

	_, _, err := openStore(t.Context(), storeOptions(file), retention)
	checkEqual(t, errors.Is(err, errNATSNotReady), true)
}
