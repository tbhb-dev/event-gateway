// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
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

func TestStoreDeliveryDuplicates(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	store := testStore(t, retention)

	const (
		event    = "pull_request"
		delivery = "delivery-1"
		body     = `{ "repository": {"full_name": "example/repository"}, "action": "opened", "text": "<example>" }`
	)

	repository := "example/repository"
	received := time.Now().UTC()
	message := envelope{
		Type:       "event",
		Provider:   "github",
		Event:      event,
		Delivery:   delivery,
		Repository: &repository,
		ReceivedAt: received.Format(time.RFC3339Nano),
		Payload:    []byte(body),
	}
	hash := sha256.Sum256(message.Payload)
	digest := hex.EncodeToString(hash[:])
	checkError(t, store.publish(ctx, message, digest))
	checkError(t, store.publish(ctx, message, digest))

	changed := sha256.Sum256([]byte(body + " "))
	err := store.publish(ctx, message, hex.EncodeToString(changed[:]))
	checkEqual(t, errors.Is(err, errBodyConflict), true)

	info, err := store.stream.Info(ctx)
	checkError(t, err)
	checkEqual(t, info.State.Msgs, uint64(1))
	checkEqual(t, info.Config.MaxAge, retention)
	checkEqual(t, info.Config.Duplicates, retention)

	msg, err := store.stream.GetMsg(ctx, info.State.FirstSeq)
	checkError(t, err)

	checkEqual(t, msg.Subject, "events.github.pull_request")
	checkEqual(t, msg.Header.Get(nats.MsgIdHdr), "github:"+delivery)

	checkEqual(t, msg.Header.Get(bodyHashKey), digest)

	var stored map[string]json.RawMessage
	checkError(t, json.Unmarshal(msg.Data, &stored))
	checkEqual(t, len(stored), 7)
	checkEqual(t, string(stored["type"]), `"event"`)
	checkEqual(t, string(stored["provider"]), `"github"`)
	checkEqual(t, string(stored["event"]), `"pull_request"`)
	checkEqual(t, string(stored["delivery"]), `"delivery-1"`)
	checkEqual(t, string(stored["repository"]), `"example/repository"`)
	checkEqual(t, string(stored["payload"]), body)

	var receivedAt time.Time
	checkError(t, json.Unmarshal(stored["received_at"], &receivedAt))
	checkEqual(t, receivedAt.Location(), time.UTC)

	checkError(t, store.stream.DeleteMsg(ctx, msg.Sequence))

	err = store.publish(ctx, message, digest)
	checkEqual(t, errors.Is(err, jetstream.ErrMsgNotFound), true)

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	err = store.publish(canceled, message, digest)
	checkEqual(t, errors.Is(err, context.Canceled), true)
}

func TestEventExpiry(t *testing.T) {
	t.Parallel()

	const (
		maxAge   = 150 * time.Millisecond
		delivery = "expiry"
	)

	ctx := t.Context()
	store := testStore(t, maxAge)
	message := envelope{
		Type:       "event",
		Provider:   "github",
		Event:      "issues",
		Delivery:   delivery,
		Repository: nil,
		ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Payload:    []byte("{}"),
	}
	hash := sha256.Sum256(message.Payload)
	checkError(t, store.publish(ctx, message, hex.EncodeToString(hash[:])))

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

	message.Payload = []byte(`{"action":"opened"}`)
	hash = sha256.Sum256(message.Payload)
	checkError(t, store.publish(ctx, message, hex.EncodeToString(hash[:])))

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
