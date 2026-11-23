// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	fixtureKey  = "fixture-signing-key"
	issuesEvent = "issues"
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

func testWebhook(t *testing.T, store eventStore, bodyCap int) *githubWebhook {
	t.Helper()

	signals, err := openTelemetry(t.Context(), io.Discard)
	checkError(t, err)
	t.Cleanup(func() { signals.close(t.Context()) })

	return &githubWebhook{secret: []byte(fixtureKey), bodyCap: bodyCap, store: store, telemetry: signals}
}

func signedRequest(ctx context.Context, event, delivery, body string) *http.Request {
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, webhookPath, strings.NewReader(body))
	req.Header.Set(eventHeader, event)
	req.Header.Set(deliveryHeader, delivery)

	mac := hmac.New(sha256.New, []byte(fixtureKey))
	//nolint:errcheck // The hash.Hash contract guarantees that Write succeeds.
	_, _ = io.WriteString(mac, body)
	req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))

	return req
}

func expectStatus(t *testing.T, h http.HandlerFunc, r *http.Request, status int) {
	t.Helper()

	checkEqual(t, responseStatus(h, r), status)
}

func responseStatus(h http.HandlerFunc, r *http.Request) int {
	w := httptest.NewRecorder()
	h(w, r)

	return w.Code
}

func TestGitHubDeliveryDuplicates(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	store := testStore(t, retention)
	hook := testWebhook(t, store, defaultBodyCap)

	const (
		event    = "pull_request"
		delivery = "delivery-1"
		body     = `{ "repository": {"full_name": "example/repository"}, "action": "opened", "text": "<example>" }`
	)

	expectStatus(t, hook.ServeHTTP, signedRequest(ctx, event, delivery, body), http.StatusAccepted)
	expectStatus(t, hook.ServeHTTP, signedRequest(ctx, event, delivery, body), http.StatusAccepted)
	expectStatus(t, hook.ServeHTTP, signedRequest(ctx, event, delivery, body+" "), http.StatusConflict)

	info, err := store.stream.Info(ctx)
	checkError(t, err)
	checkEqual(t, info.State.Msgs, uint64(1))
	checkEqual(t, info.Config.MaxAge, retention)
	checkEqual(t, info.Config.Duplicates, retention)

	msg, err := store.stream.GetMsg(ctx, info.State.FirstSeq)
	checkError(t, err)

	checkEqual(t, msg.Subject, "events.github.pull_request")
	checkEqual(t, msg.Header.Get(nats.MsgIdHdr), "github:"+delivery)

	hash := sha256.Sum256([]byte(body))
	checkEqual(t, msg.Header.Get(bodyHashKey), hex.EncodeToString(hash[:]))

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
	expectStatus(t, hook.ServeHTTP, signedRequest(ctx, event, delivery, body), http.StatusServiceUnavailable)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	expectStatus(t, hook.ServeHTTP, signedRequest(canceled, event, delivery, body), http.StatusServiceUnavailable)
}

func TestGitHubTruncation(t *testing.T) {
	t.Parallel()

	const (
		projected = `"action":"unlabeled","check_run":{"name":"Build","conclusion":"success"},` +
			`"workflow_run":{"name":"CI","conclusion":"failure","path":".github/workflows/ci.yml"},` +
			`"issue":{"number":9,"labels":[{"name":"area:example"}]},` +
			`"pull_request":{"number":42,"labels":[{"name":"review"}],"head":{"sha":"123abc"}},` +
			`"label":{"name":"removed"}`
		body     = `{` + projected + `,"unneeded":"discard me"}`
		delivery = "truncated"
	)

	store := testStore(t, retention)
	hook := testWebhook(t, store, len(body)-1)
	ctx := t.Context()
	expectStatus(t, hook.ServeHTTP, signedRequest(ctx, "pull_request", delivery, body), http.StatusAccepted)
	expectStatus(t, hook.ServeHTTP, signedRequest(ctx, "pull_request", delivery, body+" "), http.StatusConflict)

	msg, err := store.stream.GetMsg(ctx, 1)
	checkError(t, err)

	var stored map[string]json.RawMessage
	checkError(t, json.Unmarshal(msg.Data, &stored))
	checkEqual(t, string(stored["repository"]), "null")

	var got, want map[string]any
	checkError(t, json.Unmarshal(stored["payload"], &got))
	checkError(t, json.Unmarshal([]byte(`{"truncated":true,`+projected+"}"), &want))

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("projected payload = %v, want %v", got, want)
	}
}

func TestGitHubValidation(t *testing.T) {
	t.Parallel()

	store := testStore(t, retention)
	hook := testWebhook(t, store, defaultBodyCap)
	wrongSignature := "sha256=" + strings.Repeat("0", 64)

	for _, test := range []struct {
		edit                        func(*http.Request)
		name, event, delivery, body string
		status                      int
	}{
		{setSignature(""), "missing signature", issuesEvent, "1", "{", http.StatusUnauthorized},
		{setSignature("sha256=00"), "short signature", issuesEvent, "1", "{}", http.StatusUnauthorized},
		{setSignature("sha256=" + strings.Repeat("g", 64)), "bad hex", issuesEvent, "1", "{}", http.StatusUnauthorized},
		{setSignature(wrongSignature), "wrong signature", issuesEvent, "1", "{", http.StatusUnauthorized},
		{brokenBody, "read error", issuesEvent, "1", "{}", http.StatusBadRequest},
		{oversizeBody, "transport limit", issuesEvent, "1", "{}", http.StatusRequestEntityTooLarge},
		{nil, "bad event", "issue.>", "1", "{}", http.StatusBadRequest},
		{nil, "missing delivery", issuesEvent, "", "{}", http.StatusBadRequest},
		{nil, "malformed signed JSON", issuesEvent, "1", "{", http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := signedRequest(t.Context(), test.event, test.delivery, test.body)
			if test.edit != nil {
				test.edit(req)
			}

			expectStatus(t, hook.ServeHTTP, req, test.status)
		})
	}
}

func TestGitHubPingIsNotStored(t *testing.T) {
	t.Parallel()

	store := testStore(t, retention)
	hook := testWebhook(t, store, defaultBodyCap)
	expectStatus(t, hook.ServeHTTP, signedRequest(t.Context(), "ping", "ping", "{}"), http.StatusAccepted)
	info, err := store.stream.Info(t.Context())
	checkError(t, err)
	checkEqual(t, info.State.Msgs, uint64(0))
}

func TestGitHubTruncationInvalidField(t *testing.T) {
	t.Parallel()

	var store eventStore

	hook := testWebhook(t, store, 1)
	req := signedRequest(t.Context(), issuesEvent, "delivery", `{"issue":{"number":"not an integer"}}`)
	expectStatus(t, hook.ServeHTTP, req, http.StatusBadRequest)
}

func setSignature(signature string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("X-Hub-Signature-256", signature) }
}

var errBrokenBody = errors.New("broken body")

type bodyReadError struct{}

func (bodyReadError) Read([]byte) (int, error) { return 0, errBrokenBody }

func brokenBody(r *http.Request) {
	r.Body = io.NopCloser(bodyReadError{})
}

func oversizeBody(r *http.Request) {
	r.Body = io.NopCloser(bytes.NewReader(make([]byte, maxGitHubBody+1)))
}
