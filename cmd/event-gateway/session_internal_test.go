// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	pushEvent        = "push"
	pong             = "pong"
	unsupported      = "resume_after and import are not supported yet"
	pushSubscription = `{"provider":"github","event":"push","repository":"example/repo"}`
)

func exerciseSession(t *testing.T, read readAPI, hook *githubWebhook, consumer jetstream.Consumer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), shutdownTimeout)
	defer cancel()

	api := newSessionAPI(read, 1200*time.Millisecond)
	server := httptest.NewServer(http.HandlerFunc(api.connect))

	defer server.Close()

	checkSessionHandshakes(t, api)
	checkClose(ctx, t, dialSession(ctx, t, server.URL), websocket.StatusPolicyViolation, "hello timeout")
	timeout := dialSession(ctx, t, server.URL)
	checkError(t, timeout.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","more":true}`)))
	checkClose(ctx, t, timeout, websocket.StatusPolicyViolation, "hello timeout")

	const body = `{"repository":{"full_name":"example/repo"}}`

	expectStatus(t, hook.ServeHTTP, signedRequest(ctx, pushEvent, "before", body), http.StatusAccepted)
	conn := dialSession(ctx, t, server.URL)

	chunk := `{"type":"hello","more":true,"subscriptions":[` + pushSubscription + `]}`
	checkError(t, conn.Write(ctx, websocket.MessageText, []byte(chunk)))

	for _, trial := range []struct{ send, key, want, id string }{
		{`{"type":"ping"}`, typeKey, pong, ""},
		{`{"type": "ping"}`, typeKey, pong, ""},
		{`{"type":"ping","id":"p1"}`, typeKey, pong, `"p1"`},
		{"{\"type\":\"ping\"}\n", typeKey, pong, ""},
		{`[]`, codeKey, badFrame, ""},
		{`{"type":"hello","subscriptions":[{"provider":"github"}]}`, codeKey, "bad_subscription", ""},
		{`{"type":"hello","resume_after":0}`, "message", unsupported, ""},
		{`{"type":"hello","import":{}}`, "message", unsupported, ""},
		{`{"type":"hello","more":3,"id":"` + strings.Repeat("x", 65) + `"}`, codeKey, badFrame, ""},
		{`{"type":"hello","subscriptions":{}}`, codeKey, badFrame, ""},
		{`{"type":"hello","subscriptions":[` + strings.Repeat(`{},`, subscriptionLimit-1) + `{}]}`, codeKey, "limit", ""},
	} {
		checkEqual(t, string(checkSessionReply(ctx, t, conn, trial.send, trial.key, trial.want)["id"]), trial.id)
	}

	expectStatus(t, hook.ServeHTTP, signedRequest(ctx, "issues", "filtered", body), http.StatusAccepted)
	expectStatus(t, hook.ServeHTTP, signedRequest(ctx, pushEvent, "between", body), http.StatusAccepted)

	checkSessionReply(ctx, t, conn, `{"type":"hello","id":"ready","more":false}`, "id", "ready")
	checkSessionReply(ctx, t, conn, "", "seq", "16")

	for _, kind := range []websocket.MessageType{websocket.MessageText, websocket.MessageBinary} {
		checkError(t, conn.Write(ctx, kind, []byte(`{"type":"hello"}`)))
		checkSessionReply(ctx, t, conn, "", codeKey, badFrame)
	}

	other := dialSession(ctx, t, server.URL)
	checkError(t, other.Write(ctx, websocket.MessageText, []byte(strings.Repeat("x", 4097))))
	checkClose(ctx, t, other, websocket.StatusMessageTooBig, "")

	idle := dialSession(ctx, t, server.URL)
	checkError(t, os.WriteFile(api.tokens.path, nil, 0o600))
	api.tokens.reload(ctx, api.telemetry.logger)
	expectStatus(t, hook.ServeHTTP, signedRequest(ctx, pushEvent, "revoked", body), http.StatusAccepted)
	checkClose(ctx, t, conn, tokenRevoked, "token revoked")
	checkClose(ctx, t, idle, tokenRevoked, "token revoked")
	writeReadTokens(t, api.tokens.path, "reader", readFixture)
	api.tokens.reload(ctx, api.telemetry.logger)

	socket := dialSession(ctx, t, server.URL)

	close(api.stop)
	checkClose(ctx, t, socket, websocket.StatusGoingAway, "server shutdown")

	read.store.jet.Conn().Close()

	peer := session{stream: read.store.stream, conn: nil, events: nil, subscriptions: nil}
	checkEqual(t, peer.hello(ctx, nil)["message"], "stream unavailable")

	read.store.stream = readFetchStream{Stream: read.store.stream, consumer: consumer}
	failed := httptest.NewServer(http.HandlerFunc(newSessionAPI(read, helloTimeout).connect))

	defer failed.Close()

	socket = dialSession(ctx, t, failed.URL)
	checkSessionReply(ctx, t, socket, `{"type":"hello"}`, typeKey, helloKind)
	checkClose(ctx, t, socket, websocket.StatusInternalError, "delivery failed")
}

func checkSessionHandshakes(t *testing.T, api *sessionAPI) {
	t.Helper()

	const bearer = "Bearer " + readFixture

	upgrade := http.Header{authorization: {bearer}, "Connection": {"upgrade"}, upgradeHeader: {"websocket"}}
	protocol := upgrade.Clone()
	protocol.Set("Sec-WebSocket-Protocol", sessionProtocol)

	for _, trial := range []struct {
		headers      http.Header
		path, method string
		status       int
	}{
		{nil, "/other", http.MethodGet, http.StatusNotFound},
		{nil, sessionPath, http.MethodHead, http.StatusMethodNotAllowed},
		{nil, sessionPath, http.MethodGet, http.StatusUnauthorized},
		{http.Header{authorization: {"Bearer wrong"}}, sessionPath, http.MethodGet, http.StatusUnauthorized},
		{http.Header{authorization: {bearer}}, sessionPath, http.MethodGet, http.StatusUpgradeRequired},
		{http.Header{authorization: {bearer}, "Connection": {upgradeHeader}}, sessionPath, http.MethodGet, 426},
		{upgrade, sessionPath, http.MethodGet, http.StatusBadRequest},
		{protocol, sessionPath, http.MethodGet, http.StatusBadRequest},
	} {
		req := httptest.NewRequestWithContext(t.Context(), trial.method, trial.path, nil)
		req.Header = trial.headers
		expectStatus(t, api.connect, req, trial.status)
	}
}

func checkClose(ctx context.Context, t *testing.T, conn *websocket.Conn, code websocket.StatusCode, reason string) {
	t.Helper()

	_, _, err := conn.Read(ctx)
	checkEqual(t, websocket.CloseStatus(err), code)
	checkEqual(t, strings.Contains(err.Error(), reason), true)
}

func dialSession(ctx context.Context, t *testing.T, url string) *websocket.Conn {
	t.Helper()
	//nolint:exhaustruct_v5,bodyclose // A successful upgrade transfers ownership to the socket.
	conn, _, err := websocket.Dial(ctx, url+sessionPath, &websocket.DialOptions{
		HTTPHeader: http.Header{authorization: {"Bearer " + readFixture}}, Subprotocols: []string{sessionProtocol},
	})
	checkError(t, err)
	checkEqual(t, conn.Subprotocol(), sessionProtocol)
	//nolint:errcheck // The server may have already closed the socket.
	t.Cleanup(func() { _ = conn.CloseNow() })

	return conn
}

func checkSessionReply(ctx context.Context, t *testing.T, conn *websocket.Conn, send, key, want string) sessionObject {
	t.Helper()

	if send != "" {
		checkError(t, conn.Write(ctx, websocket.MessageText, []byte(send)))
	}

	_, data, err := conn.Read(ctx)
	checkError(t, err)

	reply := sessionJSON[sessionObject](data)
	checkEqual(t, strings.Trim(string(reply[key]), `"`), want)

	return reply
}
