// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	_ "embed"
)

const pushEvent = "push"

func exerciseSession(t *testing.T, read readAPI, hook *githubWebhook) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), shutdownTimeout)
	defer cancel()
	defer read.tokens.set.Store(read.tokens.set.Load())
	//nolint:exhaustruct_v5 // The connection wait group starts empty.
	api := sessionAPI{
		stop: ctx.Done(), stream: read.store.stream, tokens: read.tokens, telemetry: read.telemetry,
		helloWait: 250 * time.Millisecond,
	}

	server := httptest.NewServer(http.HandlerFunc(api.connect))
	defer server.Close()

	var fixture struct {
		Handshakes []struct {
			Header       http.Header
			Path, Method string
			Status       int
		}
		Commands []struct {
			Key, Want string
			Send      json.RawMessage
		}
	}

	sessionTranscript, err := os.ReadFile("testdata/session.json")
	checkError(t, err)
	//nolint:musttag // The fixture uses the field names directly.
	checkError(t, json.Unmarshal(sessionTranscript, &fixture))

	for _, trial := range fixture.Handshakes {
		request := httptest.NewRequestWithContext(ctx, trial.Method, trial.Path, nil)
		request.Header = trial.Header
		expectStatus(t, api.connect, request, trial.Status)
	}

	const body = `{"repository":{"full_name":"example/repo"}}`
	expectStatus(t, hook.ServeHTTP, signedRequest(ctx, pushEvent, "before", body), http.StatusAccepted)
	conn := dialSession(ctx, t, server.URL)

	for _, trial := range fixture.Commands {
		checkError(t, conn.Write(ctx, websocket.MessageText, trial.Send))

		if trial.Key != "" {
			checkSessionReply(ctx, t, conn, trial.Key, trial.Want)
		}
	}

	checkError(t, conn.Write(ctx, websocket.MessageBinary, []byte("binary")))
	checkSessionReply(ctx, t, conn, "code", badFrame)

	for index, event := range []string{"issues", pushEvent, pushEvent} {
		expectStatus(t, hook.ServeHTTP, signedRequest(ctx, event, string(rune('a'+index)), body), http.StatusAccepted)
	}

	checkSessionReply(ctx, t, conn, "seq", "16")
	checkSessionReply(ctx, t, conn, "seq", "17")

	checkError(t, os.WriteFile(api.tokens.path, nil, 0o600))
	api.tokens.reload(ctx, api.telemetry.logger)
	_, _, err = conn.Read(ctx)
	checkEqual(t, websocket.CloseStatus(err), tokenRevoked)
	api.active.Wait()
}

func dialSession(ctx context.Context, t *testing.T, url string) *websocket.Conn {
	t.Helper()
	//nolint:exhaustruct_v5,bodyclose // A successful upgrade transfers ownership to the socket.
	conn, _, err := websocket.Dial(ctx, url+"/connect", &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer " + readFixture}}, Subprotocols: []string{sessionProtocol},
	})
	checkError(t, err)
	checkEqual(t, conn.Subprotocol(), sessionProtocol)
	//nolint:errcheck // The server may have already closed the socket.
	t.Cleanup(func() { _ = conn.CloseNow() })

	return conn
}

func checkSessionReply(ctx context.Context, t *testing.T, conn *websocket.Conn, key, want string) {
	t.Helper()

	_, data, err := conn.Read(ctx)
	checkError(t, err)

	reply := sessionJSON[map[string]json.RawMessage](data)
	checkEqual(t, strings.Trim(string(reply[key]), `"`), want)
}
