// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

// cspell:words synctest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.tbhb.dev/event-gateway/internal/filter"
)

const mcpFixture = "fixture-mcp-token"

func fakeMCPGateway(
	t *testing.T, statuses []int, handle func(*websocket.Conn),
) (*httptest.Server, *atomic.Int32) {
	t.Helper()

	requests := new(atomic.Int32)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		status := statuses[min(int(requests.Add(1))-1, len(statuses)-1)]

		if req.Header.Get(authorization) != "Bearer "+mcpFixture ||
			req.Header.Get("Sec-WebSocket-Protocol") != sessionProtocol || req.URL.Path != sessionPath {
			t.Error("gateway handshake did not match")
		}

		if status != http.StatusSwitchingProtocols {
			if status == http.StatusFound {
				writer.Header().Set("Location", sessionPath+"?redirected")
			}

			http.Error(writer, mcpFixture, status)

			return
		}

		options := new(websocket.AcceptOptions)
		options.Subprotocols = []string{sessionProtocol}

		conn, err := websocket.Accept(writer, req, options)
		if err != nil {
			t.Error("gateway upgrade failed")

			return
		}
		defer func() {
			closeErr := conn.CloseNow()
			if closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
				t.Error("gateway socket cleanup failed")
			}
		}()

		handle(conn)
	}))
	t.Cleanup(server.Close)

	return server, requests
}

func receiveGatewayHello(ctx context.Context, t *testing.T, conn *websocket.Conn) sessionObject {
	t.Helper()

	kind, data, err := conn.Read(ctx)
	checkEqual(t, err == nil, true)
	checkEqual(t, kind, websocket.MessageText)
	checkEqual(t, len(data) <= sessionFrameLimit, true)

	frame := sessionJSON[sessionObject](data)
	checkEqual(t, string(frame[typeKey]) == `"hello"`, true)
	checkEqual(t, frame["resume_after"] == nil, true)
	checkEqual(t, frame["import"] == nil, true)

	return frame
}

func TestMCPGatewayFrames(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	const (
		subscriptionCount = 16
		labelSize         = 128
	)

	subs := make([]filter.Subscription, 0, subscriptionCount)
	for i := range subscriptionCount {
		raw := fmt.Sprintf(`{"provider":"github","event":"issues","repository":"owner/repo%d","label":["%s","%s"]}`,
			i, strings.Repeat("&", labelSize), strings.Repeat("<", labelSize))
		sub, err := filter.Parse([]byte(raw))
		checkError(t, err)

		subs = append(subs, sub)
	}

	checkMCPGatewayHelloBoundary(t, subs[:12])

	server, requests := fakeMCPGateway(t, []int{http.StatusSwitchingProtocols}, func(conn *websocket.Conn) {
		frame := receiveGatewayHello(ctx, t, conn)
		checkEqual(t, string(frame["more"]) == "true", true)
		got := sessionJSON[[]filter.Subscription](frame[subsKey])
		frame = receiveGatewayHello(ctx, t, conn)
		checkEqual(t, frame["more"] == nil, true)
		got = append(got, sessionJSON[[]filter.Subscription](frame[subsKey])...)
		checkEqual(t, reflect.DeepEqual(got, subs), true)

		for _, data := range []string{
			`{"type":"hello","subscriptions":16}`, `{"type":"pong"}`, `{"type":"caught_up","seq":17}`,
			`{"type":"error","code":"bad_message"}`, `{"type":"unknown"}`, `{"type":"gap","from":3,"to":5}`,
		} {
			checkEqual(t, conn.Write(ctx, websocket.MessageText, []byte(data)) == nil, true)
		}

		checkEqual(t, conn.Write(ctx, websocket.MessageBinary, []byte(`{"type":"gap"}`)) == nil, true)

		event := sequenceEnvelope(19, []byte(`{"type":"event","provider":"github","event":"issues","delivery":"id",`+
			`"repository":"owner/repo","payload":{"action":"opened","body":"<>&"}}`))
		checkEqual(t, conn.Write(ctx, websocket.MessageText, event) == nil, true)
	})

	var messages []channelParameters

	err := runMCPGateway(ctx, slog.New(slog.DiscardHandler), server.URL+sessionPath, mcpFixture,
		func() []filter.Subscription { return subs },
		func(event channelEvent) { messages = append(messages, channelParams(event)) },
		func(context.Context, time.Duration) bool {
			cancel()

			return false
		})
	checkEqual(t, err == nil, true)
	checkEqual(t, requests.Load(), 1)

	want := sessionJSON[[]channelParameters]([]byte(`[
		{"meta":{"provider":"event-gateway","event":"notice","delivery":""},
		"content":"{\"level\":\"warning\",\"text\":\"Gateway events 3 through 5 are no longer available.\"}"},
		{"meta":{"provider":"github","event":"issues","delivery":"id","repository":"owner/repo","action":"opened"},
		"content":"{\"action\":\"opened\",\"body\":\"\\u003c\\u003e\\u0026\"}"},
		{"meta":{"provider":"event-gateway","event":"notice","delivery":""},
		"content":"{\"level\":\"warning\",\"text\":\"Gateway connection lost.\"}"}
	]`))
	checkEqual(t, reflect.DeepEqual(messages, want), true)
}

func checkMCPGatewayHelloBoundary(t *testing.T, subscriptions []filter.Subscription) {
	t.Helper()

	subs := slices.Clone(subscriptions)
	subs[0].Repository += "xxxx" // Bring the combined frame from 4092 to 4096 bytes.
	frames, err := gatewayHelloFrames(subs)
	checkEqual(t, err == nil, true)
	checkEqual(t, len(frames), 1)
	checkEqual(t, len(frames[0]), sessionFrameLimit)

	subs[0].Repository += "x"
	frames, err = gatewayHelloFrames(subs)
	checkEqual(t, err == nil, true)
	checkEqual(t, len(frames), 2)

	for _, frame := range frames {
		checkEqual(t, len(frame) <= sessionFrameLimit, true)
	}
}

func TestMCPGatewayReconnect(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	const accept, fail = http.StatusSwitchingProtocols, http.StatusForbidden

	sub, err := filter.Parse([]byte(`{"provider":"github","event":"issues","repository":"owner/repo"}`))
	checkError(t, err)

	var connections atomic.Int32

	statuses := []int{fail, accept, accept, fail, fail, fail, fail, fail, fail, accept, accept}
	server, requests := fakeMCPGateway(t, statuses, func(conn *websocket.Conn) {
		frame := receiveGatewayHello(ctx, t, conn)
		got := sessionJSON[[]filter.Subscription](frame[subsKey])

		switch connections.Add(1) {
		case 1: // Drop before the first frame, so backoff keeps increasing.
			checkEqual(t, len(got), 0)
		case 2:
			hello := []byte(`{"type":"hello","subscriptions":0}`)

			checkEqual(t, len(got), 0)
			checkEqual(t, conn.Write(ctx, websocket.MessageText, hello) == nil, true)
		case 3:
			gap := []byte(`{"type":"gap","from":8,"to":13}`)

			checkEqual(t, reflect.DeepEqual(got, []filter.Subscription{sub}), true)
			checkEqual(t, conn.Write(ctx, websocket.MessageBinary, []byte("ready")) == nil, true)
			checkEqual(t, conn.Write(ctx, websocket.MessageText, gap) == nil, true)
		default:
			checkEqual(t, conn.Close(tokenRevoked, mcpFixture) == nil, true)
		}
	})

	var (
		logs         bytes.Buffer
		subs         mcpSubscriptions
		notices      []channelNotice
		seconds      []time.Duration
		noticeCounts []int
	)

	options := &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: false, ReplaceAttr: nil}
	logger := slog.New(slog.NewTextHandler(&logs, options))
	err = runMCPGateway(ctx, logger, server.URL+sessionPath, mcpFixture, subs.list, func(event channelEvent) {
		notices = append(notices, sessionJSON[channelNotice](event.payload))
	}, func(_ context.Context, delay time.Duration) bool {
		seconds = append(seconds, delay/time.Second)
		noticeCounts = append(noticeCounts, len(notices))

		if len(seconds) == 3 {
			checkEqual(t, subs.set(sub, true), true)
		}

		return len(seconds) <= 10
	})
	checkEqual(t, errors.Is(err, errGatewayToken), true)
	checkEqual(t, requests.Load(), 11)
	checkEqual(t, slices.Equal(seconds, []time.Duration{1, 2, 1, 2, 4, 8, 16, 30, 30, 1}), true)
	checkEqual(t, slices.Equal(noticeCounts, []int{0, 0, 1, 1, 1, 1, 1, 1, 1, 4}), true)
	checkEqual(t, slices.Equal(notices, []channelNotice{
		{Level: channelWarning, Text: "Gateway connection lost."},
		{Level: "info", Text: "Gateway reconnected."},
		{Level: channelWarning, Text: "Gateway events 8 through 13 are no longer available."},
		{Level: channelWarning, Text: "Gateway connection lost."},
		{Level: channelWarning, Text: "Gateway token was rejected."},
	}), true)
	checkEqual(t, strings.Contains(logs.String(), "gateway retry scheduled"), true)
	checkEqual(t, strings.Contains(logs.String(), mcpFixture), false)
}

func TestMCPGatewayCancelBackoff(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		checkEqual(t, waitGatewayRetry(ctx, time.Millisecond), true)

		endpoint := "ws://" + mcpFixture + "@example.invalid:invalid"
		err := runMCPGateway(ctx, slog.New(slog.DiscardHandler), endpoint, mcpFixture, new(mcpSubscriptions).list,
			func(channelEvent) { t.Error("invalid dial produced a channel notification") },
			func(waitCtx context.Context, delay time.Duration) bool {
				checkEqual(t, delay, time.Second)
				time.AfterFunc(time.Millisecond, cancel)

				return waitGatewayRetry(waitCtx, delay)
			})
		checkEqual(t, err == nil, true)
		checkEqual(t, ctx.Err() != nil, true)
		checkEqual(t, runMCPGateway(ctx, slog.New(slog.DiscardHandler), "", mcpFixture, nil, nil, nil) == nil, true)
	})
}

func TestMCPGatewayRedirect(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	server, requests := fakeMCPGateway(t, []int{http.StatusFound, http.StatusUnauthorized}, nil)
	err := runMCPGateway(ctx, slog.New(slog.DiscardHandler), server.URL+sessionPath, mcpFixture,
		new(mcpSubscriptions).list, func(channelEvent) { t.Error("redirect produced a channel notification") },
		func(context.Context, time.Duration) bool { return false })
	checkEqual(t, err == nil, true)
	checkEqual(t, requests.Load(), 1)
}

func TestMCPGatewayInsecureWarning(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		url  string
		warn bool
	}{
		{url: "wss://example.invalid", warn: false},
		{url: "ws://localhost", warn: false},
		{url: "ws://192.168.1.1", warn: false},
		{url: "ws://example.invalid", warn: true},
	} {
		var logs bytes.Buffer

		warnInsecureGateway(t.Context(), slog.New(slog.NewTextHandler(&logs, nil)), test.url)
		checkEqual(t, strings.Contains(logs.String(), "unencrypted WebSocket"), test.warn)
	}
}

func TestMCPGatewayRejection(t *testing.T) {
	t.Parallel()

	for _, status := range []int{http.StatusSwitchingProtocols, http.StatusUnauthorized} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			server, requests := fakeMCPGateway(t, []int{status}, func(conn *websocket.Conn) {
				checkEqual(t, string(receiveGatewayHello(ctx, t, conn)[subsKey]) == "[]", true)
				checkEqual(t, conn.Close(tokenRevoked, mcpFixture) == nil, true)
			})

			var (
				notices []channelParameters
				subs    mcpSubscriptions
			)

			logger := slog.New(slog.DiscardHandler)
			err := runMCPGateway(ctx, logger, server.URL+sessionPath, mcpFixture, subs.list,
				func(event channelEvent) { notices = append(notices, channelParams(event)) },
				func(context.Context, time.Duration) bool {
					t.Error("token rejection scheduled a retry")

					return false
				})
			checkEqual(t, strings.Contains(fmt.Sprint(err), mcpFixture), false)
			checkEqual(t, errors.Unwrap(err) == nil, true)
			checkEqual(t, errors.Is(err, errGatewayToken), true)
			checkEqual(t, requests.Load(), 1)
			checkEqual(t, len(notices), 1)
			checkEqual(t, strings.Contains(fmt.Sprint(notices), mcpFixture), false)
			checkEqual(t, notices[0].Content, `{"level":"warning","text":"Gateway token was rejected."}`)
		})
	}
}

func TestMCPGatewayRejectionNotice(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	notifications := make(chan *jsonrpc.Request, 1)
	_, peer, notifier := connectMCPClient(t, claudeCodeClient, notifications)
	server, requests := fakeMCPGateway(t, []int{http.StatusUnauthorized}, nil)
	t.Setenv(gatewayURLEnv, server.URL+sessionPath)
	t.Setenv(gatewayTokenEnv, mcpFixture)

	initialized, stop := startMCPGateway(ctx, new(mcpSubscriptions).list, notifier, slog.New(slog.DiscardHandler))
	defer stop()

	initialized(ctx, &mcp.InitializedRequest{Session: peer, Params: nil, Extra: nil})

	select {
	case req := <-notifications:
		notice := readChannelNotification(t, req)
		checkEqual(t, notice.Meta[channelEventKey], "notice")
		checkEqual(t, notice.Content == `{"level":"warning","text":"Gateway token was rejected."}`, true)
	case <-ctx.Done():
		t.Fatal("gateway rejection notice did not arrive")
	}

	stop()
	checkEqual(t, requests.Load(), 1)
	checkEqual(t, len(notifications), 0)
}

func TestMCPGatewayInvalidHello(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	server, _ := fakeMCPGateway(t, []int{http.StatusSwitchingProtocols}, func(*websocket.Conn) {})

	for _, provider := range []string{"\xff", strings.Repeat("x", sessionFrameLimit)} {
		var sub filter.Subscription

		sub.Provider = provider
		err := runMCPGateway(ctx, slog.New(slog.DiscardHandler), server.URL+sessionPath, mcpFixture,
			func() []filter.Subscription { return []filter.Subscription{sub} },
			func(channelEvent) { t.Error("invalid hello produced a channel notification") }, waitGatewayRetry)
		checkEqual(t, errors.Is(err, errGatewayHello), true)
	}
}

func checkMCPGatewayLifetime(t *testing.T, name string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	notifications := make(chan *jsonrpc.Request, 1)
	_, peer, notifier := connectMCPClient(t, name, notifications)
	logger := slog.New(slog.DiscardHandler)

	for _, test := range [][2]string{{"", ""}, {"ws://example.invalid", ""}, {"", mcpFixture}} {
		t.Setenv(gatewayURLEnv, test[0])
		t.Setenv(gatewayTokenEnv, test[1])

		initialized, stop := startMCPGateway(ctx, nil, notifier, logger)
		checkEqual(t, initialized == nil, true)
		stop()
	}

	disconnected := make(chan struct{})
	server, requests := fakeMCPGateway(t, []int{http.StatusSwitchingProtocols}, func(conn *websocket.Conn) {
		defer close(disconnected)

		checkEqual(t, string(receiveGatewayHello(ctx, t, conn)[subsKey]) == "[]", true)
		checkEqual(t, conn.Write(ctx, websocket.MessageText, []byte(`{"type":"gap","from":3,"to":5}`)) == nil, true)

		if name != claudeCodeClient {
			checkEqual(t, conn.Close(websocket.StatusNormalClosure, "") == nil, true)

			return
		}

		_, _, err := conn.Read(ctx)
		checkEqual(t, err != nil, true)
		checkEqual(t, ctx.Err() == nil, true)
	})
	t.Setenv(gatewayURLEnv, server.URL+sessionPath)
	t.Setenv(gatewayTokenEnv, mcpFixture)

	_, stop := startMCPGateway(ctx, nil, notifier, logger)
	stop()
	checkEqual(t, requests.Load(), 0)

	initialized, stop := startMCPGateway(ctx, new(mcpSubscriptions).list, notifier, logger)
	defer stop()

	initialized(ctx, &mcp.InitializedRequest{Session: peer, Params: nil, Extra: nil})

	if name == claudeCodeClient {
		select {
		case req := <-notifications:
			checkEqual(t, readChannelNotification(t, req).Meta[channelEventKey], "notice")
		case <-ctx.Done():
			t.Fatal("gateway notification did not arrive")
		}

		stop()
	}

	select {
	case <-disconnected:
	case <-ctx.Done():
		t.Fatal("gateway socket stayed open")
	}

	stop()
	checkEqual(t, requests.Load(), 1)
	checkEqual(t, len(notifications), 0)
}
