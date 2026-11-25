// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.tbhb.dev/event-gateway/internal/filter"
)

const mcpFixture = "fixture-mcp-token"

func fakeMCPGateway(
	t *testing.T, status int, handle func(*websocket.Conn),
) (*httptest.Server, *atomic.Int32) {
	t.Helper()

	requests := new(atomic.Int32)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		requests.Add(1)

		if req.Header.Get(authorization) != "Bearer "+mcpFixture ||
			req.Header.Get("Sec-WebSocket-Protocol") != sessionProtocol || req.URL.Path != sessionPath {
			t.Error("gateway handshake did not match")
		}

		if status != http.StatusSwitchingProtocols {
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

	server, requests := fakeMCPGateway(t, http.StatusSwitchingProtocols, func(conn *websocket.Conn) {
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
		func(event channelEvent) { messages = append(messages, channelParams(event)) })
	checkEqual(t, errors.Is(err, errGatewayEnded), true)
	checkEqual(t, requests.Load(), 1)

	want := sessionJSON[[]channelParameters]([]byte(`[
		{"meta":{"provider":"event-gateway","event":"notice","delivery":""},
		"content":"{\"level\":\"warning\",\"text\":\"Gateway events 3 through 5 are no longer available.\"}"},
		{"meta":{"provider":"github","event":"issues","delivery":"id","repository":"owner/repo","action":"opened"},
		"content":"{\"action\":\"opened\",\"body\":\"\\u003c\\u003e\\u0026\"}"}
	]`))
	checkEqual(t, reflect.DeepEqual(messages, want), true)
}

func TestMCPGatewayRejection(t *testing.T) {
	t.Parallel()

	for _, status := range []int{http.StatusSwitchingProtocols, http.StatusUnauthorized, http.StatusForbidden, 0} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			endpoint := "ws://" + mcpFixture + "@example.invalid:invalid"

			if status != 0 {
				server, _ := fakeMCPGateway(t, status, func(conn *websocket.Conn) {
					checkEqual(t, string(receiveGatewayHello(ctx, t, conn)[subsKey]) == "[]", true)
					checkEqual(t, conn.Close(tokenRevoked, mcpFixture) == nil, true)
				})
				endpoint = server.URL + sessionPath
			}

			var notices []channelParameters

			err := runMCPGateway(ctx, slog.New(slog.DiscardHandler), endpoint, mcpFixture, new(mcpSubscriptions).list,
				func(event channelEvent) { notices = append(notices, channelParams(event)) })
			checkEqual(t, strings.Contains(fmt.Sprint(err), mcpFixture), false)
			checkEqual(t, errors.Unwrap(err) == nil, true)

			switch status {
			case http.StatusSwitchingProtocols, http.StatusUnauthorized:
				checkEqual(t, errors.Is(err, errGatewayToken), true)
				checkEqual(t, len(notices), 1)
				checkEqual(t, strings.Contains(fmt.Sprint(notices), mcpFixture), false)
				checkEqual(t, notices[0].Content, `{"level":"warning","text":"Gateway token was rejected."}`)
			default:
				checkEqual(t, errors.Is(err, errGatewayDial), true)
				checkEqual(t, len(notices), 0)
			}
		})
	}
}

func TestMCPGatewayInvalidHello(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	server, _ := fakeMCPGateway(t, http.StatusSwitchingProtocols, func(*websocket.Conn) {})

	for _, provider := range []string{"\xff", strings.Repeat("x", sessionFrameLimit)} {
		var sub filter.Subscription

		sub.Provider = provider
		err := runMCPGateway(ctx, slog.New(slog.DiscardHandler), server.URL+sessionPath, mcpFixture,
			func() []filter.Subscription { return []filter.Subscription{sub} },
			func(channelEvent) { t.Error("invalid hello produced a channel notification") })
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
	server, requests := fakeMCPGateway(t, http.StatusSwitchingProtocols, func(conn *websocket.Conn) {
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
