// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"github.com/coder/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.tbhb.dev/event-gateway/internal/filter"
)

const (
	gatewayURLEnv   = "EVENT_GATEWAY_URL"
	gatewayTokenEnv = "EVENT_GATEWAY_TOKEN"
)

var (
	errGatewayDial  = errors.New("gateway WebSocket handshake failed")
	errGatewayToken = errors.New("gateway token rejected")
	errGatewayEnded = errors.New("gateway WebSocket connection ended")
	errGatewayHello = errors.New("invalid gateway hello subscription")
)

func startMCPGateway(
	ctx context.Context, subscriptions func() []filter.Subscription, notifier *notifyingTransport, logger *slog.Logger,
) (func(context.Context, *mcp.InitializedRequest), func()) {
	endpoint, token := os.Getenv(gatewayURLEnv), os.Getenv(gatewayTokenEnv)
	if endpoint == "" || token == "" {
		return nil, func() {}
	}

	ctx, cancel := context.WithCancel(ctx)

	var worker sync.WaitGroup

	return func(_ context.Context, req *mcp.InitializedRequest) {
			worker.Go(func() {
				err := runMCPGateway(ctx, logger, endpoint, token, subscriptions, func(event channelEvent) {
					deliveryErr := notifier.deliverChannel(ctx, req.Session, event)
					logger.DebugContext(ctx, "gateway notification", "delivered", deliveryErr == nil)
				})
				logger.DebugContext(ctx, "gateway client stopped", "error", err)
			})
		}, func() {
			cancel()
			worker.Wait()
		}
}

// runMCPGateway forwards live gateway events for one connection. It does not
// reconnect. A dropped socket ends the worker while the MCP server keeps running.
func runMCPGateway(
	ctx context.Context,
	logger *slog.Logger,
	endpoint, token string,
	subscriptions func() []filter.Subscription,
	notify func(channelEvent),
) error {
	options := new(websocket.DialOptions)
	options.HTTPHeader = http.Header{"Authorization": {"Bearer " + token}}
	options.Subprotocols = []string{sessionProtocol}

	conn, response, err := websocket.Dial(ctx, endpoint, options)
	if err != nil {
		// The dial error may contain the URL, headers or an untrusted reply.
		if response != nil {
			defer response.Body.Close()

			if response.StatusCode == http.StatusUnauthorized {
				return rejectGatewayToken(notify)
			}
		}

		return errGatewayDial
	}

	closeSocket := sync.OnceFunc(func() {
		closeErr := conn.CloseNow()
		logger.DebugContext(ctx, "gateway socket cleanup", "closed", closeErr == nil)
	})

	stopClose := context.AfterFunc(ctx, closeSocket)
	defer closeSocket()
	defer stopClose()

	// The server may expand each payload byte to a JSON escape.
	conn.SetReadLimit(int64(len(`\u003c`)*maxPayload + sessionFrameLimit))

	frames, err := gatewayHelloFrames(subscriptions())
	if err != nil {
		return err
	}

	for err == nil {
		if len(frames) > 0 {
			err = conn.Write(ctx, websocket.MessageText, frames[0])
			frames = frames[1:]

			continue
		}

		kind, data, readErr := conn.Read(ctx)
		if readErr == nil && kind == websocket.MessageText {
			forwardGatewayFrame(sessionJSON[sessionObject](data), notify)
		}

		err = readErr
	}

	switch {
	case ctx.Err() != nil:
		return nil
	case websocket.CloseStatus(err) == tokenRevoked:
		return rejectGatewayToken(notify)
	default:
		return errGatewayEnded
	}
}

func rejectGatewayToken(notify func(channelEvent)) error {
	notify((channelNotice{Level: channelWarning, Text: "Gateway token was rejected."}).channelEvent())

	return errGatewayToken
}

func gatewayHelloFrames(subscriptions []filter.Subscription) ([][]byte, error) {
	const (
		prefix = `{"type":"hello","subscriptions":[`
		final  = `]}`
		more   = `],"more":true}`
	)

	var frames [][]byte

	frame := []byte(prefix)

	for index, sub := range subscriptions {
		suffix := final
		if index+1 < len(subscriptions) {
			suffix = more
		}

		// Encode without HTML escapes so a valid label list fits in one frame.
		item, err := json.Marshal(sub)
		if err != nil || len(prefix)+len(item)+len(suffix) > sessionFrameLimit {
			return nil, errGatewayHello
		}

		if len(frame)+len(item)+len(suffix) > sessionFrameLimit {
			frames = append(frames, append(bytes.TrimSuffix(frame, []byte(",")), more...))
			frame = []byte(prefix)
		}

		frame = append(append(frame, item...), ',')
	}

	return append(frames, append(bytes.TrimSuffix(frame, []byte(",")), final...)), nil
}

func forwardGatewayFrame(frame sessionObject, notify func(channelEvent)) {
	switch sessionJSON[string](frame[typeKey]) {
	case channelEventKey:
		notify(channelEvent{
			provider:   sessionJSON[string](frame[channelProviderKey]),
			event:      sessionJSON[string](frame[channelEventKey]),
			delivery:   sessionJSON[string](frame["delivery"]),
			repository: sessionJSON[*string](frame[channelRepositoryKey]),
			payload:    frame["payload"],
		})
	case "gap":
		notify(gatewayGap(sessionJSON[uint64](frame["from"]), sessionJSON[uint64](frame["to"])).channelEvent())
	}
}
