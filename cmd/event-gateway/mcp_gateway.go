// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.tbhb.dev/event-gateway/internal/filter"
)

const (
	gatewayURLEnv      = "EVENT_GATEWAY_URL"
	gatewayTokenEnv    = "EVENT_GATEWAY_TOKEN"
	gatewayRetryMin    = time.Second
	gatewayRetryMax    = 30 * time.Second
	gatewayRetryFactor = 2
)

var (
	errGatewayToken = errors.New("gateway token rejected")
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
				}, waitGatewayRetry)
				logger.DebugContext(ctx, "gateway client stopped", "failed", err != nil)
			})
		}, func() {
			cancel()
			worker.Wait()
		}
}

// runMCPGateway forwards live events and reconnects with capped backoff.
// Each connection sends the current subscriptions without requesting replay.
func runMCPGateway(
	ctx context.Context,
	logger *slog.Logger,
	endpoint, token string,
	subscriptions func() []filter.Subscription,
	notify func(channelEvent),
	wait func(context.Context, time.Duration) bool,
) error {
	warnInsecureGateway(ctx, logger, endpoint)

	options := new(websocket.DialOptions)
	options.HTTPHeader = http.Header{"Authorization": {"Bearer " + token}}
	options.Subprotocols = []string{sessionProtocol}
	//nolint:exhaustruct_v5 // Keep the default transport and reject redirects.
	options.HTTPClient = &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	delay := gatewayRetryMin
	ready, recovering := false, false
	receive := func(kind websocket.MessageType, data []byte) {
		ready = true

		if recovering {
			notify(gatewayReconnected().channelEvent())

			recovering = false
		}

		if kind == websocket.MessageText {
			forwardGatewayFrame(sessionJSON[sessionObject](data), notify)
		}
	}

	for ctx.Err() == nil {
		err := runGatewayConnection(ctx, logger, endpoint, options, subscriptions, receive)

		select {
		case <-ctx.Done():
			return nil
		default:
		}

		switch {
		case errors.Is(err, errGatewayToken):
			return rejectGatewayToken(notify)
		case err != nil:
			return err
		}

		if ready {
			notify(gatewayConnectionLost().channelEvent())

			delay = gatewayRetryMin
			ready, recovering = false, true
		}

		logger.DebugContext(ctx, "gateway retry scheduled", "delay_seconds", int(delay/time.Second))

		if !wait(ctx, delay) {
			break
		}

		delay = min(gatewayRetryFactor*delay, gatewayRetryMax)
	}

	return nil
}

func warnInsecureGateway(ctx context.Context, logger *slog.Logger, endpoint string) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return
	}

	switch parsed.Scheme {
	case httpScheme, "ws":
		host := parsed.Hostname()
		if !loopbackHost(host) && !net.ParseIP(host).IsPrivate() {
			logger.WarnContext(ctx, "Gateway connection uses an unencrypted WebSocket.")
		}
	}
}

func waitGatewayRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// runGatewayConnection returns an error only when retrying cannot help.
func runGatewayConnection(
	ctx context.Context,
	logger *slog.Logger,
	endpoint string,
	options *websocket.DialOptions,
	subscriptions func() []filter.Subscription,
	receive func(websocket.MessageType, []byte),
) error {
	conn, response, err := websocket.Dial(ctx, endpoint, options)
	if err != nil {
		// The dial error may contain the URL, headers or an untrusted reply.
		if response != nil {
			defer response.Body.Close()

			if response.StatusCode == http.StatusUnauthorized {
				return errGatewayToken
			}
		}

		return nil
	}

	defer func() {
		logger.DebugContext(ctx, "gateway socket cleanup", "closed", conn.CloseNow() == nil)
	}()

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
		if readErr == nil {
			receive(kind, data)
		}

		err = readErr
	}

	if websocket.CloseStatus(err) == tokenRevoked {
		return errGatewayToken
	}

	return nil
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
