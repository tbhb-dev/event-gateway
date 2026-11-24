// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/nats-io/nats.go/jetstream"

	"go.tbhb.dev/event-gateway/internal/filter"
)

const (
	sessionProtocol   = "gateway.v1"
	sessionPath       = "/connect"
	upgradeHeader     = "Upgrade"
	codeKey           = "code"
	subscriptionLimit = 256
	helloTimeout      = 10 * time.Second
	tokenRevoked      = websocket.StatusCode(4401)
	badFrame          = "bad_message"
	helloKind         = "hello"
	typeKey           = "type"
	subsKey           = "subscriptions"
	idLimit           = 64
)

type sessionAPI struct {
	readAPI

	stop      chan struct{}
	active    sync.WaitGroup
	helloWait time.Duration
}

func newSessionAPI(api readAPI, wait time.Duration) *sessionAPI {
	//nolint:exhaustruct_v5 // The connection wait group starts empty.
	return &sessionAPI{readAPI: api, stop: make(chan struct{}), helloWait: wait}
}

func (api *sessionAPI) connect(writer http.ResponseWriter, req *http.Request) {
	header := req.Header.Get("Authorization")

	name := api.tokens.authenticate(header)
	switch {
	case req.URL.Path != sessionPath:
		writer.WriteHeader(http.StatusNotFound)
	case req.Method != http.MethodGet:
		writer.Header().Set("Allow", http.MethodGet)
		writer.WriteHeader(http.StatusMethodNotAllowed)
	case name == "":
		writer.Header().Set("WWW-Authenticate", "Bearer")
		writer.WriteHeader(http.StatusUnauthorized)
	case !offers(req, "Connection", upgradeHeader), !offers(req, upgradeHeader, "websocket"):
		writer.Header().Set(upgradeHeader, "websocket")
		writer.WriteHeader(http.StatusUpgradeRequired)
	case !offers(req, "Sec-WebSocket-Protocol", sessionProtocol):
		writer.WriteHeader(http.StatusBadRequest)
	default:
		api.active.Add(1)
		defer api.active.Done()
		//nolint:exhaustruct_v5 // Keep the default origin checks.
		conn, err := websocket.Accept(writer, req, &websocket.AcceptOptions{Subprotocols: []string{sessionProtocol}})
		if err != nil {
			return
		}

		//nolint:mnd // The protocol caps every client frame at 4 KiB.
		conn.SetReadLimit(4096)

		api.telemetry.logger.InfoContext(req.Context(), "session connected", "token.name", name)

		state := session{conn: conn, stream: api.store.stream, events: make(chan jetstream.Msg), subscriptions: nil}
		code, reason := api.run(req.Context(), &state, sha256.Sum256([]byte(strings.Fields(header)[1])))
		_ = conn.Close(code, reason)
	}
}

func offers(req *http.Request, key, value string) bool {
	return slices.ContainsFunc(strings.Split(strings.Join(req.Header.Values(key), ","), ","), func(item string) bool {
		item = strings.TrimSpace(item)

		return item == value || key != "Sec-WebSocket-Protocol" && strings.EqualFold(item, value)
	})
}

type session struct {
	stream        jetstream.Stream
	conn          *websocket.Conn
	events        chan jetstream.Msg
	subscriptions []filter.Subscription
}

type sessionFrame struct {
	err  error
	data []byte
	kind websocket.MessageType
}

func readSession(ctx context.Context, conn *websocket.Conn, frames chan<- sessionFrame) {
	for {
		kind, data, err := conn.Read(context.WithoutCancel(ctx))
		select {
		case frames <- sessionFrame{kind: kind, data: data, err: err}:
		case <-ctx.Done():
			return
		}
	}
}

//nolint:gocognit // The select serializes commands, lifecycle signals and event writes.
func (api *sessionAPI) run(ctx context.Context, peer *session, hash [sha256.Size]byte) (websocket.StatusCode, string) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()

	frames := make(chan sessionFrame)
	go readSession(ctx, peer.conn, frames)

	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	var deadline <-chan time.Time

	var live <-chan jetstream.Msg

	for {
		var err error

		select {
		case <-api.stop:
			return websocket.StatusGoingAway, "server shutdown"
		case <-deadline:
			return websocket.StatusPolicyViolation, "hello timeout"
		case <-tick.C:
			if (*api.tokens.set.Load())[hash] == "" {
				return tokenRevoked, "token revoked"
			}
		case frame := <-frames:
			if frame.err != nil {
				return websocket.CloseStatus(frame.err), ""
			}

			reply := peer.command(ctx, frame, deadline == nil, live == nil)
			switch {
			case reply[typeKey] == helloKind:
				deadline = nil
				live = peer.events
			case deadline == nil && reply == nil:
				deadline = time.After(api.helloWait)
			}

			if reply != nil {
				err = peer.write(ctx, reply)
			}
		case event := <-live:
			err = peer.deliver(ctx, event)
		}

		if err != nil {
			return websocket.StatusInternalError, "delivery failed"
		}
	}
}

// sessionReply sends the subscription count for hello and echoes valid request IDs.
type sessionReply map[string]any

func (s *session) command(ctx context.Context, frame sessionFrame, first, waiting bool) sessionReply {
	if frame.kind == websocket.MessageText && string(frame.data) == `{"type":"ping"}` {
		return sessionReply{typeKey: "pong"}
	}

	object := sessionJSON[map[string]json.RawMessage](frame.data)
	reply := errorReply(badFrame, "unexpected message")

	if frame.kind == websocket.MessageText && sessionJSON[string](object[typeKey]) == helloKind && waiting {
		reply = s.hello(ctx, object, first)
	}

	id := sessionJSON[string](object["id"])
	if reply != nil && id != "" && len([]rune(id)) <= idLimit {
		reply["id"] = id
	}

	return reply
}

//nolint:ireturn // The caller chooses the concrete JSON type.
func sessionJSON[T any](raw json.RawMessage) T {
	var value T
	//nolint:errcheck // The command's scalar and raw JSON fields decode to zero on invalid input.
	_ = json.Unmarshal(raw, &value)

	return value
}

func errorReply(code, msg string) sessionReply {
	return sessionReply{typeKey: "error", codeKey: code, "message": msg}
}

func (s *session) hello(ctx context.Context, object map[string]json.RawMessage, first bool) sessionReply {
	more := string(object["more"])
	raw := sessionJSON[[]json.RawMessage](object[subsKey])

	switch {
	case object["resume_after"] != nil || object["import"] != nil:
		return errorReply(badFrame, "resume_after and import are not supported yet")
	case more != "" && more != "true" && more != "false", object[subsKey] != nil && raw == nil:
		return errorReply(badFrame, "invalid hello fields")
	case len(s.subscriptions)+len(raw) > subscriptionLimit:
		return errorReply("limit", "too many subscriptions")
	}

	subs := s.subscriptions

	for _, item := range raw {
		sub, err := filter.Parse(item)
		if err != nil {
			//nolint:errcheck,errorlint,forcetypeassert // filter.Parse returns only unwrapped protocol errors.
			failure := err.(*filter.Error)

			return errorReply(failure.Code, failure.Message)
		}

		subs = append(subs, sub)
	}

	// Creating the consumer sets the live boundary. The run loop starts
	// receiving its events after the final hello, once all filters are known.
	if first && !s.consume(ctx) {
		return errorReply(badFrame, "stream unavailable")
	}

	s.subscriptions = subs

	if more == "true" {
		return nil
	}

	return sessionReply{typeKey: helloKind, subsKey: len(subs)}
}

func (s *session) consume(ctx context.Context) bool {
	//nolint:exhaustruct_v5 // Use the default ordered consumer configuration.
	consumer, err := s.stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverNewPolicy, InactiveThreshold: shutdownTimeout, MaxResetAttempts: 1,
	})
	if err != nil {
		return false
	}

	send := func(msg jetstream.Msg) {
		select {
		case s.events <- msg:
		case <-ctx.Done():
		}
	}

	pull, err := consumer.Consume(
		send,
		jetstream.ConsumeErrHandler(func(jetstream.ConsumeContext, error) { send(nil) }),
	)
	if err != nil {
		close(s.events)
	} else {
		context.AfterFunc(ctx, pull.Stop)
	}

	return true
}

func (s *session) deliver(ctx context.Context, message jetstream.Msg) error {
	if message == nil {
		return io.EOF
	}

	event := sessionJSON[struct {
		Provider, Event, Repository string
		Payload                     json.RawMessage
	}](message.Data())

	fields := filter.Extract(event.Event, event.Payload)
	for _, sub := range s.subscriptions {
		if sub.Matches(event.Provider, event.Event, event.Repository, fields) {
			//nolint:errcheck // Ordered delivery validates metadata.
			meta, _ := message.Metadata()

			return s.write(ctx, sequenceEnvelope(meta.Sequence.Stream, message.Data()))
		}
	}

	return nil
}

//nolint:wrapcheck // The run loop converts write errors into the protocol close status.
func (s *session) write(ctx context.Context, reply any) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	//nolint:errcheck,errchkjson // Replies and stored envelopes contain only JSON values.
	data, _ := json.Marshal(reply)

	return s.conn.Write(ctx, websocket.MessageText, data)
}
