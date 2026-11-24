// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/nats-io/nats.go/jetstream"

	"go.tbhb.dev/event-gateway/internal/filter"
)

const (
	sessionProtocol   = "gateway.v1"
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
	stop      <-chan struct{}
	stream    jetstream.Stream
	tokens    *readTokens
	telemetry telemetry
	active    sync.WaitGroup
	helloWait time.Duration
}

func (api *sessionAPI) connect(response http.ResponseWriter, request *http.Request) {
	header := request.Header.Get("Authorization")

	name := api.tokens.authenticate(header)
	switch {
	case request.URL.Path != "/connect":
		response.WriteHeader(http.StatusNotFound)
	case request.Method != http.MethodGet:
		response.Header().Set("Allow", http.MethodGet)
		response.WriteHeader(http.StatusMethodNotAllowed)
	case name == "":
		response.Header().Set("WWW-Authenticate", "Bearer")
		response.WriteHeader(http.StatusUnauthorized)
	case !offers(request, "Connection", "upgrade"), !offers(request, "Upgrade", "websocket"):
		response.Header().Set("Upgrade", "websocket")
		response.WriteHeader(http.StatusUpgradeRequired)
	case !offers(request, "Sec-WebSocket-Protocol", sessionProtocol):
		response.WriteHeader(http.StatusBadRequest)
	default:
		api.accept(response, request, name, sha256.Sum256([]byte(strings.Fields(header)[1])))
	}
}

func offers(request *http.Request, key, value string) bool {
	for item := range strings.SplitSeq(strings.Join(request.Header.Values(key), ","), ",") {
		item = strings.TrimSpace(item)
		if item == value || key != "Sec-WebSocket-Protocol" && strings.EqualFold(item, value) {
			return true
		}
	}

	return false
}

func (api *sessionAPI) accept(resp http.ResponseWriter, req *http.Request, name string, hash [sha256.Size]byte) {
	api.active.Add(1)
	defer api.active.Done()
	//nolint:exhaustruct_v5 // Keep the default origin checks.
	conn, err := websocket.Accept(resp, req, &websocket.AcceptOptions{Subprotocols: []string{sessionProtocol}})
	if err != nil {
		return
	}

	//nolint:mnd // The protocol caps every client frame at 4 KiB.
	conn.SetReadLimit(4096)

	runes := []rune(name)
	name = strconv.QuoteToASCII(string(runes[:min(idLimit, len(runes))]))

	ctx := context.WithoutCancel(req.Context())
	api.telemetry.logger.InfoContext(ctx, "session connected", "token.name", name)

	state := session{conn: conn, subscriptions: []filter.Subscription{}, subjects: nil, start: 0}
	code, reason := api.run(ctx, &state, hash)
	_ = conn.Close(code, reason)
	api.telemetry.logger.InfoContext(ctx, "session closed", "token.name", name,
		"close.code", int(code), "subscriptions", len(state.subscriptions))
}

type session struct {
	conn          *websocket.Conn
	subscriptions []filter.Subscription
	subjects      []string
	start         uint64
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
	ctx, cancel := context.WithCancel(ctx)

	stop := func() {}
	defer func() { stop() }()
	defer cancel()

	frames := make(chan sessionFrame)
	go readSession(ctx, peer.conn, frames)

	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	timer := time.NewTimer(api.helloWait)

	timer.Stop()
	defer timer.Stop()

	events := make(chan jetstream.Msg)

	for {
		healthy := true

		select {
		case <-api.stop:
			return websocket.StatusGoingAway, "server shutdown"
		case <-timer.C:
			return websocket.StatusPolicyViolation, "hello timeout"
		case <-tick.C:
			if (*api.tokens.set.Load())[hash] == "" {
				return tokenRevoked, "token revoked"
			}
		case frame := <-frames:
			if frame.err != nil {
				return websocket.CloseStatus(frame.err), ""
			}

			first := peer.start == 0

			reply := peer.command(ctx, api.stream, frame)
			switch {
			case reply[typeKey] == helloKind:
				timer.Stop()

				stop = peer.consume(ctx, api.stream, events)
			case first && reply == nil:
				timer.Reset(api.helloWait)
			}

			healthy = reply == nil || peer.write(ctx, reply)
		case event := <-events:
			healthy = peer.deliver(ctx, event)
		}

		if !healthy {
			return websocket.StatusInternalError, "delivery failed"
		}
	}
}

// sessionReply sends the subscription count for hello and echoes valid request IDs.
type sessionReply map[string]any

func (s *session) command(ctx context.Context, stream jetstream.Stream, frame sessionFrame) sessionReply {
	if frame.kind == websocket.MessageText && string(frame.data) == `{"type":"ping"}` {
		return sessionReply{typeKey: "pong"}
	}

	object := sessionJSON[map[string]json.RawMessage](frame.data)
	kind := sessionJSON[string](object[typeKey])
	reply := sessionError(badFrame, "unexpected message")

	switch {
	case frame.kind != websocket.MessageText:
	case kind == helloKind && s.subjects == nil:
		reply = s.hello(ctx, stream, object)
	}

	id := sessionJSON[string](object["id"])
	if reply != nil && id != "" && len([]rune(id)) <= idLimit {
		reply["id"] = id
	}

	return reply
}

//nolint:ireturn // The caller chooses the concrete JSON type.
func sessionJSON[T any](raw json.RawMessage) T {
	var value, zero T
	if json.Unmarshal(raw, &value) != nil {
		return zero
	}

	return value
}

func sessionError(code, message string) sessionReply {
	return sessionReply{typeKey: "error", "code": code, "message": message}
}

//nolint:cyclop // Validate and apply the initial hello and subsequent chunks atomically.
func (s *session) hello(ctx context.Context, stream jetstream.Stream, object map[string]json.RawMessage) sessionReply {
	resume := sessionJSON[*uint64](object["resume_after"])
	more := sessionJSON[*bool](object["more"])

	raw := sessionJSON[[]json.RawMessage](object[subsKey])
	switch {
	case object["import"] != nil:
		return sessionError(badFrame, "not supported yet")
	case object["resume_after"] != nil && (resume == nil || s.start != 0),
		object["more"] != nil && more == nil, object[subsKey] != nil && raw == nil:
		return sessionError(badFrame, "invalid hello fields")
	case len(s.subscriptions)+len(raw) > subscriptionLimit:
		return sessionError("limit", "too many subscriptions")
	}

	subs := s.subscriptions

	for _, item := range raw {
		sub, err := filter.Parse(item)
		if err != nil {
			return subscriptionError(err)
		}

		subs = append(subs, sub)
	}

	if s.start == 0 {
		info, err := stream.Info(ctx)
		switch {
		case err != nil:
			return sessionError(badFrame, "stream unavailable")
		case resume != nil:
			s.start = max(*resume, *resume+1)
		default:
			s.start = info.State.LastSeq + 1
		}
	}

	s.subscriptions = subs

	if more != nil && *more {
		return nil
	}

	s.subjects = make([]string, 0, len(subs))

	for _, sub := range subs {
		s.subjects = append(s.subjects, "events."+sub.Provider+".>")
	}

	slices.Sort(s.subjects)
	s.subjects = slices.Compact(s.subjects)

	return sessionReply{typeKey: helloKind, subsKey: len(subs)}
}

func subscriptionError(err error) sessionReply {
	//nolint:errcheck,errorlint,forcetypeassert // filter.Parse returns only unwrapped protocol errors.
	failure := err.(*filter.Error)

	return sessionError(failure.Code, failure.Message)
}

func (s *session) consume(ctx context.Context, stream jetstream.Stream, events chan jetstream.Msg) func() {
	//nolint:exhaustruct_v5 // Use the default ordered consumer configuration.
	consumer, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: s.start,
		FilterSubjects: s.subjects, InactiveThreshold: shutdownTimeout, MaxResetAttempts: 1,
	})
	if err != nil {
		close(events)

		return func() {}
	}

	send := func(msg jetstream.Msg) {
		select {
		case events <- msg:
		case <-ctx.Done():
		}
	}

	pull, err := consumer.Consume(
		send,
		jetstream.ConsumeErrHandler(func(jetstream.ConsumeContext, error) { send(nil) }),
	)
	if err != nil {
		close(events)
	}

	return func() {
		if pull != nil {
			pull.Stop()
		}

		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		//nolint:errcheck // JetStream expires the consumer on deletion failure.
		_ = stream.DeleteConsumer(cleanup, consumer.CachedInfo().Name)
	}
}

func (s *session) deliver(ctx context.Context, message jetstream.Msg) bool {
	if message == nil {
		return false
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

	return true
}

func (s *session) write(ctx context.Context, reply any) bool {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	//nolint:errcheck,errchkjson // Replies and stored envelopes contain only JSON values.
	data, _ := json.Marshal(reply)

	return s.conn.Write(ctx, websocket.MessageText, data) == nil
}
