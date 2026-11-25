// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestMCPCommand(t *testing.T) {
	checkMCPGatewayLifetime(t, claudeCodeClient)
	checkMCPGatewayLifetime(t, mcpServerName)

	t.Setenv(gatewayURLEnv, "")
	t.Setenv(gatewayTokenEnv, "")

	for _, cancelEarly := range []bool{false, true} {
		reader, writer, err := os.Pipe()
		checkError(t, err)

		stdin := os.Stdin
		os.Stdin = reader //nolint:reassign // Exercise the command's actual stdio transport.

		t.Cleanup(func() { os.Stdin = stdin }) //nolint:reassign // Restore the test process's stdin.

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		t.Cleanup(cancel)

		var want error

		if cancelEarly {
			cancel()
			t.Cleanup(func() { checkError(t, writer.Close()) })

			want = context.Canceled
		} else {
			checkError(t, writer.Close())
		}

		err = run(ctx, io.Discard, slog.DiscardHandler, []string{mcpCommand})
		checkEqual(t, errors.Is(err, want), true)
	}

	err := run(t.Context(), io.Discard, slog.DiscardHandler, []string{mcpCommand, "unexpected"})
	checkEqual(t, errors.Is(err, errMCPUsage), true)
}

func connectMCP(t *testing.T) *mcp.ClientSession {
	t.Helper()

	session, _, _ := connectMCPClient(t, "test", nil)

	return session
}

func connectMCPClient(
	t *testing.T, name string, notifications chan<- *jsonrpc.Request,
) (*mcp.ClientSession, *mcp.ServerSession, *notifyingTransport) {
	t.Helper()

	return connectMCPWithSubscriptions(t, new(mcpSubscriptions), name, notifications)
}

func connectMCPWithSubscriptions(
	t *testing.T, subs *mcpSubscriptions, name string, notifications chan<- *jsonrpc.Request,
) (*mcp.ClientSession, *mcp.ServerSession, *notifyingTransport) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	logger := slog.New(slog.DiscardHandler)
	in, out := mcp.NewInMemoryTransports()
	notifier := &notifyingTransport{inner: in, conn: nil, mu: sync.Mutex{}}
	server := newMCPServer(logger, subs, notifier, nil)
	peer, err := server.Connect(ctx, notifier, nil)
	checkError(t, err)
	t.Cleanup(func() { checkError(t, peer.Close()) })

	//nolint:exhaustruct_v5 // Use SDK defaults for the test client.
	client := mcp.NewClient(&mcp.Implementation{Name: name, Version: version}, &mcp.ClientOptions{Logger: logger})
	session, err := client.Connect(ctx, captureMCPTransport{transport: out, notifications: notifications}, nil)
	checkError(t, err)
	t.Cleanup(func() { checkError(t, session.Close()) })

	return session, peer, notifier
}

type captureMCPTransport struct {
	transport     mcp.Transport
	notifications chan<- *jsonrpc.Request
}

//nolint:ireturn // Implement mcp.Transport.
func (t captureMCPTransport) Connect(ctx context.Context) (mcp.Connection, error) {
	conn, err := t.transport.Connect(ctx)

	return captureMCPConnection{
		Connection:    conn,
		notifications: t.notifications,
	}, err //nolint:wrapcheck // Pass through the SDK error.
}

type captureMCPConnection struct {
	mcp.Connection

	notifications chan<- *jsonrpc.Request
}

//nolint:ireturn // Implement mcp.Connection.
func (c captureMCPConnection) Read(ctx context.Context) (jsonrpc.Message, error) {
	msg, err := c.Connection.Read(ctx)
	if request, ok := msg.(*jsonrpc.Request); ok && c.notifications != nil &&
		request.Method == "notifications/claude/channel" {
		c.notifications <- request
	}

	return msg, err //nolint:wrapcheck // Pass through the SDK error.
}

func TestMCPTools(t *testing.T) {
	t.Parallel()
	session := connectMCP(t)

	init := session.InitializeResult()
	checkEqual(t, init.ServerInfo.Name, "event-gateway")
	checkEqual(t, init.ProtocolVersion, "2025-11-25")
	checkEqual(t, init.Capabilities.Logging != nil, true) //nolint:staticcheck // Check the legacy logging capability.
	checkEqual(t, init.Capabilities.Tools.ListChanged, false)
	capabilities, err := json.Marshal(init.Capabilities.Experimental)
	checkError(t, err)
	checkEqual(t, string(capabilities), `{"claude/channel":{}}`)

	result, err := session.ListTools(t.Context(), nil)
	checkError(t, err)

	names := make([]string, 0, len(result.Tools))
	for _, tool := range result.Tools {
		names = append(names, tool.Name)

		data, marshalErr := json.Marshal(tool.InputSchema)
		checkError(t, marshalErr)

		var schema struct {
			AdditionalProperties *bool                      `json:"additionalProperties"`
			Properties           map[string]json.RawMessage `json:"properties"`
			Type                 string                     `json:"type"`
			Required             []string                   `json:"required"`
		}
		checkError(t, json.Unmarshal(data, &schema))
		checkEqual(t, schema.Type, "object")
		checkEqual(t, schema.AdditionalProperties != nil && !*schema.AdditionalProperties, true)

		switch tool.Name {
		case subscribeTool:
			checkEqual(
				t,
				slices.Equal(schema.Required, []string{channelProviderKey, channelEventKey, channelRepositoryKey}),
				true,
			)
		case unsubscribeTool:
			checkEqual(t, slices.Equal(schema.Required, []string{channelProviderKey}), true)
		case checkDeliveryTool, listSubscriptionsTool:
			checkEqual(t, len(schema.Required), 0)
			checkEqual(t, len(schema.Properties), 0)

			continue
		}

		keys := []string{
			channelProviderKey, channelEventKey, channelRepositoryKey, "action", "conclusion", "name", "workflow_path",
			"number", "label",
		}
		checkEqual(t, len(schema.Properties), len(keys))

		for _, key := range keys {
			checkEqual(t, schema.Properties[key] != nil, true)
		}
	}

	slices.Sort(names)
	checkEqual(
		t,
		slices.Equal(names, []string{checkDeliveryTool, listSubscriptionsTool, subscribeTool, unsubscribeTool}),
		true,
	)
}

func TestMCPCheckDelivery(t *testing.T) {
	t.Parallel()

	const clientName = "claude-code"

	for _, name := range []string{clientName, "test"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			notifications := make(chan *jsonrpc.Request, 3)
			session, _, _ := connectMCPClient(t, name, notifications)
			checkEqual(t, callMCPTool(t, session, checkDeliveryTool, `{"extra":true}`).IsError, true)
			checkEqual(t, callMCPTool(t, session, checkDeliveryTool, "").IsError, name != clientName)

			// The tool response follows any notifications on the same connection.
			if name == clientName {
				checkEqual(t, len(notifications), 2)

				for _, event := range []string{"self_test", "echo"} {
					params := readChannelNotification(t, <-notifications)
					checkEqual(t, params.Meta[channelEventKey], event)
					checkEqual(t, params.Content, `{"message":"Local stdio delivery test"}`)
				}
			}

			checkEqual(t, len(notifications), 0)
		})
	}
}

func readChannelNotification(t *testing.T, req *jsonrpc.Request) channelParameters {
	t.Helper()
	checkEqual(t, req.ID, jsonrpc.ID{})

	var params channelParameters
	checkError(t, json.Unmarshal(req.Params, &params))
	checkEqual(t, params.Meta[channelProviderKey], mcpServerName)

	return params
}

type failingMCPTransport struct{}

//nolint:ireturn // Implement mcp.Transport.
func (failingMCPTransport) Connect(context.Context) (mcp.Connection, error) {
	return nil, io.ErrClosedPipe
}

func TestMCPNotifyWithoutConnection(t *testing.T) {
	t.Parallel()

	notifier := &notifyingTransport{inner: failingMCPTransport{}, conn: nil, mu: sync.Mutex{}}
	checkEqual(t, notifier.Notify(t.Context(), channelMethod, nil), errNoConnection)
	_, err := notifier.Connect(t.Context())
	checkEqual(t, errors.Is(err, io.ErrClosedPipe), true)
	checkEqual(t, notifier.Notify(t.Context(), channelMethod, nil), errNoConnection)
}

func TestMCPChannelBeforeClientName(t *testing.T) {
	t.Parallel()

	var event channelEvent

	for _, init := range []*mcp.InitializeParams{nil, new(mcp.InitializeParams)} {
		in, _ := mcp.NewInMemoryTransports()
		notifier := &notifyingTransport{inner: in, conn: nil, mu: sync.Mutex{}}
		server := newMCPServer(slog.New(slog.DiscardHandler), new(mcpSubscriptions), notifier, nil)
		//nolint:exhaustruct_v5 // Only the client's initialization state matters here.
		opts := &mcp.ServerSessionOptions{State: &mcp.ServerSessionState{InitializeParams: init}}
		peer, err := server.Connect(t.Context(), notifier, opts)
		checkError(t, err)
		checkEqual(t, notifier.deliverChannel(t.Context(), peer, event), errChannelClient)
		checkError(t, peer.Close())
	}
}

func TestMCPChannelParams(t *testing.T) {
	t.Parallel()

	prefix := strings.Repeat("x", 252) + "<>&"
	long := prefix + "é"
	got := channelParams(channelEvent{
		provider: long, event: long, repository: &long, delivery: long,
		payload: json.RawMessage(`{"action":"` + long + `","body":"<>&"}`),
	})
	clipped := strings.Repeat("x", 252) + `\u003c\u003e\u0026`
	checkEqual(t, maps.Equal(got.Meta, map[string]string{
		channelProviderKey: clipped, channelEventKey: clipped, channelRepositoryKey: clipped,
		"delivery": clipped, "action": clipped,
	}), true)
	checkEqual(t, got.Content, `{"action":"`+clipped+`é","body":"\u003c\u003e\u0026"}`)

	for _, payload := range []string{`[]`, `{}`, `{"action":null}`, `{"action":false}`} {
		plain := channelParams(channelEvent{
			provider: "example", event: pushEvent, repository: nil, delivery: "id", payload: json.RawMessage(payload),
		})
		checkEqual(
			t,
			maps.Equal(
				plain.Meta,
				map[string]string{channelProviderKey: "example", channelEventKey: pushEvent, "delivery": "id"},
			),
			true,
		)
		checkEqual(t, plain.Content, payload)
	}
}

func TestMCPChannelContentLimit(t *testing.T) {
	t.Parallel()

	const (
		prefix = `{"action":"opened","body":"`
		suffix = `"}`
	)

	body := strings.Repeat("<", 65536-len(prefix)-len(suffix))
	event := channelEvent{
		provider: "sample", event: "issues", repository: nil, delivery: "id",
		payload: json.RawMessage(prefix + body + suffix),
	}
	got := channelParams(event)
	checkEqual(t, got.Meta["truncated"], "")
	checkEqual(t, got.Content == prefix+strings.Repeat(`\u003c`, len(body))+suffix, true)

	event.payload = append(event.payload, ' ')
	got = channelParams(event)
	checkEqual(t, got.Meta["truncated"], "true")
	checkEqual(t, got.Content, "sample issues (action 'opened'): payload of 65537 bytes omitted.")
}

func TestMCPChannelNotice(t *testing.T) {
	t.Parallel()

	notifications := make(chan *jsonrpc.Request, 1)
	_, peer, notifier := connectMCPClient(t, claudeCodeClient, notifications)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	for _, test := range []struct {
		notice  channelNotice
		content string
	}{
		{gatewayConnectionLost(), `{"level":"warning","text":"Gateway connection lost."}`},
		{gatewayReconnected(), `{"level":"info","text":"Gateway reconnected."}`},
		{gatewayGap(3, 5), `{"level":"warning","text":"Gateway events 3 through 5 are no longer available."}`},
	} {
		checkError(t, notifier.deliverChannel(ctx, peer, test.notice.channelEvent()))

		select {
		case req := <-notifications:
			params := readChannelNotification(t, req)
			checkEqual(t, params.Meta[channelEventKey], "notice")
			checkEqual(t, params.Content, test.content)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func TestMCPSubscriptions(t *testing.T) {
	t.Parallel()
	session := connectMCP(t)

	const (
		scope     = `"provider":"github","event":"issues","repository":"Owner/Repo"`
		filters   = `{"provider":"github","event":"issues","repository":"owner/repo","number":[1,2],"label":["Type: Bug"]}`
		other     = `{"provider":"github","event":"issues","repository":"owner/repo","number":[2]}`
		empty     = `{"subscriptions":[]}`
		changed   = `{"changed":true}`
		unchanged = `{"changed":false}`
	)

	for _, test := range []struct {
		tool, arguments, want string
	}{
		{listSubscriptionsTool, "", empty},
		{subscribeTool, "{" + scope + `,"number":[2,1,1],"label":["Type: Bug"]}`, changed},
		{subscribeTool, filters, unchanged},
		{subscribeTool, other, changed},
		{unsubscribeTool, "{" + scope + "}", unchanged},
		{unsubscribeTool, "{" + scope + `,"number":[1,2],"label":["type: bug"]}`, unchanged},
		{unsubscribeTool, "{" + scope + `,"number":[1],"label":["Type: Bug"]}`, unchanged},
		{listSubscriptionsTool, "", `{"subscriptions":[` + filters + "," + other + "]}"},
		{unsubscribeTool, filters, changed},
		{unsubscribeTool, filters, unchanged},
		{listSubscriptionsTool, "", `{"subscriptions":[` + other + "]}"},
		{unsubscribeTool, other, changed},
		{subscribeTool, `{"provider":"grafana","event":"alert.firing","repository":"owner/repo"}`, changed},
		{unsubscribeTool, `{"provider":"grafana"}`, unchanged},
		{unsubscribeTool, `{"provider":"grafana","event":"alert.firing","repository":"owner/repo"}`, changed},
		{subscribeTool, "{" + scope + `,"number":[1,2,3,4,5,6,7,9007199254740991],` +
			`"label":["A","B","C","D","E","F","G","` + strings.Repeat("z", 128) + `"]}`, changed},
	} {
		got := callMCPTool(t, session, test.tool, test.arguments)
		checkEqual(t, got.IsError, false)
		data, err := json.Marshal(got.StructuredContent)
		checkError(t, err)

		var expected any
		checkError(t, json.Unmarshal([]byte(test.want), &expected))
		want, err := json.Marshal(expected)
		checkError(t, err)
		checkEqual(t, string(data), string(want))
	}
}

func TestMCPRejectsInvalidArguments(t *testing.T) {
	t.Parallel()

	const (
		issue = `{"provider":"github","event":"issues","repository":"owner/repo",`
		push  = `{"provider":"github","event":"push","repository":"owner/repo",`
	)

	session := connectMCP(t)

	for _, arguments := range []string{
		`{}`, `{"provider":"github","event":"issues"}`, `{"provider":"github","repository":"owner/repo"}`,
		issue + `"extra":true}`, issue + `"number":[]}`, issue + `"number":[1,2,3,4,5,6,7,8,9]}`,
		issue + `"number":[0]}`, issue + `"number":[-1]}`, issue + `"number":[1.1]}`,
		issue + `"number":[1.0]}`, issue + `"number":[9007199254740992]}`, push + `"number":[1]}`,
		issue + `"label":[]}`, issue + `"label":["a","b","c","d","e","f","g","h","i"]}`,
		issue + `"label":[""]}`, issue + `"label":["caf\u00e9"]}`,
		issue + `"label":["` + strings.Repeat("z", 129) + `"]}`, push + `"label":["bug"]}`,
	} {
		for _, tool := range []string{subscribeTool, unsubscribeTool} {
			got := callMCPTool(t, session, tool, arguments)
			if !got.IsError {
				t.Errorf("%s accepted %s", tool, arguments)
			}
		}
	}

	checkEqual(t, callMCPTool(t, session, subscribeTool, `{"provider":"grafana"}`).IsError, true)
	checkEqual(t, callMCPTool(t, session, listSubscriptionsTool, `{"extra":true}`).IsError, true)
}

func callMCPTool(t *testing.T, session *mcp.ClientSession, tool, arguments string) *mcp.CallToolResult {
	t.Helper()

	var raw json.RawMessage
	if arguments != "" {
		raw = json.RawMessage(arguments)
	}

	//nolint:exhaustruct_v5 // This test sends only the tool name and arguments.
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: tool, Arguments: raw})
	checkError(t, err)

	return result
}
