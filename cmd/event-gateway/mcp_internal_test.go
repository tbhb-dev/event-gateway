// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

//nolint:paralleltest // The SDK stdio transport reads the process's stdin.
func TestMCPCommand(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	logger := slog.New(slog.DiscardHandler)
	server := newMCPServer(logger, new(mcpSubscriptions))
	in, out := mcp.NewInMemoryTransports()
	peer, err := server.Connect(ctx, in, nil)
	checkError(t, err)
	t.Cleanup(func() { checkError(t, peer.Close()) })

	//nolint:exhaustruct_v5 // Use SDK defaults for the test client.
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: version}, &mcp.ClientOptions{Logger: logger})
	session, err := client.Connect(ctx, out, nil)
	checkError(t, err)
	t.Cleanup(func() { checkError(t, session.Close()) })

	return session
}

func TestMCPTools(t *testing.T) {
	t.Parallel()
	session := connectMCP(t)

	init := session.InitializeResult()
	checkEqual(t, init.ServerInfo.Name, "event-gateway")
	checkEqual(t, init.ProtocolVersion, "2025-11-25")
	checkEqual(t, init.Capabilities.Logging != nil, true) //nolint:staticcheck // Check the legacy logging capability.
	checkEqual(t, init.Capabilities.Tools.ListChanged, false)
	checkEqual(t, len(init.Capabilities.Experimental), 0)

	result, err := session.ListTools(t.Context(), nil)
	checkError(t, err)

	const (
		eventProperty    = "event"
		providerProperty = "provider"
	)

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
			checkEqual(t, slices.Equal(schema.Required, []string{providerProperty, eventProperty, "repository"}), true)
		case unsubscribeTool:
			checkEqual(t, slices.Equal(schema.Required, []string{providerProperty}), true)
		case listSubscriptionsTool:
			checkEqual(t, len(schema.Required), 0)
			checkEqual(t, len(schema.Properties), 0)

			continue
		}

		keys := []string{
			providerProperty, eventProperty, "repository", "action", "conclusion", "name", "workflow_path",
			"number", "label",
		}
		checkEqual(t, len(schema.Properties), len(keys))

		for _, key := range keys {
			checkEqual(t, schema.Properties[key] != nil, true)
		}
	}

	slices.Sort(names)
	checkEqual(t, slices.Equal(names, []string{listSubscriptionsTool, subscribeTool, unsubscribeTool}), true)
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
