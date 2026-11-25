// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.tbhb.dev/event-gateway/internal/filter"
)

const (
	mcpCommand            = "mcp"
	subscribeTool         = "subscribe"
	unsubscribeTool       = "unsubscribe"
	listSubscriptionsTool = "list_subscriptions"
)

var errMCPUsage = errors.New("usage: event-gateway mcp")

func runMCP(ctx context.Context, logHandler slog.Handler, args []string) error {
	if len(args) != 0 {
		return errMCPUsage
	}

	server := newMCPServer(slog.New(logHandler), new(mcpSubscriptions))

	err := server.Run(ctx, &mcp.StdioTransport{MaxLineLength: 0})
	if err != nil {
		return fmt.Errorf("serve MCP: %w", err)
	}

	return nil
}

func newMCPServer(logger *slog.Logger, subs *mcpSubscriptions) *mcp.Server {
	//nolint:exhaustruct_v5 // Keep SDK defaults for unused protocol features.
	server := mcp.NewServer(&mcp.Implementation{Name: "event-gateway", Version: version}, &mcp.ServerOptions{
		Logger: logger,
		Capabilities: &mcp.ServerCapabilities{
			Logging: &mcp.LoggingCapabilities{}, //nolint:staticcheck // Advertise logging for the negotiated protocols.
			Tools:   &mcp.ToolCapabilities{},
		},
		SupportedProtocolVersions: []string{"2025-11-25", "2025-06-18", "2025-03-26"},
	})

	//nolint:exhaustruct_v5 // Only the tool name, description and input schema are needed.
	mcp.AddTool(server, &mcp.Tool{
		Name: subscribeTool, Description: "Start receiving matching webhook events in this session.",
		InputSchema: subscriptionSchema(`["provider","event","repository"]`),
	}, subs.change)
	//nolint:exhaustruct_v5 // Only the tool name, description and input schema are needed.
	mcp.AddTool(server, &mcp.Tool{
		Name: unsubscribeTool, Description: "Stop receiving the events a subscription matched.",
		InputSchema: subscriptionSchema(`["provider"]`),
	}, subs.change)
	//nolint:exhaustruct_v5 // Only the tool name, description and input schema are needed.
	mcp.AddTool(server, &mcp.Tool{
		Name: listSubscriptionsTool, Description: "List this session's subscriptions.",
		InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`),
	}, func(context.Context, *mcp.CallToolRequest, json.RawMessage) (*mcp.CallToolResult, any, error) {
		return nil, map[string][]filter.Subscription{subsKey: subs.list()}, nil
	})

	return server
}

func subscriptionSchema(required string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{
		"type":"object",
		"additionalProperties":false,
		"required":%s,
		"properties":{
			"provider":{"type":"string","pattern":"^[a-z0-9-]{1,32}$"},
			"event":{"type":"string","pattern":"^[a-z0-9_.-]{1,64}$"},
			"repository":{"type":"string","pattern":"^[A-Za-z0-9_.-]{1,39}/[A-Za-z0-9_.-]{1,100}$"},
			"action":{"type":"array","minItems":1,"maxItems":8,"items":{"type":"string","pattern":"^[a-z0-9_.-]{1,64}$"}},
			"conclusion":{"type":"array","minItems":1,"maxItems":8,"items":{"type":"string","pattern":"^[a-z0-9_.-]{1,64}$"}},
			"name":{"type":"string","pattern":"^[\\x20-\\x7e]{1,128}$"},
			"workflow_path":{"type":"string","pattern":"^[A-Za-z0-9_./-]{1,256}$"},
			"number":{"type":"array","minItems":1,"maxItems":8,"items":{"type":"integer","minimum":1}},
			"label":{"type":"array","minItems":1,"maxItems":8,"items":{"type":"string","pattern":"^[\\x20-\\x7e]{1,128}$"}}
		}
	}`, required))
}

type mcpSubscriptions struct {
	subs []filter.Subscription
	mu   sync.Mutex
}

// list snapshots the set. The parsed subscriptions and their fields are immutable.
func (s *mcpSubscriptions) list() []filter.Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]filter.Subscription{}, s.subs...)
}

func (s *mcpSubscriptions) change(
	_ context.Context, req *mcp.CallToolRequest, _ json.RawMessage,
) (*mcp.CallToolResult, any, error) {
	// The SDK checks the schema first. Parse the original arguments so number
	// filters use the same integer decoding as gateway sessions.
	sub, err := filter.Parse(req.Params.Arguments)
	if err != nil {
		return nil, nil, fmt.Errorf("parse subscription: %w", err)
	}

	return nil, map[string]bool{"changed": s.set(sub, req.Params.Name == subscribeTool)}, nil
}

func (s *mcpSubscriptions) set(sub filter.Subscription, add bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	index := slices.IndexFunc(s.subs, func(existing filter.Subscription) bool {
		return reflect.DeepEqual(existing, sub)
	})

	switch {
	case add && index < 0:
		s.subs = append(s.subs, sub)
	case !add && index >= 0:
		s.subs = slices.Delete(s.subs, index, index+1)
	default:
		return false
	}

	return true
}
