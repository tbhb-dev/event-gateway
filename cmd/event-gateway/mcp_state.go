// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

// cspell:words awrel

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"go.tbhb.dev/event-gateway/internal/filter"
)

const (
	mcpSessionKeyEnv     = "EVENT_GATEWAY_SESSION_KEY"
	awrelSessionEnv      = "AWREL_SESSION_ID"
	claudeCodeSessionEnv = "CLAUDE_CODE_SESSION_ID"
	mcpStateHomeEnv      = "XDG_STATE_HOME"
	mcpStateHomeWarning  = "MCP subscription state directory is unavailable."
	mcpStateReadWarning  = "MCP subscription state could not be fully loaded."
	mcpStateWriteWarning = "MCP subscription state could not be saved."
	mcpStateVersion      = 1
	mcpStateDirMode      = 0o700
	mcpStateFileMode     = 0o600
)

type mcpState struct {
	logger *slog.Logger
	dir    string
	file   string
}

// loadMCPSubscriptions restores the saved set before the gateway sends hello.
// An unset session key leaves subscriptions in memory only.
func loadMCPSubscriptions(ctx context.Context, logger *slog.Logger) *mcpSubscriptions {
	subs := new(mcpSubscriptions)

	key := cmp.Or(os.Getenv(mcpSessionKeyEnv), os.Getenv(awrelSessionEnv), os.Getenv(claudeCodeSessionEnv))
	if key == "" {
		return subs
	}

	dir := os.Getenv(mcpStateHomeEnv)
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(os.Getenv("HOME"), ".local", "state")
	}

	if !filepath.IsAbs(dir) {
		logger.WarnContext(ctx, mcpStateHomeWarning)

		return subs
	}

	state := mcpState{
		logger: logger,
		dir:    dir,
		file:   filepath.Join(mcpServerName, "sessions", fmt.Sprintf("%x.json", sha256.Sum256([]byte(key)))),
	}
	if !state.load(subs) {
		logger.WarnContext(ctx, mcpStateReadWarning)
	}

	subs.state = state

	return subs
}

func (state mcpState) load(subs *mcpSubscriptions) bool {
	root, err := os.OpenRoot(state.dir)
	if err != nil {
		return false
	}
	defer root.Close()

	data, err := root.ReadFile(state.file)
	if err != nil {
		return false
	}

	var saved struct {
		Subscriptions []json.RawMessage `json:"subscriptions"`
		Version       int               `json:"version"`
	}

	if json.Unmarshal(data, &saved) != nil || saved.Version != mcpStateVersion || saved.Subscriptions == nil {
		return false
	}

	valid := true

	for _, raw := range saved.Subscriptions {
		sub, parseErr := filter.Parse(raw)
		if parseErr != nil {
			valid = false

			continue
		}

		subs.set(sub, true)
	}

	return valid
}

func (state mcpState) write(subs []filter.Subscription) bool {
	saved := struct {
		Subscriptions []filter.Subscription `json:"subscriptions"`
		Version       int                   `json:"version"`
	}{Version: mcpStateVersion, Subscriptions: subs}
	//nolint:errcheck,errchkjson // The state contains only integers, strings and slices of those types.
	data, _ := json.Marshal(saved)

	//nolint:errcheck // Opening the root checks whether directory creation succeeded.
	_ = os.MkdirAll(state.dir, mcpStateDirMode)

	root, err := os.OpenRoot(state.dir)
	if err != nil {
		return false
	}
	defer root.Close()

	dir := filepath.Dir(state.file)
	//nolint:errcheck // Creating the temp file checks whether directory creation succeeded.
	_ = root.MkdirAll(dir, mcpStateDirMode)
	temp := filepath.Join(dir, "subscriptions-"+rand.Text())

	file, err := root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mcpStateFileMode)
	if err != nil {
		return false
	}

	return replaceMCPState(root, file, temp, state.file, data)
}

func replaceMCPState(root *os.Root, file *os.File, temp, target string, data []byte) bool {
	_, err := file.Write(data)
	if errors.Join(err, file.Sync(), file.Close()) == nil && root.Rename(temp, target) == nil {
		return true
	}

	//nolint:errcheck // Cleanup is best effort after a failed write.
	_ = root.Remove(temp)

	return false
}
