// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

// cspell:words awrel

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"go.tbhb.dev/event-gateway/internal/filter"
)

const (
	mcpStateTestSubscription = `{"provider":"github","event":"issues","repository":"owner/repo","number":[2]}`
	mcpStateTestKey          = "fixture-session"
)

func mcpStateDocument(subscriptions string) string {
	return `{"subscriptions":[` + subscriptions + `],"version":1}`
}

func setMCPStateKeys(t *testing.T, keys [3]string) {
	t.Helper()

	for i, name := range [...]string{mcpSessionKeyEnv, awrelSessionEnv, claudeCodeSessionEnv} {
		t.Setenv(name, keys[i])
	}
}

func TestMCPStateLocation(t *testing.T) {
	dir, home := t.TempDir(), t.TempDir()
	fallback := filepath.Join(home, ".local", "state")

	const awrelKey = "fixture-awrel-session"

	for _, test := range []struct {
		keys                [3]string
		xdg, home, dir, key string
		warning             string
	}{
		{[3]string{"../first", awrelKey, mcpStateTestKey}, dir, home, dir, "../first", mcpStateReadWarning},
		{[3]string{"", awrelKey, mcpStateTestKey}, "relative", home, fallback, awrelKey, mcpStateReadWarning},
		{[3]string{"", "", mcpStateTestKey}, "", home, fallback, mcpStateTestKey, mcpStateReadWarning},
		{[3]string{"", "", mcpStateTestKey}, "relative", "", "", "", mcpStateHomeWarning},
	} {
		t.Setenv(mcpStateHomeEnv, test.xdg)
		t.Setenv("HOME", test.home)
		setMCPStateKeys(t, test.keys)

		var logs bytes.Buffer

		subs := loadMCPSubscriptions(t.Context(), slog.New(slog.NewTextHandler(&logs, nil)))
		checkEqual(t, subs.state.dir, test.dir)

		if test.key != "" {
			want := fmt.Sprintf("%x.json", sha256.Sum256([]byte(test.key)))
			checkEqual(t, subs.state.file, filepath.Join("event-gateway", "sessions", want))
		} else {
			checkEqual(t, subs.state.file, "")
		}

		checkMCPStateLog(t, &logs, test.warning)
	}

	t.Setenv(mcpStateHomeEnv, dir)
	setMCPStateKeys(t, [3]string{})

	sub, err := filter.Parse([]byte(mcpStateTestSubscription))
	checkError(t, err)

	var logs bytes.Buffer

	subs := loadMCPSubscriptions(t.Context(), slog.New(slog.NewTextHandler(&logs, nil)))
	checkMCPStateLog(t, &logs, "")
	checkEqual(t, subs.state.file, "")
	checkEqual(t, subs.set(sub, true), true)
	checkEqual(t, len(subs.list()), 1)

	entries, err := os.ReadDir(dir)
	checkError(t, err)
	checkEqual(t, len(entries), 0)
}

func TestMCPStateLoad(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(mcpStateHomeEnv, dir)
	setMCPStateKeys(t, [3]string{mcpStateTestKey, "", ""})

	empty := loadMCPSubscriptions(t.Context(), slog.New(slog.DiscardHandler))
	path := filepath.Join(dir, empty.state.file)
	checkError(t, os.MkdirAll(path, 0o700))

	var logs bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&logs, nil))
	checkEqual(t, len(loadMCPSubscriptions(t.Context(), logger).list()), 0)
	checkMCPStateLog(t, &logs, mcpStateReadWarning)
	checkError(t, os.Remove(path))

	mixed := mcpStateTestSubscription + `,{"provider":"bad/provider"},null,` + mcpStateTestSubscription
	for _, test := range []struct {
		data    string
		warning string
		count   int
	}{
		{"", mcpStateReadWarning, 0},
		{`{"version":1,"subscriptions":[` + mcpStateTestSubscription + "]", mcpStateReadWarning, 0},
		{`{"version":2,"subscriptions":[` + mcpStateTestSubscription + "]}", mcpStateReadWarning, 0},
		{`{"version":1,"subscriptions":null}`, mcpStateReadWarning, 0},
		{mcpStateDocument(mcpStateTestSubscription), "", 1},
		{mcpStateDocument(mixed), mcpStateReadWarning, 1},
	} {
		checkError(t, os.WriteFile(path, []byte(test.data), 0o600))
		got := loadMCPSubscriptions(t.Context(), logger)
		checkEqual(t, len(got.list()), test.count)
		checkMCPStateLog(t, &logs, test.warning)
	}
}

func TestMCPStateTools(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	t.Setenv(mcpStateHomeEnv, dir)
	setMCPStateKeys(t, [3]string{mcpStateTestKey, "", ""})
	t.Setenv(gatewayTokenEnv, mcpFixture)
	t.Setenv(gatewayURLEnv, "ws://example.invalid")

	var logs bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&logs, nil))
	subs := loadMCPSubscriptions(t.Context(), logger)
	checkMCPStateLog(t, &logs, mcpStateReadWarning)
	session, _, _ := connectMCPWithSubscriptions(t, subs, "test", nil)
	path := filepath.Join(dir, subs.state.file)

	for _, test := range [][2]string{
		{subscribeTool, mcpStateTestSubscription}, {unsubscribeTool, ""},
	} {
		checkEqual(t, callMCPTool(t, session, test[0], mcpStateTestSubscription).IsError, false)

		data, err := os.ReadFile(path)
		checkError(t, err)
		checkEqual(t, string(data), mcpStateDocument(test[1]))
		checkEqual(t, bytes.Contains(data, []byte(mcpFixture)), false)
	}

	checkMCPStateLog(t, &logs, "")
	checkMCPStateMode(t, path, 0o600)
	checkMCPStateMode(t, filepath.Dir(path), 0o700)
	checkMCPStateMode(t, filepath.Dir(filepath.Dir(path)), 0o700)
	checkMCPStateMode(t, dir, 0o700)

	entries, err := os.ReadDir(filepath.Dir(path))
	checkError(t, err)
	checkEqual(t, len(entries), 1)
}

func TestMCPStateSaveFailure(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(mcpStateHomeEnv, dir)
	setMCPStateKeys(t, [3]string{mcpStateTestKey, "", ""})

	var logs bytes.Buffer

	subs := loadMCPSubscriptions(t.Context(), slog.New(slog.NewTextHandler(&logs, nil)))
	checkMCPStateLog(t, &logs, mcpStateReadWarning)

	path := filepath.Join(dir, subs.state.file)
	blocker := filepath.Join(dir, "blocker")
	checkError(t, os.WriteFile(blocker, nil, 0o600))
	checkEqual(t, (mcpState{logger: nil, dir: blocker, file: subs.state.file}).write(nil), false)

	blocked := mcpState{logger: nil, dir: dir, file: filepath.Join("blocker", filepath.Base(path))}
	checkEqual(t, blocked.write(nil), false)
	checkError(t, os.MkdirAll(path, 0o700))

	session, _, _ := connectMCPWithSubscriptions(t, subs, "test", nil)
	checkEqual(t, callMCPTool(t, session, subscribeTool, mcpStateTestSubscription).IsError, false)
	checkMCPStateLog(t, &logs, mcpStateWriteWarning)
	checkEqual(t, len(subs.list()), 1)
	checkEqual(t, callMCPTool(t, session, listSubscriptionsTool, "").IsError, false)

	entries, err := os.ReadDir(filepath.Dir(path))
	checkError(t, err)
	checkEqual(t, len(entries), 1)
	checkEqual(t, entries[0].IsDir(), true)
	checkError(t, os.Remove(path))
	checkEqual(t, callMCPTool(t, session, unsubscribeTool, mcpStateTestSubscription).IsError, false)
	checkMCPStateLog(t, &logs, "")
}

func TestMCPStateTempWriteFailure(t *testing.T) {
	t.Parallel()

	root, err := os.OpenRoot(t.TempDir())
	checkError(t, err)
	t.Cleanup(func() { checkError(t, root.Close()) })

	const temp = "temp"

	file, err := root.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_RDONLY, 0o600)
	checkError(t, err)
	checkEqual(t, replaceMCPState(root, file, temp, "target", []byte("data")), false)
	_, err = root.Stat(temp)
	checkEqual(t, os.IsNotExist(err), true)
}

func checkMCPStateMode(t *testing.T, path string, mode os.FileMode) {
	t.Helper()

	if runtime.GOOS == "windows" {
		return
	}

	info, err := os.Stat(path)
	checkError(t, err)
	checkEqual(t, info.Mode().Perm(), mode)
}

func checkMCPStateLog(t *testing.T, logs *bytes.Buffer, warning string) {
	t.Helper()

	got := logs.String()
	logs.Reset()

	if warning == "" {
		checkEqual(t, got, "")

		return
	}

	_, message, found := strings.Cut(got, " level=WARN msg=")
	checkEqual(t, found, true)
	checkEqual(t, message, fmt.Sprintf("%q\n", warning))
}
