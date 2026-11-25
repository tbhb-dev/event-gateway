// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package buildloop_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.tbhb.dev/event-gateway/internal/buildloop"
)

const (
	helperArg   = "buildloop-helper"
	modeExit    = "exit"
	modeFlood   = "flood"
	modeSleep   = "sleep"
	modeWD      = "wd"
	missingName = "buildloop-missing-executable"
	bothStreams = "to stdout\nto stderr\n"
)

// TestMain turns the test binary into the command the tests run.
func TestMain(m *testing.M) {
	if len(os.Args) > 2 && os.Args[1] == helperArg {
		os.Exit(helper(os.Args[2], os.Args[3:]))
	}

	os.Exit(m.Run())
}

func helper(mode string, args []string) int {
	switch mode {
	case modeExit:
		_, _ = os.Stdout.WriteString("to stdout\n")
		_, _ = os.Stderr.WriteString("to stderr\n")

		return number(args)
	case modeFlood:
		_, _ = os.Stdout.WriteString(strings.Repeat("x", number(args)))
	case modeSleep:
		time.Sleep(time.Minute)
	case modeWD:
		dir, err := os.Getwd()
		if err != nil {
			return 1
		}

		_, _ = os.Stdout.WriteString(dir)
	}

	return 0
}

func number(args []string) int {
	value, err := strconv.Atoi(args[0])
	if err != nil {
		return 1
	}

	return value
}

func self(t *testing.T) string {
	t.Helper()

	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	return path
}

func helperCommand(t *testing.T, timeout time.Duration, args ...string) buildloop.Command {
	t.Helper()

	return buildloop.Command{
		Executable: self(t),
		Dir:        "",
		Args:       append([]string{helperArg}, args...),
		Timeout:    timeout,
	}
}

func sameDir(t *testing.T, got, want string) bool {
	t.Helper()

	gotInfo, err := os.Stat(got)
	if err != nil {
		t.Fatal(err)
	}

	wantInfo, err := os.Stat(want)
	if err != nil {
		t.Fatal(err)
	}

	return os.SameFile(gotInfo, wantInfo)
}

func TestRun(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	workDir := helperCommand(t, 0, modeWD)
	workDir.Dir = tempDir

	tests := []struct {
		check   func(buildloop.Result) bool
		name    string
		command buildloop.Command
	}{
		{
			name:    "success",
			command: helperCommand(t, 0, modeExit, "0"),
			check:   func(r buildloop.Result) bool { return r.ExitCode == 0 && r.Output == bothStreams },
		},
		{
			name:    "nonzero exit",
			command: helperCommand(t, time.Minute, modeExit, "3"),
			check:   func(r buildloop.Result) bool { return r.ExitCode == 3 && r.Output == bothStreams },
		},
		{
			name:    "output exactly at the cap",
			command: helperCommand(t, 0, modeFlood, strconv.Itoa(buildloop.OutputLimit)),
			check: func(r buildloop.Result) bool {
				return len(r.Output) == buildloop.OutputLimit && !r.Truncated
			},
		},
		{
			name:    "working directory",
			command: workDir,
			check:   func(r buildloop.Result) bool { return sameDir(t, r.Output, tempDir) },
		},
	}

	runner := buildloop.NewRunner(self(t))

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, err := runner.Run(t.Context(), testCase.command)
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}

			if !testCase.check(got) || got.Duration <= 0 {
				t.Errorf("Run() = exit %d, %d bytes, duration %s", got.ExitCode, len(got.Output), got.Duration)
			}
		})
	}
}

func TestRunErrors(t *testing.T) {
	t.Parallel()

	timeout := 200 * time.Millisecond
	tests := []struct {
		check      func(error) bool
		name       string
		command    buildloop.Command
		wantOutput int
	}{
		{
			name:    "timeout",
			command: helperCommand(t, timeout, modeSleep),
			check: func(err error) bool {
				var target *buildloop.TimeoutError

				return errors.As(err, &target) && target.Timeout == timeout && target.Error() != ""
			},
			wantOutput: 0,
		},
		{
			name:    "output cap",
			command: helperCommand(t, 0, modeFlood, strconv.Itoa(buildloop.OutputLimit+10)),
			check: func(err error) bool {
				var target *buildloop.OutputCapError

				return errors.As(err, &target) && target.Limit == buildloop.OutputLimit && target.Error() != ""
			},
			wantOutput: buildloop.OutputLimit,
		},
		{
			name:    "disallowed command",
			command: buildloop.Command{Executable: "sh", Dir: "", Args: []string{"-c", "true"}, Timeout: 0},
			check: func(err error) bool {
				var target *buildloop.NotAllowedError

				return errors.As(err, &target) && target.Executable == "sh" && target.Error() != ""
			},
			wantOutput: 0,
		},
		{
			name:    "allowed name matches only exactly",
			command: buildloop.Command{Executable: "./" + self(t), Dir: "", Args: nil, Timeout: 0},
			check: func(err error) bool {
				var target *buildloop.NotAllowedError

				return errors.As(err, &target)
			},
			wantOutput: 0,
		},
		{
			name:    "command that cannot start",
			command: buildloop.Command{Executable: missingName, Dir: "", Args: nil, Timeout: 0},
			check: func(err error) bool {
				var notAllowed *buildloop.NotAllowedError

				return err != nil && !errors.As(err, &notAllowed)
			},
			wantOutput: 0,
		},
	}

	runner := buildloop.NewRunner(self(t), missingName)

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, err := runner.Run(t.Context(), testCase.command)
			if !testCase.check(err) {
				t.Fatalf("Run() error = %v", err)
			}

			if len(got.Output) != testCase.wantOutput || got.Truncated != (testCase.wantOutput > 0) {
				t.Errorf(
					"Run() = %d bytes, truncated %v, want %d bytes",
					len(got.Output),
					got.Truncated,
					testCase.wantOutput,
				)
			}
		})
	}
}

func TestRunCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := buildloop.NewRunner(self(t)).Run(ctx, helperCommand(t, 0, modeSleep))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run() error = %v, want context.Canceled", err)
	}
}
