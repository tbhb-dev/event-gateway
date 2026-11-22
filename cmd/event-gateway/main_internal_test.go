// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"bytes"
	"testing"
)

func TestRunPrintsVersion(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer

	run(&out)

	if got, want := out.String(), "event-gateway dev\n"; got != want {
		t.Errorf("run() wrote %q, want %q", got, want)
	}
}
