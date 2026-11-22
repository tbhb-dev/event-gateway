// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Command event-gateway prints its version. Step 1 of the design adds the
// service.
package main

import (
	"fmt"
	"io"
	"os"
)

// version is the build's version. Release builds don't stamp it yet.
const version = "dev"

func main() {
	run(os.Stdout)
}

// run writes the version line to out.
func run(out io.Writer) {
	fmt.Fprintf(out, "event-gateway %s\n", version)
}
