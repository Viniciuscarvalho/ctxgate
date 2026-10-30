// Command fakeupstream is the stdio MCP upstream used by tests. All behavior
// lives in internal/testutil/mcpfake.
package main

import (
	"fmt"
	"os"

	"github.com/Viniciuscarvalho/ctxgate/internal/testutil/mcpfake"
)

func main() {
	ran, err := mcpfake.RunFromEnv(os.Stdin, os.Stdout)
	if err != nil || !ran {
		if err == nil {
			err = fmt.Errorf("fakeupstream: %s is not set", mcpfake.EnvKey)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
