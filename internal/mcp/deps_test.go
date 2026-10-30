package mcp

import (
	"os/exec"
	"strings"
	"testing"
)

func TestMCPDoesNotImportStoreOrMetrics(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, dep := range strings.Fields(string(out)) {
		for _, banned := range []string{"/internal/store", "/internal/metrics"} {
			if strings.HasSuffix(dep, banned) {
				t.Errorf("internal/mcp depends on %s", dep)
			}
		}
	}
}
