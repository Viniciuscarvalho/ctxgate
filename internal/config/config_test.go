package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadAcceptsOneUpstreamAndAppliesStartupTimeout(t *testing.T) {
	path := writeConfig(t, `
version: 1
upstreams:
  - name: filesystem
    command: npx
    args: ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
    env:
      LOG_LEVEL: debug
pipeline:
  enabled: true
  timeout: 200ms
  max_in_flight: 16
  budget_tokens: 6000
  dedup:
    enabled: true
    similarity: 0.9
  rank:
    enabled: true
    k1: 1.2
    b: 0.75
tools:
  search:
    query_args: [query, q]
store:
  path: ctxgate.db
  retention_days: 30
  capture_content: false
metrics:
  listen: 127.0.0.1:9090
`)

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if time.Duration(got.Upstreams[0].StartupTimeout) != 30*time.Second {
		t.Fatalf("StartupTimeout = %s, want 30s", time.Duration(got.Upstreams[0].StartupTimeout))
	}
	if got.Upstreams[0].Command != "npx" || got.Upstreams[0].Env["LOG_LEVEL"] != "debug" {
		t.Fatalf("upstream = %#v", got.Upstreams[0])
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		contains string
	}{
		{name: "invalid YAML", yaml: "upstreams: [\n", contains: "config:"},
		{name: "no upstream", yaml: "version: 1\n", contains: "config.upstreams: exactly one upstream is required, got 0"},
		{name: "unknown field", yaml: "upstreams:\n  - name: filesystem\n    command: npx\n    commmand: typo\n", contains: "commmand"},
		{name: "invalid startup timeout", yaml: "upstreams:\n  - name: filesystem\n    command: npx\n    startup_timeout: 5\n", contains: "config.upstreams[].startup_timeout:"},
		{name: "empty file", yaml: "", contains: "config: file is empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.yaml))
			if err == nil || !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("Load() error = %v, want %q", err, tt.contains)
			}
		})
	}
}

func TestLoadRequiresUpstreamCommand(t *testing.T) {
	_, err := Load(writeConfig(t, `
upstreams:
  - name: filesystem
`))
	if err == nil || !strings.Contains(err.Error(), "upstreams[0].command") {
		t.Fatalf("Load() error = %v, want command field", err)
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ctxgate.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
