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

func TestLoadRejectsUnknownFields(t *testing.T) {
	_, err := Load(writeConfig(t, `
upstreams:
  - name: filesystem
    command: npx
    commmand: typo
`))
	if err == nil || !strings.Contains(err.Error(), "commmand") {
		t.Fatalf("Load() error = %v, want unknown field name", err)
	}
}

func TestLoadRequiresExactlyOneUpstream(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{name: "missing", yaml: "version: 1\n"},
		{name: "multiple", yaml: "upstreams:\n  - name: one\n    command: one\n  - name: two\n    command: two\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.yaml))
			if err == nil || !strings.Contains(err.Error(), "upstreams") {
				t.Fatalf("Load() error = %v, want upstreams validation error", err)
			}
		})
	}
}

func TestLoadReportsInvalidUpstreamField(t *testing.T) {
	_, err := Load(writeConfig(t, `
upstreams:
  - name: filesystem
    command: npx
    startup_timeout: shortly
`))
	if err == nil || !strings.Contains(err.Error(), "startup_timeout") {
		t.Fatalf("Load() error = %v, want startup_timeout field", err)
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
