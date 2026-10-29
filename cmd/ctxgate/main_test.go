package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"--version"}, &stdout, &stderr); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if !strings.HasPrefix(stdout.String(), "ctxgate ") {
		t.Fatalf("stdout = %q, want version", stdout.String())
	}
}

func TestRunServeLogsConfiguredUpstreamAndStopsCleanly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctxgate.yaml")
	if err := os.WriteFile(path, []byte("upstreams:\n  - name: filesystem\n    command: npx\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	if err := run(ctx, []string{"serve", "--config", path}, &stdout, &stderr); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "filesystem") {
		t.Fatalf("stdout = %q, stderr = %q", stdout.String(), stderr.String())
	}
}

func TestRunServeReportsConfigurationErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"serve", "--config", "missing.yaml"}, &stdout, &stderr)
	if err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "config") {
		t.Fatalf("run() error = %v, want config error", err)
	}
}
