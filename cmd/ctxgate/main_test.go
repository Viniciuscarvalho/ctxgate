package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"--version"}, strings.NewReader(""), &stdout, &stderr); err != nil {
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
	if err := run(ctx, []string{"serve", "--config", path}, strings.NewReader(""), &stdout, &stderr); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "filesystem") {
		t.Fatalf("stdout = %q, stderr = %q", stdout.String(), stderr.String())
	}
}

func TestRunServeReportsConfigurationErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"serve", "--config", "missing.yaml"}, strings.NewReader(""), &stdout, &stderr)
	if err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "config") {
		t.Fatalf("run() error = %v, want config error", err)
	}
}

func TestRunServeStopsCleanlyWhenStdinCloses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctxgate.yaml")
	if err := os.WriteFile(path, []byte("upstreams:\n  - name: filesystem\n    command: npx\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"serve", "--config", path}, strings.NewReader(""), &stdout, &stderr); err != nil {
		t.Fatalf("run() error = %v", err)
	}
}

func TestServeExitsSuccessfullyWhenStdinCloses(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "ctxgate")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, output)
	}

	configPath := filepath.Join(t.TempDir(), "ctxgate.yaml")
	if err := os.WriteFile(configPath, []byte("upstreams:\n  - name: filesystem\n    command: npx\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "serve", "--config", configPath)
	command.Stdin = strings.NewReader("")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("ctxgate serve exit = %v, output = %s", err, output)
	}
}
