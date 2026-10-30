package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
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
	if err := os.WriteFile(path, []byte("upstreams:\n  - name: filesystem\n    command: sh\n"), 0o600); err != nil {
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
	if err := os.WriteFile(path, []byte("upstreams:\n  - name: filesystem\n    command: sh\n"), 0o600); err != nil {
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
	if err := os.WriteFile(configPath, []byte("upstreams:\n  - name: filesystem\n    command: sh\n"), 0o600); err != nil {
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

func TestServeReportsInvalidUpstreamStartupTimeout(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "ctxgate")
	build(t, binary, ".")
	configPath := filepath.Join(t.TempDir(), "ctxgate.yaml")
	config := "upstreams:\n  - name: fake\n    command: sh\n    startup_timeout: shortly\n"
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(binary, "serve", "--config", configPath)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("ctxgate serve succeeded with an invalid startup_timeout")
	}
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
		t.Fatalf("exit = %v, want code 1", err)
	}
	if !strings.Contains(string(output), "config.upstreams[0].startup_timeout") {
		t.Fatalf("output = %q, want field path", output)
	}
}

func TestServeAppliesStartupTimeoutAfterFirstRequest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctxgate.yaml")
	config := "upstreams:\n  - name: quiet\n    command: sh\n    args: [\"-c\", \"cat >/dev/null; sleep 60\"]\n    startup_timeout: 20ms\n"
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	inputReader, inputWriter := io.Pipe()
	go func() {
		_, _ = io.WriteString(inputWriter, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}\n")
	}()

	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"serve", "--config", path}, inputReader, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "startup_timeout") {
		t.Fatalf("run() error = %v, want startup timeout", err)
	}
	_ = inputWriter.Close()
}

func TestServeTerminatesUpstreamWhenAgentStdinCloses(t *testing.T) {
	dir := t.TempDir()
	ctxgate := filepath.Join(dir, "ctxgate")
	build(t, ctxgate, ".")
	fake := filepath.Join(dir, "fake-upstream")
	build(t, fake, "../../internal/testutil/fakeupstream")

	pidPath := filepath.Join(dir, "upstream.pid")
	configPath := filepath.Join(dir, "ctxgate.yaml")
	config := "upstreams:\n  - name: fake\n    command: " + fake + "\n    env:\n      CTXGATE_FAKE_PID_FILE: " + pidPath + "\n"
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(ctxgate, "serve", "--config", configPath)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	pid := waitForPID(t, pidPath)
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("ctxgate serve exit = %v, output = %s", err, output.String())
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("upstream process %d is still alive", pid)
}

func build(t *testing.T, output, pkg string) {
	t.Helper()
	command := exec.Command("go", "build", "-o", output, pkg)
	if result, err := command.CombinedOutput(); err != nil {
		t.Fatalf("go build %s: %v\n%s", pkg, err, result)
	}
}

func waitForPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("upstream did not write %s", path)
	return 0
}
