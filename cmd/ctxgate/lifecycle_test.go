package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Viniciuscarvalho/ctxgate/internal/testutil/mcpfake"
)

// serve starts the built ctxgate against the built fakeupstream configured by cfg.
func serve(t *testing.T, cfg mcpfake.Config, upstreamYAML string) (*exec.Cmd, *syncBuffer, *syncBuffer, func(string)) {
	t.Helper()
	dir := t.TempDir()
	ctxgate, fake := filepath.Join(dir, "ctxgate"), filepath.Join(dir, "fakeupstream")
	build(t, ctxgate, ".")
	build(t, fake, "../../internal/testutil/fakeupstream")
	configPath := filepath.Join(dir, "ctxgate.yaml")
	if err := os.WriteFile(configPath, []byte("upstreams:\n  - name: fake\n    command: "+fake+"\n"+upstreamYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(ctxgate, "serve", "--config", configPath)
	command.Env = fakeEnv(cfg)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, stderr := &syncBuffer{}, &syncBuffer{}
	command.Stdout, command.Stderr = stdout, stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill() })
	return command, stdout, stderr, func(line string) {
		if line == "" {
			_ = stdin.Close()
			return
		}
		if _, err := stdin.Write([]byte(line + "\n")); err != nil {
			t.Fatal(err)
		}
	}
}

// syncBuffer lets a test read the output while the process is still writing it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func waitExit(t *testing.T, command *exec.Cmd) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("ctxgate serve did not exit within 10s")
	}
	return nil
}

// requireProtocolOnly fails unless every stdout line is a JSON-RPC 2.0 message.
func requireProtocolOnly(t *testing.T, stdout string) []map[string]any {
	t.Helper()
	var messages []map[string]any
	for _, line := range strings.Split(strings.TrimSuffix(stdout, "\n"), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil || m["jsonrpc"] != "2.0" {
			t.Fatalf("stdout carries a non-protocol line %q (stdout = %q)", line, stdout)
		}
		messages = append(messages, m)
	}
	return messages
}

func TestServeKeepsStdoutForProtocolAndLogsOnStderr(t *testing.T) {
	cfg := mcpfake.Config{Tools: []mcpfake.Tool{{Name: "fixed", Text: "ok"}}}
	command, stdout, stderr, send := serve(t, cfg, "")
	send(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(stdout.String(), `"id":1`) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	send("")
	if err := waitExit(t, command); err != nil {
		t.Fatalf("exit = %v, stderr = %s", err, stderr.String())
	}
	if messages := requireProtocolOnly(t, stdout.String()); len(messages) != 1 {
		t.Fatalf("stdout = %q, want exactly the one response", stdout.String())
	}
	if !strings.Contains(stderr.String(), "ctxgate: configured upstream") {
		t.Fatalf("stderr = %q, want the log line", stderr.String())
	}
}

func TestServeExitsNonZeroAndAnswersPendingWhenUpstreamDies(t *testing.T) {
	cfg := mcpfake.Config{Tools: []mcpfake.Tool{
		{Name: "hang", Delay: time.Hour},
		{Name: "crash", Delay: 100 * time.Millisecond, Crash: true},
	}}
	command, stdout, stderr, send := serve(t, cfg, "")
	send(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"hang"}}`)
	send(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"crash"}}`)

	err := waitExit(t, command)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() == 0 {
		t.Fatalf("exit = %v, want a non-zero exit code", err)
	}
	messages := requireProtocolOnly(t, stdout.String())
	if len(messages) != 2 {
		t.Fatalf("stdout = %q, want one error per pending request", stdout.String())
	}
	for _, m := range messages {
		rpcErr, _ := m["error"].(map[string]any)
		if rpcErr == nil || rpcErr["code"] != float64(-32603) {
			t.Fatalf("message %v, want JSON-RPC error -32603", m)
		}
	}
	if strings.Contains(stderr.String(), "panic") || strings.Contains(stderr.String(), "goroutine ") {
		t.Fatalf("stderr shows a panic: %s", stderr.String())
	}
}

func TestServeFailsImmediatelyWhenUpstreamCommandDoesNotExist(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "ctxgate")
	build(t, binary, ".")
	configPath := filepath.Join(t.TempDir(), "ctxgate.yaml")
	if err := os.WriteFile(configPath, []byte("upstreams:\n  - name: ghost\n    command: ctxgate-no-such-command\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	command := exec.Command(binary, "serve", "--config", configPath)
	command.Stdin = strings.NewReader("") // closed: a hang here would be a bug
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()

	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() == 0 {
		t.Fatalf("exit = %v, want a non-zero exit code", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("took %s to report a missing command", elapsed)
	}
	if !strings.Contains(stderr.String(), "ctxgate-no-such-command") || !strings.Contains(stderr.String(), "not found") || stdout.Len() != 0 {
		t.Fatalf("stdout = %q, stderr = %q, want a readable exec error on stderr only", stdout, stderr)
	}
}
