//go:build unix

package mcp_test

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Viniciuscarvalho/ctxgate/internal/testutil/mcpfake"
)

// rawSession drives a serveFunc with raw JSON-RPC lines and no SDK in between.
type rawSession struct {
	in    io.WriteCloser
	lines chan string
	done  chan error
}

func startRaw(t *testing.T, serve serveFunc) *rawSession {
	t.Helper()
	agentInR, agentInW := io.Pipe()
	agentOutR, agentOutW := io.Pipe()
	s := &rawSession{in: agentInW, lines: make(chan string, 64), done: make(chan error, 1)}
	go func() {
		s.done <- serve(t.Context(), agentInR, agentOutW)
		_ = agentOutW.Close()
	}()
	go func() {
		reader := bufio.NewReader(agentOutR)
		for {
			line, err := reader.ReadString('\n')
			if line != "" {
				s.lines <- line
			}
			if err != nil {
				close(s.lines)
				return
			}
		}
	}()
	t.Cleanup(func() { _ = agentInW.Close() })
	return s
}

func (s *rawSession) send(t *testing.T, line string) {
	t.Helper()
	if _, err := io.WriteString(s.in, line+"\n"); err != nil {
		t.Fatal(err)
	}
}

func (s *rawSession) read(t *testing.T) string {
	t.Helper()
	select {
	case line, ok := <-s.lines:
		if !ok {
			t.Fatal("agent output closed before the expected line")
		}
		return line
	case <-time.After(5 * time.Second):
		t.Fatal("no line from the relay within 5s")
	}
	return ""
}

func (s *rawSession) wait(t *testing.T, within time.Duration) error {
	t.Helper()
	select {
	case err := <-s.done:
		return err
	case <-time.After(within):
		t.Fatalf("serve did not return within %s", within)
	}
	return nil
}

type rpcMessage struct {
	ID     json.RawMessage `json:"id"`
	Result struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func decode(t *testing.T, line string) rpcMessage {
	t.Helper()
	var m rpcMessage
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("line is not JSON: %q: %v", line, err)
	}
	return m
}

// The upstream answers concurrently, so late-sent calls with short sleeps come
// back first. The test reads the raw replies and proves they really left in the
// reverse of the order they were sent.
func TestRepliesArriveInReverseOrder(t *testing.T) {
	const calls = 6
	cfg := mcpfake.Config{Tools: []mcpfake.Tool{{Name: "echo", Echo: true}}}
	relays := map[string]serveFunc{"memory": memoryRelay(cfg), "subprocess": subprocessRelay(cfg, 10*time.Second)}
	for name, serve := range relays {
		t.Run(name, func(t *testing.T) {
			s := startRaw(t, serve)
			for i := 1; i <= calls; i++ {
				s.send(t, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":"echo","arguments":{"text":"r%d","sleep_ms":%d}}}`, i, i, (calls-i+1)*60))
			}
			for want := calls; want >= 1; want-- {
				m := decode(t, s.read(t))
				if string(m.ID) != strconv.Itoa(want) {
					t.Fatalf("reply order: got id %s where reverse order expects id %d", m.ID, want)
				}
				if len(m.Result.Content) != 1 || m.Result.Content[0].Text != fmt.Sprintf("r%d", want) {
					t.Fatalf("id %d carries %+v", want, m.Result)
				}
			}
		})
	}
}

func TestUpstreamDeathAnswersEveryPendingRequest(t *testing.T) {
	cfg := mcpfake.Config{Tools: []mcpfake.Tool{
		{Name: "hang", Delay: time.Hour},
		{Name: "crash", Delay: 100 * time.Millisecond, Crash: true},
	}}
	s := startRaw(t, subprocessRelay(cfg, 10*time.Second))
	s.send(t, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"hang"}}`)
	s.send(t, `{"jsonrpc":"2.0","id":"two","method":"tools/call","params":{"name":"crash"}}`)

	got := map[string]int{}
	for range 2 {
		m := decode(t, s.read(t))
		if m.Error == nil {
			t.Fatalf("id %s got a result, want an error", m.ID)
		}
		got[string(m.ID)] = m.Error.Code
	}
	if len(got) != 2 || got["1"] != -32603 || got[`"two"`] != -32603 {
		t.Fatalf("errors by id = %v, want -32603 for 1 and \"two\"", got)
	}
	if err := s.wait(t, 5*time.Second); err == nil {
		t.Fatal("serve returned nil after the upstream died, want an error (non-zero exit)")
	}
}

func TestStartupTimeoutAnswersPendingRequestAndStops(t *testing.T) {
	cfg := mcpfake.Config{Tools: []mcpfake.Tool{{Name: "hang", Delay: time.Hour}}}
	s := startRaw(t, subprocessRelay(cfg, 150*time.Millisecond))
	s.send(t, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"hang"}}`)

	m := decode(t, s.read(t))
	if m.Error == nil || m.Error.Code != -32603 {
		t.Fatalf("reply = %+v, want JSON-RPC error -32603", m)
	}
	if n := strings.Count(m.Error.Message, "startup_timeout"); n != 1 {
		t.Fatalf("agent message %q cites startup_timeout %d times, want 1", m.Error.Message, n)
	}
	err := s.wait(t, 5*time.Second)
	if err == nil || strings.Count(err.Error(), "startup_timeout") != 1 {
		t.Fatalf("serve error = %v, want one citation of startup_timeout", err)
	}
}

func TestShutdownTerminatesUpstreamGroupWithoutWaitingForGrace(t *testing.T) {
	cfg, pidFile, mark := lifecycleConfig(t, false)
	elapsed := closeAgentAndMeasure(t, cfg, pidFile, mark)
	if elapsed >= time.Second {
		t.Fatalf("shutdown took %s, a SIGTERM-friendly upstream must not wait for the 2s grace", elapsed)
	}
}

func TestShutdownKillsUpstreamThatIgnoresSIGTERMAfterTwoSeconds(t *testing.T) {
	cfg, pidFile, mark := lifecycleConfig(t, true)
	elapsed := closeAgentAndMeasure(t, cfg, pidFile, mark)
	if elapsed < 2*time.Second || elapsed > 3500*time.Millisecond {
		t.Fatalf("shutdown took %s, want the fixed 2s grace before SIGKILL", elapsed)
	}
}

// The upstream itself exits when its stdin closes but leaves behind a child
// that ignores SIGTERM: the group is still killed after the grace.
func TestShutdownKillsStubbornChildOfUpstreamThatExitedByItself(t *testing.T) {
	cfg, pidFile, mark := lifecycleConfig(t, true)
	cfg.Linger = false
	elapsed := closeAgentAndMeasure(t, cfg, pidFile, mark)
	if elapsed < 2*time.Second || elapsed > 3500*time.Millisecond {
		t.Fatalf("shutdown took %s, want the fixed 2s grace before SIGKILL", elapsed)
	}
}

func lifecycleConfig(t *testing.T, ignoreTERM bool) (mcpfake.Config, string, string) {
	t.Helper()
	if _, err := exec.LookPath("pgrep"); err != nil {
		t.Skip("pgrep not available")
	}
	mark := fmt.Sprintf("ctxgate-fake-child-%d-%d", os.Getpid(), time.Now().UnixNano())
	pidFile := t.TempDir() + "/upstream.pid"
	return mcpfake.Config{IgnoreTERM: ignoreTERM, Linger: true, ChildMark: mark, PIDFile: pidFile}, pidFile, mark
}

// closeAgentAndMeasure starts the upstream (plus a grandchild in its process
// group), closes the agent's stdin, and returns how long serve took to return.
// It then requires that neither the upstream nor its child is left alive.
func closeAgentAndMeasure(t *testing.T, cfg mcpfake.Config, pidFile, mark string) time.Duration {
	t.Helper()
	s := startRaw(t, subprocessRelay(cfg, 10*time.Second))
	eventually(t, 5*time.Second, "grandchild to be ready", func() bool {
		_, err := os.Stat(pidFile + ".child")
		return err == nil && pgrepCount(t, mark) == 1
	})
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	_ = s.in.Close()
	if err := s.wait(t, 10*time.Second); err != nil {
		t.Fatalf("serve error = %v, want clean shutdown", err)
	}
	elapsed := time.Since(start)

	eventually(t, 2*time.Second, "no orphan child (pgrep empty)", func() bool { return pgrepCount(t, mark) == 0 })
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("upstream %d is still alive: %v", pid, err)
	}
	return elapsed
}

func pgrepCount(t *testing.T, mark string) int {
	t.Helper()
	out, err := exec.Command("pgrep", "-f", "--", mark).Output()
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return 0
	}
	if err != nil {
		t.Fatalf("pgrep: %v", err)
	}
	return len(strings.Fields(string(out)))
}

func eventually(t *testing.T, within time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
