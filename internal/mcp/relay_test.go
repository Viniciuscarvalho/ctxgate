package mcp

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRelayPassesLinesByteForByte(t *testing.T) {
	agentInR, agentInW := io.Pipe()
	upstreamInR, upstreamInW := io.Pipe()
	upstreamOutR, upstreamOutW := io.Pipe()
	var agentOut lockedBuffer

	relay := Relay{UpstreamIn: upstreamInW, UpstreamOut: upstreamOutR}
	done := make(chan error, 1)
	go func() { done <- relay.Serve(context.Background(), agentInR, &agentOut) }()

	request := []byte(`[{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"x"}}]` + "\n")
	response := []byte(`{"jsonrpc":"2.0","id":7,"result":{"content":[{"type":"text","text":"unchanged"}]}}` + "\n")

	go func() {
		if _, err := agentInW.Write(request); err != nil {
			t.Error(err)
		}
	}()
	gotRequest := make([]byte, len(request))
	if _, err := io.ReadFull(upstreamInR, gotRequest); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotRequest, request) {
		t.Fatalf("request changed: got %q, want %q", gotRequest, request)
	}
	go func() {
		_, _ = upstreamOutW.Write(response)
		_ = upstreamOutW.Close()
	}()
	waitForOutput(t, &agentOut, len(response))
	_ = agentInW.Close()

	if err := <-done; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if got := agentOut.Bytes(); !bytes.Equal(got, response) {
		t.Fatalf("response changed: got %q, want %q", got, response)
	}
}

func TestRelaySupportsLinesLargerThanOneMegabyte(t *testing.T) {
	agentInR, agentInW := io.Pipe()
	upstreamInR, upstreamInW := io.Pipe()
	upstreamOutR, upstreamOutW := io.Pipe()
	var agentOut lockedBuffer
	relay := Relay{UpstreamIn: upstreamInW, UpstreamOut: upstreamOutR}
	done := make(chan error, 1)
	go func() { done <- relay.Serve(context.Background(), agentInR, &agentOut) }()

	line := []byte(`{"jsonrpc":"2.0","id":"large","result":{"content":[{"type":"text","text":"` + strings.Repeat("x", 1024*1024) + `"}]}}` + "\n")
	go func() {
		_, _ = upstreamOutW.Write(line)
		_ = upstreamOutW.Close()
	}()
	if _, err := agentInW.Write([]byte("{\"jsonrpc\":\"2.0\",\"id\":\"large\",\"method\":\"tools/call\"}\n")); err != nil {
		t.Fatal(err)
	}
	request := []byte("{\"jsonrpc\":\"2.0\",\"id\":\"large\",\"method\":\"tools/call\"}\n")
	gotRequest := make([]byte, len(request))
	if _, err := io.ReadFull(upstreamInR, gotRequest); err != nil {
		t.Fatal(err)
	}
	waitForOutput(t, &agentOut, len(line))
	_ = agentInW.Close()
	if err := <-done; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if !bytes.Equal(agentOut.Bytes(), line) {
		t.Fatalf("large response was changed or truncated: got %d bytes, want %d", agentOut.Len(), len(line))
	}
}

func TestRelayKeepsConcurrentToolCallIDs(t *testing.T) {
	agentInR, agentInW := io.Pipe()
	upstreamInR, upstreamInW := io.Pipe()
	upstreamOutR, upstreamOutW := io.Pipe()
	var agentOut lockedBuffer
	relay := Relay{UpstreamIn: upstreamInW, UpstreamOut: upstreamOutR}
	done := make(chan error, 1)
	go func() { done <- relay.Serve(context.Background(), agentInR, &agentOut) }()

	requests := "{\"jsonrpc\":\"2.0\",\"id\":\"first\",\"method\":\"tools/call\"}\n{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"tools/call\"}\n"
	go func() {
		_, _ = agentInW.Write([]byte(requests))
	}()
	gotRequests := make([]byte, len(requests))
	if _, err := io.ReadFull(upstreamInR, gotRequests); err != nil {
		t.Fatal(err)
	}
	if got := string(gotRequests); got != requests {
		t.Fatalf("requests = %q, want %q", got, requests)
	}
	go func() {
		_, _ = io.WriteString(upstreamOutW, "{\"jsonrpc\":\"2.0\",\"id\":42,\"result\":{}}\n{\"jsonrpc\":\"2.0\",\"id\":\"first\",\"result\":{}}\n")
		_ = upstreamOutW.Close()
	}()
	waitForOutput(t, &agentOut, len(`{"jsonrpc":"2.0","id":42,"result":{}}`)+len(`{"jsonrpc":"2.0","id":"first","result":{}}`)+2)
	_ = agentInW.Close()

	if err := <-done; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	got := agentOut.String()
	for _, id := range []string{`"id":42`, `"id":"first"`} {
		if !strings.Contains(got, id) {
			t.Fatalf("response %q does not preserve %s", got, id)
		}
	}
}

func TestRelayPassesNonToolMCPMessagesUnchanged(t *testing.T) {
	agentInR, agentInW := io.Pipe()
	upstreamInR, upstreamInW := io.Pipe()
	upstreamOutR, upstreamOutW := io.Pipe()
	var agentOut lockedBuffer
	relay := Relay{UpstreamIn: upstreamInW, UpstreamOut: upstreamOutR}
	done := make(chan error, 1)
	go func() { done <- relay.Serve(context.Background(), agentInR, &agentOut) }()

	requests := "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":\"p\"}}\n" +
		"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":1}}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"resources/read\",\"params\":{\"uri\":\"file:///x\"}}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"resources/list\"}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"prompts/list\"}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"x-unknown/method\",\"params\":{\"extra\":true},\"zzz\":1}\n"
	go func() { _, _ = io.WriteString(agentInW, requests) }()
	forwarded := make([]byte, len(requests))
	if _, err := io.ReadFull(upstreamInR, forwarded); err != nil {
		t.Fatal(err)
	}
	if got := string(forwarded); got != requests {
		t.Fatalf("requests changed: got %q, want %q", got, requests)
	}

	responses := "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"sampling/createMessage\",\"params\":{}}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"contents\":[]}}\n" +
		"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{\"level\":\"info\",\"data\":\"x\"}}\n" +
		"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":\"p\",\"progress\":1}}\n" +
		"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":3}}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":4,\"result\":{\"resources\":[]}}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":5,\"result\":{\"prompts\":[]}}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":9,\"method\":\"x-unknown/server\",\"weird\":[1,2]}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":6,\"result\":{}}\n"
	go func() {
		_, _ = io.WriteString(upstreamOutW, responses)
		_ = upstreamOutW.Close()
	}()
	waitForOutput(t, &agentOut, len(responses))
	_ = agentInW.Close()
	if err := <-done; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if got := agentOut.String(); got != responses {
		t.Fatalf("responses changed: got %q, want %q", got, responses)
	}
}

func waitForOutput(t *testing.T, output *lockedBuffer, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if output.Len() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("output length = %d, want at least %d", output.Len(), want)
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Len()
}

func (b *lockedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.b.Bytes()...)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestRelayReturnsAfterAgentEOF(t *testing.T) {
	agentInR, agentInW := io.Pipe()
	upstreamInR, upstreamInW := io.Pipe()
	upstreamOutR, _ := io.Pipe()
	relay := Relay{UpstreamIn: upstreamInW, UpstreamOut: upstreamOutR}
	done := make(chan error, 1)
	go func() { done <- relay.Serve(context.Background(), agentInR, io.Discard) }()
	_ = agentInW.Close()
	_, _ = io.Copy(io.Discard, upstreamInR)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() did not return after agent EOF")
	}
}

func TestRelayAnswersPendingRequestsWhenUpstreamCloses(t *testing.T) {
	agentInR, agentInW := io.Pipe()
	upstreamInR, upstreamInW := io.Pipe()
	upstreamOutR, upstreamOutW := io.Pipe()
	var agentOut lockedBuffer
	relay := Relay{UpstreamIn: upstreamInW, UpstreamOut: upstreamOutR}
	done := make(chan error, 1)
	go func() { done <- relay.Serve(context.Background(), agentInR, &agentOut) }()

	requests := "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\"}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":\"b\",\"method\":\"tools/call\"}\n" +
		"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n"
	go func() { _, _ = io.WriteString(agentInW, requests) }()
	if _, err := io.ReadFull(upstreamInR, make([]byte, len(requests))); err != nil {
		t.Fatal(err)
	}
	answered := "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n"
	go func() {
		_, _ = io.WriteString(upstreamOutW, answered)
		_ = upstreamOutW.Close()
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Serve() = nil after the upstream closed with a pending request")
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() did not return after the upstream closed")
	}
	want := answered + "{\"jsonrpc\":\"2.0\",\"id\":\"b\",\"error\":{\"code\":-32603,\"message\":\"upstream exited before responding\"}}\n"
	if got := agentOut.String(); got != want {
		t.Fatalf("agent output = %q, want %q", got, want)
	}
}

func TestRelayAnswersRequestThatCannotReachTheUpstream(t *testing.T) {
	agentInR, agentInW := io.Pipe()
	upstreamInR, upstreamInW := io.Pipe()
	upstreamOutR, _ := io.Pipe()
	_ = upstreamInR.Close() // upstream is gone: writes fail with io.ErrClosedPipe
	var agentOut lockedBuffer
	relay := Relay{UpstreamIn: upstreamInW, UpstreamOut: upstreamOutR}
	done := make(chan error, 1)
	go func() { done <- relay.Serve(context.Background(), agentInR, &agentOut) }()

	go func() { _, _ = io.WriteString(agentInW, "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\"}\n") }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Serve() = nil after a failed write to the upstream")
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() did not return after a failed write")
	}
	want := "{\"jsonrpc\":\"2.0\",\"id\":3,\"error\":{\"code\":-32603,\"message\":\"upstream exited before responding\"}}\n"
	if got := agentOut.String(); got != want {
		t.Fatalf("agent output = %q, want %q", got, want)
	}
}
