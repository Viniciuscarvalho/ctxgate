package mcp_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Viniciuscarvalho/ctxgate/internal/mcp"
	"github.com/Viniciuscarvalho/ctxgate/internal/testutil/mcpfake"
)

// TestFakeUpstreamProcess is not a test: when re-executed with mcpfake.EnvKey
// set, the test binary becomes the fake stdio upstream (subprocess variant).
func TestFakeUpstreamProcess(t *testing.T) {
	cfg, ok, err := mcpfake.FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return
	}
	if err := cfg.Serve(os.Stdin, os.Stdout); err != nil {
		t.Fatal(err)
	}
}

// connector returns an SDK client session wired to a fake upstream built from cfg.
type connector func(t *testing.T, cfg mcpfake.Config) *sdk.ClientSession

const largeBytes = 2 << 20

var variants = []struct {
	name    string
	connect connector
}{
	{"memory", connectMemory},
	{"subprocess", connectSubprocess},
}

func testConfig() mcpfake.Config {
	return mcpfake.Config{Tools: []mcpfake.Tool{
		{Name: "fixed", Text: "olá, mundo ☕"},
		{Name: "slow", Text: "done", Delay: 50 * time.Millisecond},
		{Name: "toolerr", Text: "falhou de propósito", IsError: true},
		{Name: "rpcerr", RPCError: "boom"},
		{Name: "large", LargeBytes: largeBytes},
		{Name: "echo", Echo: true},
	}}
}

func TestPassthroughListsTools(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			want := mustListTools(t, connectDirect(t, testConfig()))
			got := mustListTools(t, v.connect(t, testConfig()))
			if got != want {
				t.Fatalf("tools/list differs from direct upstream\nrelay:  %s\ndirect: %s", got, want)
			}
			for _, name := range []string{"fixed", "slow", "toolerr", "rpcerr", "large", "echo"} {
				if !strings.Contains(got, `"name":"`+name+`"`) {
					t.Errorf("tool %q missing from %s", name, got)
				}
			}
		})
	}
}

func TestPassthroughSuccessfulCall(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			res, err := v.connect(t, testConfig()).CallTool(context.Background(), &sdk.CallToolParams{Name: "fixed"})
			if err != nil {
				t.Fatal(err)
			}
			if res.IsError || textOf(t, res) != "olá, mundo ☕" {
				t.Fatalf("result = %+v", res)
			}
		})
	}
}

func TestPassthroughLatency(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			session := v.connect(t, testConfig())
			start := time.Now()
			res, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: "slow"})
			if err != nil || textOf(t, res) != "done" {
				t.Fatalf("result = %+v, err = %v", res, err)
			}
			if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
				t.Fatalf("elapsed %s, want at least the configured 50ms latency", elapsed)
			}
		})
	}
}

func TestPassthroughUpstreamErrorsArriveIntact(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			ctx := context.Background()
			direct, session := connectDirect(t, testConfig()), v.connect(t, testConfig())

			res, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "toolerr"})
			if err != nil {
				t.Fatal(err)
			}
			if !res.IsError || textOf(t, res) != "falhou de propósito" {
				t.Fatalf("tool error result = %+v", res)
			}

			_, wantErr := direct.CallTool(ctx, &sdk.CallToolParams{Name: "rpcerr"})
			_, gotErr := session.CallTool(ctx, &sdk.CallToolParams{Name: "rpcerr"})
			if wantErr == nil || gotErr == nil || gotErr.Error() != wantErr.Error() || !strings.Contains(gotErr.Error(), "boom") {
				t.Fatalf("JSON-RPC error = %v, direct = %v", gotErr, wantErr)
			}
		})
	}
}

func TestPassthroughLargeResponse(t *testing.T) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			res, err := v.connect(t, testConfig()).CallTool(context.Background(), &sdk.CallToolParams{Name: "large"})
			if err != nil {
				t.Fatal(err)
			}
			if got := textOf(t, res); got != strings.Repeat("x", largeBytes) {
				t.Fatalf("large response has %d bytes, want %d", len(got), largeBytes)
			}
		})
	}
}

func TestPassthroughConcurrentCallsKeepTheirIDs(t *testing.T) {
	const calls = 20
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			session := v.connect(t, testConfig())
			var wg sync.WaitGroup
			for i := range calls {
				wg.Add(1)
				go func() {
					defer wg.Done()
					want := fmt.Sprintf("call-%d", i)
					// Earlier calls sleep longer, so replies leave in reverse order.
					args := map[string]any{"text": want, "sleep_ms": (calls - i) * 5}
					res, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: "echo", Arguments: args})
					if err != nil {
						t.Errorf("call %d: %v", i, err)
						return
					}
					if got := textOf(t, res); got != want {
						t.Errorf("call %d got %q, want %q (swapped response)", i, got, want)
					}
				}()
			}
			wg.Wait()
		})
	}
}

// TestPassthroughPromptsGetIsByteIdentical sends a raw prompts/get line (odd
// spacing and key order on purpose) and compares the raw response line with the
// one the fake writes when no relay is in between.
func TestPassthroughPromptsGetIsByteIdentical(t *testing.T) {
	cfg := mcpfake.Config{Prompts: []mcpfake.Prompt{{Name: "resumo", Text: "texto com acentuação ☕ e \\n escapado"}}}
	request := `{ "params":{"arguments":{"tema":"café"}, "name":"resumo"},  "method":"prompts/get","id":"p-1","jsonrpc":"2.0" }` + "\n"

	want := exchange(t, request, func(_ context.Context, in io.Reader, out io.Writer) error { return cfg.Serve(in, out) })
	if !strings.Contains(want, `"text":"texto com acentuação ☕`) {
		t.Fatalf("baseline response is not the prompt: %q", want)
	}
	relays := map[string]func(context.Context, io.Reader, io.Writer) error{
		"memory": func(ctx context.Context, in io.Reader, out io.Writer) error {
			upInR, upInW := io.Pipe()
			upOutR, upOutW := io.Pipe()
			go func() {
				_ = cfg.Serve(upInR, upOutW)
				_ = upOutW.Close()
			}()
			return mcp.Relay{UpstreamIn: upInW, UpstreamOut: upOutR}.Serve(ctx, in, out)
		},
		"subprocess": func(ctx context.Context, in io.Reader, out io.Writer) error {
			args := []string{"-test.run=^TestFakeUpstreamProcess$", "--"}
			return mcp.Run(ctx, in, out, io.Discard, os.Args[0], args, cfg.Env(), 10*time.Second)
		},
	}
	for name, serve := range relays {
		t.Run(name, func(t *testing.T) {
			if got := exchange(t, request, serve); got != want {
				t.Fatalf("prompts/get response changed by the relay\ngot:  %q\nwant: %q", got, want)
			}
		})
	}
}

// exchange writes one raw request line to serve and returns the raw response line.
func exchange(t *testing.T, request string, serve func(context.Context, io.Reader, io.Writer) error) string {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, inR, outW)
		_ = outW.Close()
	}()
	if _, err := io.WriteString(inW, request); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(outR).ReadString('\n')
	if err != nil {
		t.Fatalf("reading response: %v", err)
	}
	_ = inW.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("serve did not stop after input closed")
	}
	return line
}

// connectDirect talks to the fake with no relay in between; it is the baseline.
func connectDirect(t *testing.T, cfg mcpfake.Config) *sdk.ClientSession {
	t.Helper()
	upIn, clientOut := io.Pipe()
	clientIn, upOut := io.Pipe()
	go func() {
		_ = cfg.Serve(upIn, upOut)
		_ = upOut.Close()
	}()
	return newClient(t, clientIn, clientOut)
}

// connectMemory relays between the SDK client and an in-process fake over io.Pipe.
func connectMemory(t *testing.T, cfg mcpfake.Config) *sdk.ClientSession {
	t.Helper()
	upstreamInR, upstreamInW := io.Pipe()
	upstreamOutR, upstreamOutW := io.Pipe()
	go func() {
		_ = cfg.Serve(upstreamInR, upstreamOutW)
		_ = upstreamOutW.Close()
	}()
	return connectRelay(t, func(ctx context.Context, agentIn io.Reader, agentOut io.Writer) error {
		return mcp.Relay{UpstreamIn: upstreamInW, UpstreamOut: upstreamOutR}.Serve(ctx, agentIn, agentOut)
	})
}

// connectSubprocess relays to a re-exec of this test binary acting as the fake.
func connectSubprocess(t *testing.T, cfg mcpfake.Config) *sdk.ClientSession {
	t.Helper()
	return connectRelay(t, func(ctx context.Context, agentIn io.Reader, agentOut io.Writer) error {
		args := []string{"-test.run=^TestFakeUpstreamProcess$", "--"}
		return mcp.Run(ctx, agentIn, agentOut, io.Discard, os.Args[0], args, cfg.Env(), 10*time.Second)
	})
}

// connectRelay starts serve between pipes and returns an SDK client on the agent side.
func connectRelay(t *testing.T, serve func(context.Context, io.Reader, io.Writer) error) *sdk.ClientSession {
	t.Helper()
	agentInR, agentInW := io.Pipe()
	agentOutR, agentOutW := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, agentInR, agentOutW)
		_ = agentOutW.Close()
	}()
	session := newClient(t, agentOutR, agentInW)
	t.Cleanup(func() {
		_ = session.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("relay returned %v", err)
			}
		case <-time.After(5 * time.Second):
			cancel()
			t.Error("relay did not stop after the client closed")
		}
		cancel()
	})
	return session
}

func newClient(t *testing.T, r io.ReadCloser, w io.WriteCloser) *sdk.ClientSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := sdk.NewClient(&sdk.Implementation{Name: "ctxgate-test-client", Version: "v0"}, nil)
	session, err := client.Connect(ctx, &sdk.IOTransport{Reader: r, Writer: w}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func mustListTools(t *testing.T, session *sdk.ClientSession) string {
	t.Helper()
	tools, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(tools.Tools)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func textOf(t *testing.T, res *sdk.CallToolResult) string {
	t.Helper()
	if len(res.Content) != 1 {
		t.Fatalf("content = %+v, want one item", res.Content)
	}
	text, ok := res.Content[0].(*sdk.TextContent)
	if !ok {
		t.Fatalf("content[0] = %T, want text", res.Content[0])
	}
	return text.Text
}
