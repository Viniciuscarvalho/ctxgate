// Package mcpfake is a configurable fake MCP upstream for tests. It speaks
// newline-delimited JSON-RPC over any reader/writer pair, so the same fake runs
// in memory (io.Pipe) or as a re-exec of the test binary (stdio).
package mcpfake

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// EnvKey carries a JSON-encoded Config to a re-executed test binary.
const EnvKey = "CTXGATE_FAKE_MCP_CONFIG"

// Tool describes one fake tool and how it answers tools/call.
type Tool struct {
	Name       string        `json:"name"`
	Text       string        `json:"text,omitempty"`       // fixed reply text
	Delay      time.Duration `json:"delay,omitempty"`      // latency before replying
	IsError    bool          `json:"isError,omitempty"`    // reply is a tool error result
	RPCError   string        `json:"rpcError,omitempty"`   // reply is a JSON-RPC error with this message
	LargeBytes int           `json:"largeBytes,omitempty"` // reply text is this many bytes
	Echo       bool          `json:"echo,omitempty"`       // reply text is arguments.text after arguments.sleep_ms
}

// Prompt is a fake prompt answered by prompts/get with a single user message.
type Prompt struct {
	Name string `json:"name"`
	Text string `json:"text"`
}

// Config is the whole fake upstream.
type Config struct {
	Tools   []Tool   `json:"tools"`
	Prompts []Prompt `json:"prompts,omitempty"`
}

// Env returns the environment that makes a re-executed test binary load c.
func (c Config) Env() map[string]string {
	data, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return map[string]string{EnvKey: string(data)}
}

// FromEnv loads the Config set by Env. ok is false outside a fake process.
func FromEnv() (c Config, ok bool, err error) {
	raw := os.Getenv(EnvKey)
	if raw == "" {
		return c, false, nil
	}
	return c, true, json.Unmarshal([]byte(raw), &c)
}

type request struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// Serve answers requests from r on w until r ends. Requests run concurrently,
// so replies may leave in a different order than they arrived.
func (c Config) Serve(r io.Reader, w io.Writer) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	defer wg.Wait()
	send := func(resp response) {
		resp.JSONRPC = "2.0"
		data, err := json.Marshal(resp)
		if err != nil {
			panic(err)
		}
		mu.Lock()
		defer mu.Unlock()
		_, _ = w.Write(append(data, '\n'))
	}

	reader := bufio.NewReader(r)
	for {
		line, err := reader.ReadBytes('\n')
		var req request
		if len(line) > 0 && json.Unmarshal(line, &req) == nil && len(req.ID) > 0 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				send(c.handle(req))
			}()
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (c Config) handle(req request) response {
	resp := response{ID: req.ID}
	switch req.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &p)
		resp.Result = map[string]any{
			"protocolVersion": p.ProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}, "prompts": map[string]any{}},
			"serverInfo":      map[string]any{"name": "mcpfake", "version": "0"},
		}
	case "ping":
		resp.Result = map[string]any{}
	case "tools/list":
		tools := make([]map[string]any, 0, len(c.Tools))
		for _, t := range c.Tools {
			tools = append(tools, map[string]any{"name": t.Name, "inputSchema": map[string]any{"type": "object"}})
		}
		resp.Result = map[string]any{"tools": tools}
	case "tools/call":
		resp = c.call(req)
	case "prompts/get":
		resp = c.prompt(req)
	default:
		resp.Error = &rpcError{Code: -32601, Message: "method not found: " + req.Method}
	}
	return resp
}

func (c Config) call(req request) response {
	resp := response{ID: req.ID}
	var p struct {
		Name      string `json:"name"`
		Arguments struct {
			Text    string `json:"text"`
			SleepMS int    `json:"sleep_ms"`
		} `json:"arguments"`
	}
	_ = json.Unmarshal(req.Params, &p)
	for _, t := range c.Tools {
		if t.Name != p.Name {
			continue
		}
		time.Sleep(t.Delay)
		if t.RPCError != "" {
			resp.Error = &rpcError{Code: -32000, Message: t.RPCError}
			return resp
		}
		text := t.Text
		if t.LargeBytes > 0 {
			text = strings.Repeat("x", t.LargeBytes)
		}
		if t.Echo {
			time.Sleep(time.Duration(p.Arguments.SleepMS) * time.Millisecond)
			text = p.Arguments.Text
		}
		resp.Result = map[string]any{
			"content": []map[string]any{{"type": "text", "text": text}},
			"isError": t.IsError,
		}
		return resp
	}
	resp.Error = &rpcError{Code: -32602, Message: "unknown tool: " + p.Name}
	return resp
}

func (c Config) prompt(req request) response {
	resp := response{ID: req.ID}
	var p struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(req.Params, &p)
	for _, pr := range c.Prompts {
		if pr.Name == p.Name {
			resp.Result = map[string]any{
				"description": "fake prompt " + pr.Name,
				"messages": []map[string]any{{
					"role":    "user",
					"content": map[string]any{"type": "text", "text": pr.Text},
				}},
			}
			return resp
		}
	}
	resp.Error = &rpcError{Code: -32602, Message: "unknown prompt: " + p.Name}
	return resp
}
