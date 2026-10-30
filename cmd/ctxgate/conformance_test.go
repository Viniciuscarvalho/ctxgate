package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const sdkUpstreamEnv = "CTXGATE_MCP_SDK_UPSTREAM"

func TestMCPConformanceSDKUpstream(t *testing.T) {
	if os.Getenv(sdkUpstreamEnv) != "1" {
		return
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "ctxgate-conformance", Version: "v1.0.0"}, nil)
	server.AddTool(&mcp.Tool{Name: "echo", Description: "returns exact UTF-8 text", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{
			&mcp.TextContent{Text: "ctxgate preserves: café ☕\\nsecond line"},
		}}, nil
	})
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		t.Errorf("SDK upstream server: %v", err)
	}
}

func TestMCPConformanceThroughCtxgate(t *testing.T) {
	ctxgate := filepath.Join(t.TempDir(), "ctxgate")
	build(t, ctxgate, ".")

	for _, protocolVersion := range []string{"2025-11-25", "2026-07-28"} {
		t.Run(protocolVersion, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			direct := callSDKUpstream(t, ctx, protocolVersion, sdkUpstreamCommand())

			configPath := filepath.Join(t.TempDir(), "ctxgate.yaml")
			config := fmt.Sprintf("upstreams:\n  - name: sdk\n    command: %s\n    args: [%s, %s]\n    env:\n      %s: %s\n", yamlString(os.Args[0]), yamlString("-test.run=^TestMCPConformanceSDKUpstream$"), yamlString("--"), sdkUpstreamEnv, yamlString("1"))
			if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			throughRelay := callSDKUpstream(t, ctx, protocolVersion, exec.Command(ctxgate, "serve", "--config", configPath))

			if !bytes.Equal(throughRelay.tools, direct.tools) {
				t.Fatalf("tools/list differs from upstream\nrelay: %s\nupstream: %s", throughRelay.tools, direct.tools)
			}
			if !bytes.Equal(throughRelay.content, direct.content) {
				t.Fatalf("tools/call content differs from upstream\nrelay: %s\nupstream: %s", throughRelay.content, direct.content)
			}
		})
	}
}

type sdkResult struct {
	tools   []byte
	content []byte
}

func callSDKUpstream(t *testing.T, ctx context.Context, protocolVersion string, command *exec.Cmd) sdkResult {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "ctxgate-conformance-client", Version: "v1.0.0"}, nil)
	session, err := client.Connect(ctx, &mcp.CommandTransport{Command: command}, &mcp.ClientSessionOptions{ProtocolVersion: protocolVersion})
	if err != nil {
		t.Fatalf("SDK client connect (%s): %v", protocolVersion, err)
	}
	defer session.Close()

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("SDK tools/list (%s): %v", protocolVersion, err)
	}
	toolBytes, err := json.Marshal(tools.Tools)
	if err != nil {
		t.Fatal(err)
	}

	call, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "echo"})
	if err != nil {
		t.Fatalf("SDK tools/call (%s): %v", protocolVersion, err)
	}
	contentBytes, err := json.Marshal(call.Content)
	if err != nil {
		t.Fatal(err)
	}
	return sdkResult{tools: toolBytes, content: contentBytes}
}

func sdkUpstreamCommand() *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestMCPConformanceSDKUpstream$", "--")
	command.Env = append(os.Environ(), sdkUpstreamEnv+"=1")
	return command
}

func yamlString(value string) string {
	return strconv.Quote(value)
}
