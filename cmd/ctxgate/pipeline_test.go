package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	repeatUpstreamEnv = "CTXGATE_MCP_REPEAT_UPSTREAM"
	repeated          = "Este parágrafo o upstream manda três vezes."
)

func TestRepeatSDKUpstream(t *testing.T) {
	if os.Getenv(repeatUpstreamEnv) != "1" {
		return
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "ctxgate-repeat", Version: "v1.0.0"}, nil)
	server.AddTool(&mcp.Tool{Name: "repeat", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		text := repeated + "\n\n" + repeated + "\n\n" + repeated
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}, nil
	})
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		t.Errorf("SDK upstream server: %v", err)
	}
}

func TestServeAppliesTheConfiguredPipeline(t *testing.T) {
	ctxgate := filepath.Join(t.TempDir(), "ctxgate")
	build(t, ctxgate, ".")
	triple := repeated + "\n\n" + repeated + "\n\n" + repeated

	cases := []struct {
		name, pipeline, want string
	}{
		{"pipeline desligado", "pipeline:\n  enabled: false\n  dedup:\n    enabled: true\n", triple},
		{"pipeline ligado sem dedup", "pipeline:\n  enabled: true\n", triple},
		{"pipeline ligado com dedup", "pipeline:\n  enabled: true\n  dedup:\n    enabled: true\n", repeated + "\n\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			configPath := filepath.Join(t.TempDir(), "ctxgate.yaml")
			config := fmt.Sprintf("upstreams:\n  - name: repeat\n    command: %s\n    args: [%s, %s]\n    env:\n      %s: %s\n%s", yamlString(os.Args[0]), yamlString("-test.run=^TestRepeatSDKUpstream$"), yamlString("--"), repeatUpstreamEnv, yamlString("1"), tc.pipeline)
			if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			client := mcp.NewClient(&mcp.Implementation{Name: "ctxgate-pipeline-client", Version: "v1.0.0"}, nil)
			session, err := client.Connect(ctx, &mcp.CommandTransport{Command: exec.Command(ctxgate, "serve", "--config", configPath)}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			call, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "repeat"})
			if err != nil {
				t.Fatal(err)
			}
			text, ok := call.Content[0].(*mcp.TextContent)
			if len(call.Content) != 1 || !ok || text.Text != tc.want {
				t.Fatalf("agent received %+v, want %q", call.Content, tc.want)
			}
		})
	}
}
