package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Viniciuscarvalho/ctxgate/internal/config"
	"github.com/Viniciuscarvalho/ctxgate/internal/mcp"
	"github.com/Viniciuscarvalho/ctxgate/internal/pipeline"
)

// relayFromYAML aplica as opções que o serve monta a partir de um YAML com
// pipeline.max_in_flight = maxInFlight e devolve o Relay resultante.
func relayFromYAML(t *testing.T, maxInFlight string) mcp.Relay {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ctxgate.yaml")
	yaml := "upstreams:\n  - name: u\n    command: /bin/true\npipeline:\n  enabled: true\n  dedup:\n    enabled: true\n  max_in_flight: " + maxInFlight + "\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := pipelineOptions(cfg.Pipeline, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	var relay mcp.Relay
	for _, opt := range opts {
		opt(&relay)
	}
	return relay
}

func TestYAMLMaxInFlightReachesTheRelay(t *testing.T) {
	if got := relayFromYAML(t, "3").MaxInFlight; got != 3 {
		t.Fatalf("Relay.MaxInFlight = %d, want 3 (pipeline.max_in_flight do YAML)", got)
	}
}

// O mesmo valor limita as contagens exatas do pipeline: com max_in_flight=1, só uma das
// respostas grandes simultâneas ganha a contagem exata; as outras saem com Exact=false.
func TestYAMLMaxInFlightReachesThePipelineCounts(t *testing.T) {
	red := relayFromYAML(t, "1").Reducer
	if red == nil {
		t.Fatal("pipeline enabled but the relay has no reducer")
	}
	text := strings.Repeat("uma palavra qualquer ", (2<<20)/21+1) // >256 KB: contagem exata em segundo plano
	const calls = 6
	finals := make(chan pipeline.Report, calls)
	var wg sync.WaitGroup
	for range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o := red.Run(context.Background(), pipeline.Input{Call: pipeline.Call{Tool: "t"}, Texts: []string{text}})
			select {
			case f := <-o.Done:
				finals <- f
			case <-time.After(30 * time.Second):
				t.Error("final report never delivered")
				finals <- pipeline.Report{}
			}
		}()
	}
	wg.Wait()
	exact := 0
	for range calls {
		if (<-finals).Exact {
			exact++
		}
	}
	if exact == calls {
		t.Fatalf("all %d final reports are exact: the YAML max_in_flight=1 did not limit the counts", calls)
	}
}
