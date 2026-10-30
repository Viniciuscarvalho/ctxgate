package pipeline_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/Viniciuscarvalho/ctxgate/internal/pipeline"
)

// p95 devolve o percentil 95 (nearest-rank) das durações.
func p95(d []time.Duration) time.Duration {
	slices.Sort(d)
	return d[(len(d)*95+99)/100-1]
}

func sample(p *pipeline.Pipeline, text string, n int) []time.Duration {
	in := pipeline.Input{Call: pipeline.Call{Tool: "bench"}, Texts: []string{text}}
	// aquecimento: tabelas do tokenizer e caches
	for range 3 {
		p.Run(context.Background(), in)
	}
	out := make([]time.Duration, n)
	for i := range out {
		start := time.Now()
		p.Run(context.Background(), in)
		out[i] = time.Since(start)
	}
	return out
}

// TestRunP95GateUpTo100KB é o gate do CTO: p95 < 20 ms para respostas de até 100 KB,
// com a contagem exata no caminho crítico. Sob -race o tempo não representa nada.
func TestRunP95GateUpTo100KB(t *testing.T) {
	if raceEnabled || testing.Short() {
		t.Skip("gate de latência só roda sem -race e sem -short")
	}
	p := newPipeline(t)
	for _, size := range []int{10 << 10, 50 << 10, 100 << 10} {
		got := p95(sample(p, mixedResponse(size), 300))
		t.Logf("%3d KB: p95 = %s", size>>10, got)
		if got >= 20*time.Millisecond {
			t.Errorf("%d KB: p95 = %s, want < 20ms", size>>10, got)
		}
	}
}

// BenchmarkRun mede o Run por tamanho e publica o p95. De 100 KB a 256 KB o p95 é só
// reportado; acima de 256 KB não há contagem exata no caminho crítico.
func BenchmarkRun(b *testing.B) {
	for _, size := range []int{10 << 10, 100 << 10, 250 << 10, 300 << 10, 2 << 20} {
		text := mixedResponse(size)
		b.Run(sizeName(size), func(b *testing.B) {
			p := newPipeline(b)
			p.Run(context.Background(), pipeline.Input{Texts: []string{text}})
			b.ResetTimer()
			durations := make([]time.Duration, 0, b.N)
			for range b.N {
				start := time.Now()
				p.Run(context.Background(), pipeline.Input{Texts: []string{text}})
				durations = append(durations, time.Since(start))
			}
			b.ReportMetric(float64(p95(durations))/float64(time.Millisecond), "p95-ms")
		})
	}
}

func sizeName(size int) string {
	if size >= 1<<20 {
		return fmt.Sprintf("%dMiB", size>>20)
	}
	return fmt.Sprintf("%dKB", size>>10)
}
