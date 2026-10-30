package tokens

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCl100kReferenceCounts(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Encoding(); got != "cl100k_base" {
		t.Fatalf("Encoding() = %q", got)
	}
	cases := []struct {
		name string
		text string
		want int
	}{
		{"vazio", "", 0},
		{"ascii", "hello world", 2},
		{"ascii com pontuação", "Hello, world!", 4},
		{"pt-BR com acentos", "Não há ação sem atenção.", 8},
		{"código", "func main() {\n\tfmt.Println(\"hi\")\n}", 10},
		{"json", `{"name":"ctxgate","tags":["mcp","go"],"n":42}`, 17},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := c.Count(tc.text)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("Count(%q) = %d, want %d", tc.text, got, tc.want)
			}
		})
	}
}

// fakeCounter conta palavras; permite atraso, falha e contagem de chamadas.
type fakeCounter struct {
	delay time.Duration
	fail  func(string) error
	calls atomic.Int64
}

func (f *fakeCounter) Encoding() string { return "fake" }

func (f *fakeCounter) Count(s string) (int, error) {
	f.calls.Add(1)
	time.Sleep(f.delay)
	if f.fail != nil {
		if err := f.fail(s); err != nil {
			return 0, err
		}
	}
	return len(strings.Fields(s)), nil
}

func TestCountAllPerChunkAndCachedByHash(t *testing.T) {
	c := &fakeCounter{}
	got, err := CountAll(context.Background(), c, []string{"a b", "c", "a b", "", "a b"})
	if err != nil {
		t.Fatal(err)
	}
	want := []int{2, 1, 2, 0, 2}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("counts = %v, want %v", got, want)
		}
	}
	if n := c.calls.Load(); n != 3 {
		t.Fatalf("Count called %d times, want 3 (one per distinct chunk)", n)
	}
}

func TestCountAllUsesAtMostFourWorkers(t *testing.T) {
	var running, peak atomic.Int64
	c := countFunc(func(s string) (int, error) {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		running.Add(-1)
		return 1, nil
	})
	texts := make([]string, 40)
	for i := range texts {
		texts[i] = strings.Repeat("x", i+1)
	}
	if _, err := CountAll(context.Background(), c, texts); err != nil {
		t.Fatal(err)
	}
	if p := peak.Load(); p > 4 {
		t.Fatalf("peak concurrency = %d, want <= 4", p)
	}
}

func TestCountAllReportsCounterErrorPanicAndCancel(t *testing.T) {
	boom := errors.New("boom")
	if _, err := CountAll(context.Background(), &fakeCounter{fail: func(string) error { return boom }}, []string{"a"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	p := countFunc(func(string) (int, error) { panic("kaboom") })
	if _, err := CountAll(context.Background(), p, []string{"a"}); err == nil {
		t.Fatal("panic in counter must become an error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CountAll(ctx, &fakeCounter{}, []string{"a", "b"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestUnchangedSmallResponseRecordsExactEqualCounts(t *testing.T) {
	c := &fakeCounter{}
	m := Unchanged(context.Background(), c, []string{"um dois", "três"}, 0)
	want := Measurement{Encoding: "fake", TokensBefore: 3, TokensAfter: 3, Exact: true}
	if m != want {
		t.Fatalf("m = %+v, want %+v", m, want)
	}
}

func TestUnchangedNonTextBlocksRecordBytesNotTokens(t *testing.T) {
	c := &fakeCounter{}
	m := Unchanged(context.Background(), c, []string{"um dois"}, 4096)
	if m.NonTextBytes != 4096 || m.TokensBefore != 2 || m.TokensAfter != 2 {
		t.Fatalf("m = %+v", m)
	}
	only := Unchanged(context.Background(), c, nil, 4096)
	if only.NonTextBytes != 4096 || only.TokensBefore != 0 || !only.Exact {
		t.Fatalf("only non-text: %+v", only)
	}
	if n := c.calls.Load(); n != 1 {
		t.Fatalf("non-text bytes must not be tokenized; Count calls = %d", n)
	}
}

func TestUnchangedAsyncDoesNotBlockTheSend(t *testing.T) {
	c := &fakeCounter{delay: 300 * time.Millisecond}
	// A resposta já foi entregue ao agente antes de a contagem começar.
	start := time.Now()
	ch := UnchangedAsync(context.Background(), c, []string{"a b c"}, 0)
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("UnchangedAsync blocked for %v", d)
	}
	select {
	case <-ch:
		t.Fatal("measurement arrived before the slow counter finished")
	default:
	}
	m := <-ch
	if m.TokensBefore != 3 || m.TokensBefore != m.TokensAfter || !m.Exact || m.Encoding != "fake" {
		t.Fatalf("m = %+v", m)
	}
}

func TestFailedCountMarksNotExact(t *testing.T) {
	boom := errors.New("boom")
	c := &fakeCounter{fail: func(string) error { return boom }}
	m := Unchanged(context.Background(), c, []string{"a b"}, 10)
	if m.Exact {
		t.Fatal("Exact must be false when counting fails")
	}
	if m.Encoding != "fake" || m.NonTextBytes != 10 {
		t.Fatalf("m = %+v", m)
	}
	if got := <-UnchangedAsync(context.Background(), c, []string{"a b"}, 0); got.Exact {
		t.Fatal("async: Exact must be false when counting fails")
	}
}

func TestTokensDoesNotImportPipelineMetricsMCPOrStore(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, dep := range strings.Fields(string(out)) {
		for _, banned := range []string{"/internal/pipeline", "/internal/metrics", "/internal/mcp", "/internal/store"} {
			if strings.HasSuffix(dep, banned) {
				t.Errorf("internal/tokens depends on %s", dep)
			}
		}
	}
}

type countFunc func(string) (int, error)

func (f countFunc) Count(s string) (int, error) { return f(s) }
func (countFunc) Encoding() string              { return "fake" }

func TestAsyncCountSurvivesCancelAfterSend(t *testing.T) {
	c := &fakeCounter{delay: 50 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	ch := UnchangedAsync(ctx, c, []string{"a b", "c d e"}, 0)
	cancel() // o agente cancelou a requisição depois do envio
	m := <-ch
	if !m.Exact || m.TokensBefore != 5 || m.TokensAfter != 5 {
		t.Fatalf("m = %+v, want Exact=true with 5 tokens", m)
	}
}

func TestAsyncCountHasItsOwnTimeout(t *testing.T) {
	old := asyncTimeout
	asyncTimeout = 30 * time.Millisecond
	defer func() { asyncTimeout = old }()
	c := &fakeCounter{delay: 100 * time.Millisecond}
	texts := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} // mais textos que workers
	m := <-UnchangedAsync(context.Background(), c, texts, 0)
	if m.Exact {
		t.Fatalf("m = %+v, want Exact=false after the async timeout", m)
	}
}

func TestMeasureAsyncCountsKeptTextsAfter(t *testing.T) {
	c := &fakeCounter{}
	m := <-MeasureAsync(context.Background(), nil, c, []string{"a b c", "d e", "f"}, []bool{true, false, true}, 7)
	if !m.Exact || m.TokensBefore != 6 || m.TokensAfter != 4 || m.NonTextBytes != 7 {
		t.Fatalf("m = %+v, want before=6 after=4 nonText=7", m)
	}
}

// O padrão do prazo próprio da contagem em segundo plano é 5s (AC da #13); o teste
// acima só prova que o prazo existe, usando um valor menor.
// blockingCounter segura cada Count até release fechar e avisa quando começa.
type blockingCounter struct {
	started chan struct{}
	release chan struct{}
	calls   atomic.Int64
}

func newBlockingCounter() *blockingCounter {
	return &blockingCounter{started: make(chan struct{}, 8), release: make(chan struct{})}
}

func (b *blockingCounter) Encoding() string { return "fake" }
func (b *blockingCounter) Count(string) (int, error) {
	b.calls.Add(1)
	b.started <- struct{}{}
	<-b.release
	return 1, nil
}

// Com o limite cheio a contagem exata não roda e não bloqueia: a medição sai com Exact=false.
func TestMeasureAsyncWithTheLimitFullSkipsTheCountAndIsNotExact(t *testing.T) {
	slow := newBlockingCounter()
	lim := NewLimiter(1)
	first := MeasureAsync(context.Background(), lim, slow, []string{"a"}, nil, 0)
	<-slow.started // a vaga única está ocupada

	other := &fakeCounter{}
	select {
	case m := <-MeasureAsync(context.Background(), lim, other, []string{"a b c"}, nil, 9):
		if m.Exact || m.TokensBefore != 0 || m.TokensAfter != 0 || m.NonTextBytes != 9 || m.Encoding != "fake" {
			t.Fatalf("m = %+v, want Exact=false, zero tokens, NonTextBytes=9", m)
		}
	case <-time.After(time.Second):
		t.Fatal("MeasureAsync blocked with the limit full")
	}
	if other.calls.Load() != 0 {
		t.Fatalf("the count ran %d times with the limit full", other.calls.Load())
	}

	close(slow.release)
	if m := <-first; !m.Exact {
		t.Fatalf("first = %+v, want Exact=true", m)
	}
}

func TestMeasureAsyncFreesItsSlotWhenTheCountEnds(t *testing.T) {
	lim := NewLimiter(1)
	c := &fakeCounter{}
	for i := range 3 { // cada contagem termina antes da próxima: a vaga única volta
		if m := <-MeasureAsync(context.Background(), lim, c, []string{"a b"}, nil, 0); !m.Exact {
			t.Fatalf("call %d: %+v, want Exact=true", i, m)
		}
	}
}

func TestAsyncCountDefaultTimeoutIs5s(t *testing.T) {
	if asyncTimeout != 5*time.Second {
		t.Fatalf("asyncTimeout = %v, want 5s", asyncTimeout)
	}
}
