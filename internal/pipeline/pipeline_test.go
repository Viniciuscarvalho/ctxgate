package pipeline_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Viniciuscarvalho/ctxgate/internal/pipeline"
	"github.com/Viniciuscarvalho/ctxgate/internal/tokens"
)

func realCounter(t testing.TB) tokens.Counter {
	t.Helper()
	c, err := tokens.New()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newPipeline(t testing.TB, stages ...pipeline.Stage) *pipeline.Pipeline {
	t.Helper()
	if len(stages) == 0 {
		stages = []pipeline.Stage{pipeline.ExactDedup{}}
	}
	return pipeline.New(pipeline.Options{Counter: realCounter(t), Stages: stages})
}

func run(t testing.TB, p *pipeline.Pipeline, texts ...string) pipeline.Outcome {
	t.Helper()
	return p.Run(context.Background(), pipeline.Input{Call: pipeline.Call{Tool: "t"}, Texts: texts})
}

func final(t testing.TB, o pipeline.Outcome) pipeline.Report {
	t.Helper()
	select {
	case r := <-o.Done:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("Done never delivered the final report")
		return pipeline.Report{}
	}
}

const para = "O servidor devolveu três linhas de log idênticas neste parágrafo."

func TestDedupKeepsFirstOccurrenceAndSavesTokens(t *testing.T) {
	in := para + "\n\nOutro parágrafo único.\n\n" + para + "\n\n" + para
	o := run(t, newPipeline(t), in)

	if want := para + "\n\nOutro parágrafo único.\n\n"; o.Texts[0] != want {
		t.Fatalf("text = %q, want %q", o.Texts[0], want)
	}
	r := o.Report
	if r.Mode != pipeline.ModeApplied || r.SkipReason != "" || r.Err != "" {
		t.Fatalf("report = %+v, want applied", r)
	}
	if !r.Exact || r.Encoding != "cl100k_base" || r.TokensAfter >= r.TokensBefore || r.TokensAfter == 0 {
		t.Fatalf("measurement = %+v, want exact cl100k with tokens_after < tokens_before", r.Measurement)
	}
	if len(r.Stages) != 1 || r.Stages[0].Name != "dedup" || r.Stages[0].Dropped != 2 || r.Stages[0].TokensDropped <= 0 {
		t.Fatalf("stages = %+v", r.Stages)
	}
	if final(t, o).TokensAfter != r.TokensAfter {
		t.Fatal("final report differs from the synchronous one for an exact count")
	}
}

func TestRunTable(t *testing.T) {
	fence := "```go\nx := 1\nx := 1\nx := 1\n```"
	cases := []struct {
		name  string
		texts []string
		want  []string
		mode  pipeline.Mode
	}{
		{"sem repetição sai idêntico", []string{"um\n\ndois\n\ntrês"}, []string{"um\n\ndois\n\ntrês"}, pipeline.ModePassthrough},
		{"bloco vazio", []string{""}, []string{""}, pipeline.ModePassthrough},
		{"sem blocos", nil, nil, pipeline.ModePassthrough},
		{"repetição entre blocos esvazia o segundo", []string{"a b c\n\nd e f", "a b c"}, []string{"a b c\n\nd e f", ""}, pipeline.ModeApplied},
		{"normaliza caixa, espaços e pontuação de borda", []string{"Olá   Mundo!\n\nolá mundo\n\n— OLÁ MUNDO..."}, []string{"Olá   Mundo!\n\n"}, pipeline.ModeApplied},
		{"parágrafos só de espaço não deduplicam", []string{"a\n\n \n\n \n\nb"}, []string{"a\n\n \n\n \n\nb"}, pipeline.ModePassthrough},
		{"separadores longos preservados", []string{"a\n\n\n\nb\n\n\n\na"}, []string{"a\n\n\n\nb\n\n\n\n"}, pipeline.ModeApplied},
		{"bloco de código com linhas repetidas não é partido", []string{"antes\n\n" + fence + "\n\ndepois"}, []string{"antes\n\n" + fence + "\n\ndepois"}, pipeline.ModePassthrough},
		{"bloco de código inteiro repetido sai uma vez", []string{fence + "\n\n" + fence + "\n\nfim"}, []string{fence + "\n\nfim"}, pipeline.ModeApplied},
		{"linha em branco dentro do código não parte o bloco", []string{"```\na\n\nb\n```\n\n```\na\n\nb\n```"}, []string{"```\na\n\nb\n```\n\n"}, pipeline.ModeApplied},
		{"CRLF e texto sem quebra final", []string{"a\r\n\r\nb\r\n\r\na"}, []string{"a\r\n\r\nb\r\n\r\n"}, pipeline.ModeApplied},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := run(t, newPipeline(t), tc.texts...)
			if len(o.Texts) != len(tc.texts) {
				t.Fatalf("len(Texts) = %d, want %d", len(o.Texts), len(tc.texts))
			}
			for i := range tc.want {
				if o.Texts[i] != tc.want[i] {
					t.Errorf("Texts[%d] = %q, want %q", i, o.Texts[i], tc.want[i])
				}
			}
			if o.Report.Mode != tc.mode {
				t.Errorf("Mode = %q, want %q (report %+v)", o.Report.Mode, tc.mode, o.Report)
			}
			if o.Report.TokensAfter > o.Report.TokensBefore {
				t.Errorf("tokens_after %d > tokens_before %d", o.Report.TokensAfter, o.Report.TokensBefore)
			}
			if tc.mode == pipeline.ModePassthrough && o.Report.TokensAfter != o.Report.TokensBefore {
				t.Errorf("passthrough must keep tokens: %+v", o.Report.Measurement)
			}
		})
	}
}

func TestLongParagraphIsSplitByLineAndDedupedPerLine(t *testing.T) {
	var lines []string
	for i := range 120 {
		lines = append(lines, fmt.Sprintf("linha %d com palavras suficientes para pesar alguns tokens aqui", i))
	}
	repeated := lines[7]
	input := strings.Join(lines, "\n") + "\n" + repeated // parágrafo único > 512 tokens
	want := strings.Join(lines, "\n") + "\n"
	o := run(t, newPipeline(t), input)
	if o.Texts[0] != want || o.Report.Mode != pipeline.ModeApplied {
		t.Fatalf("got mode %q, text ends %q", o.Report.Mode, o.Texts[0][max(0, len(o.Texts[0])-80):])
	}

	// Parágrafo curto com a mesma repetição não é subdividido: sai idêntico.
	short := "a b\nc d\na b"
	if o := run(t, newPipeline(t), short); o.Texts[0] != short {
		t.Fatalf("short paragraph changed: %q", o.Texts[0])
	}
}

func TestHugeSingleLineIsOneChunk(t *testing.T) {
	line := strings.Repeat(`{"k":"v"},`, 2000)
	o := run(t, newPipeline(t), line+"\n\n"+line)
	if o.Texts[0] != line+"\n\n" || o.Report.Mode != pipeline.ModeApplied {
		t.Fatalf("mode %q, len %d", o.Report.Mode, len(o.Texts[0]))
	}
	if o := run(t, newPipeline(t), line); o.Texts[0] != line {
		t.Fatal("single huge line must pass intact")
	}
}

func TestSkippedInputPassesIntactWithReason(t *testing.T) {
	text := para + "\n\n" + para
	in := pipeline.Input{Texts: []string{text}, NonTextBytes: 123, SkipReason: "is_error"}
	o := newPipeline(t).Run(context.Background(), in)
	if o.Texts[0] != text || o.Report.Mode != pipeline.ModePassthrough || o.Report.SkipReason != "is_error" {
		t.Fatalf("outcome = %+v", o)
	}
	f := final(t, o)
	if !f.Exact || f.TokensBefore == 0 || f.TokensBefore != f.TokensAfter || f.NonTextBytes != 123 || f.SkipReason != "is_error" {
		t.Fatalf("final = %+v, want exact, before == after, non-text bytes kept", f)
	}
}

type stageFunc struct {
	name string
	fn   func(context.Context, *pipeline.Doc) error
}

func (s stageFunc) Name() string                                     { return s.name }
func (s stageFunc) Apply(ctx context.Context, d *pipeline.Doc) error { return s.fn(ctx, d) }

func TestStageFailureFallsBackToTheOriginalResponse(t *testing.T) {
	text := para + "\n\n" + para
	cases := []struct {
		name    string
		stage   func(context.Context, *pipeline.Doc) error
		timeout time.Duration
		errHas  string
	}{
		{"erro", func(context.Context, *pipeline.Doc) error { return errors.New("boom") }, 0, "boom"},
		{"panic", func(context.Context, *pipeline.Doc) error { panic("kaboom") }, 0, "kaboom"},
		{"panic de ponteiro nulo", func(context.Context, *pipeline.Doc) error { var d *pipeline.Doc; _ = d.Chunks; return nil }, 0, "panic"},
		{"timeout (estágio que ignora ctx)", func(context.Context, *pipeline.Doc) error { time.Sleep(2 * time.Second); return nil }, 30 * time.Millisecond, "deadline"},
		{"timeout (estágio que respeita ctx)", func(ctx context.Context, _ *pipeline.Doc) error { <-ctx.Done(); return ctx.Err() }, 30 * time.Millisecond, "deadline"},
		{"estágio edita o texto", func(_ context.Context, d *pipeline.Doc) error { d.Chunks[0].Text = "inventado"; return nil }, 0, "invariant"},
		{"estágio ressuscita trecho", func(_ context.Context, d *pipeline.Doc) error { d.Chunks[1].Keep = true; return nil }, 0, "invariant"},
		{"estágio cria trecho", func(_ context.Context, d *pipeline.Doc) error {
			d.Chunks = append(d.Chunks, pipeline.Chunk{Text: "novo", Keep: true})
			return nil
		}, 0, "invariant"},
		{"estágio remove tudo", func(_ context.Context, d *pipeline.Doc) error {
			for i := range d.Chunks {
				d.Chunks[i].Keep = false
			}
			return nil
		}, 0, "invariant"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			p := pipeline.New(pipeline.Options{
				Counter: realCounter(t),
				Stages:  []pipeline.Stage{pipeline.ExactDedup{}, stageFunc{"bad", tc.stage}},
				Timeout: tc.timeout,
				Logger:  slog.New(slog.NewTextHandler(&logs, nil)),
			})
			start := time.Now()
			o := run(t, p, text)
			if time.Since(start) > time.Second {
				t.Fatalf("Run took %s, the timeout must bound it", time.Since(start))
			}
			if o.Texts[0] != text || len(o.Texts) != 1 {
				t.Fatalf("texts = %q, want the original response", o.Texts)
			}
			r := o.Report
			if r.Mode != pipeline.ModeFallback || !strings.Contains(r.Err, tc.errHas) {
				t.Fatalf("report = %+v, want fallback with %q", r, tc.errHas)
			}
			if !strings.Contains(logs.String(), "fallback") || !strings.Contains(logs.String(), tc.errHas) {
				t.Fatalf("error was not logged: %q", logs.String())
			}
			if f := final(t, o); !f.Exact || f.TokensBefore != f.TokensAfter || f.Mode != pipeline.ModeFallback {
				t.Fatalf("final = %+v, want the original measured exactly", f)
			}
		})
	}
}

// dropMarked é um segundo estágio de teste: ele só usa a interface Stage.
type dropMarked struct{}

func (dropMarked) Name() string { return "drop-marked" }
func (dropMarked) Apply(_ context.Context, d *pipeline.Doc) error {
	for i := range d.Chunks {
		if strings.Contains(d.Chunks[i].Text, "RUÍDO") && d.Chunks[i].Keep {
			d.Chunks[i].Keep, d.Chunks[i].Reason = false, "noise"
		}
	}
	return nil
}

func TestSecondStageRegistersThroughStageInterface(t *testing.T) {
	p := newPipeline(t, pipeline.ExactDedup{}, dropMarked{})
	o := run(t, p, "útil\n\nRUÍDO 1\n\nútil\n\nfim")
	if o.Texts[0] != "útil\n\nfim" || o.Report.Mode != pipeline.ModeApplied {
		t.Fatalf("texts = %q, report = %+v", o.Texts, o.Report)
	}
	names := []string{o.Report.Stages[0].Name, o.Report.Stages[1].Name}
	if names[0] != "dedup" || names[1] != "drop-marked" || o.Report.Stages[1].Dropped != 1 {
		t.Fatalf("stages = %+v", o.Report.Stages)
	}
}

func TestInvariantsOnGeneratedResponses(t *testing.T) {
	p := newPipeline(t)
	for seed := range 150 {
		blocks := generated(seed)
		o := p.Run(context.Background(), pipeline.Input{Texts: blocks})
		if len(o.Texts) != len(blocks) {
			t.Fatalf("seed %d: %d blocks out, %d in", seed, len(o.Texts), len(blocks))
		}
		if o.Report.Mode == pipeline.ModeFallback {
			t.Fatalf("seed %d: unexpected fallback: %s", seed, o.Report.Err)
		}
		if o.Report.TokensAfter > o.Report.TokensBefore {
			t.Fatalf("seed %d: tokens_after %d > tokens_before %d", seed, o.Report.TokensAfter, o.Report.TokensBefore)
		}
		inputParas := map[string]bool{}
		for _, b := range blocks {
			for _, ptxt := range strings.SplitAfter(b, "\n\n") {
				inputParas[ptxt] = true
			}
		}
		seen := map[string]bool{}
		for _, b := range o.Texts {
			for _, ptxt := range strings.SplitAfter(b, "\n\n") {
				if ptxt == "" {
					continue
				}
				if !inputParas[ptxt] { // saída ⊆ entrada
					t.Fatalf("seed %d: output paragraph %q is not in the input", seed, ptxt)
				}
				if key := strings.TrimSpace(ptxt); seen[key] { // primeira ocorrência apenas
					t.Fatalf("seed %d: paragraph %q survived twice", seed, key)
				} else {
					seen[key] = true
				}
			}
		}
		if len(strings.Join(blocks, "")) > 0 && len(strings.Join(o.Texts, "")) == 0 {
			t.Fatalf("seed %d: everything was removed", seed)
		}
	}
}

func TestRunIsSafeForConcurrentUse(t *testing.T) {
	p := newPipeline(t)
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			text := fmt.Sprintf("p%d\n\nq\n\np%d\n\nq", i, i)
			o := p.Run(context.Background(), pipeline.Input{Texts: []string{text}})
			if want := fmt.Sprintf("p%d\n\nq\n\n", i); o.Texts[0] != want {
				t.Errorf("call %d: %q, want %q", i, o.Texts[0], want)
			}
		}()
	}
	wg.Wait()
}

// ---- contagem exata no caminho crítico só até 256 KB ----

type slowCounter struct {
	tokens.Counter
	gate  chan struct{} // se não nulo, Count espera aqui
	delay time.Duration
	calls atomic.Int64
	err   error
}

func (c *slowCounter) Count(s string) (int, error) {
	c.calls.Add(1)
	if c.gate != nil {
		<-c.gate
	}
	time.Sleep(c.delay)
	if c.err != nil {
		return 0, c.err
	}
	return c.Counter.Count(s)
}

func bigText(bytes int) string {
	var b strings.Builder
	for i := 0; b.Len() < bytes; i++ {
		fmt.Fprintf(&b, "Parágrafo %d do relatório com texto de enchimento suficiente.\n\n", i%50)
	}
	return b.String()
}

func TestResponseUpTo256KBIsCountedExactlyInTheCriticalPath(t *testing.T) {
	c := &slowCounter{Counter: realCounter(t)}
	p := pipeline.New(pipeline.Options{Counter: c, Stages: []pipeline.Stage{pipeline.ExactDedup{}}})
	o := run(t, p, bigText(250<<10))
	if !o.Report.Exact || c.calls.Load() == 0 || o.Report.TokensAfter >= o.Report.TokensBefore {
		t.Fatalf("report = %+v, calls = %d, want exact count in Run", o.Report, c.calls.Load())
	}
}

func TestResponseOver256KBCutsOnEstimateAndCountsLater(t *testing.T) {
	gate := make(chan struct{})
	c := &slowCounter{Counter: realCounter(t), gate: gate}
	// Timeout folgado: o teste prova a contagem fora do caminho crítico, não a latência (sob -race).
	p := pipeline.New(pipeline.Options{Counter: c, Stages: []pipeline.Stage{pipeline.ExactDedup{}}, Timeout: 10 * time.Second})
	text := bigText(2 << 20) // 2 MiB

	done := make(chan pipeline.Outcome, 1)
	go func() { done <- run(t, p, text) }()
	var o pipeline.Outcome
	select {
	case o = <-done: // Run voltou com a contagem exata ainda bloqueada: ela não está no caminho crítico
	case <-time.After(2 * time.Second):
		t.Fatal("Run waited for the exact count of a 2 MiB response")
	}
	r := o.Report
	if r.Exact || r.Mode != pipeline.ModeApplied || r.TokensAfter >= r.TokensBefore {
		t.Fatalf("report = %+v, want applied with estimated tokens", r)
	}
	if want := (len(text) + 2) / 3; r.TokensBefore < want || r.TokensBefore > want*105/100 {
		t.Fatalf("estimate = %d, want bytes/3 = %d up to +5%% of per-chunk rounding", r.TokensBefore, want)
	}
	if len(o.Texts[0]) >= len(text) {
		t.Fatal("the 2 MiB response was not reduced")
	}

	close(gate)
	f := final(t, o)
	if !f.Exact || f.TokensAfter >= f.TokensBefore {
		t.Fatalf("final = %+v, want exact", f)
	}
	if f.TokensBefore == r.TokensBefore {
		t.Fatalf("final tokens equal the estimate (%d): exact count did not run", f.TokensBefore)
	}
}

func TestAsyncCountSurvivesRequestCancellation(t *testing.T) {
	c := &slowCounter{Counter: realCounter(t), delay: 20 * time.Millisecond}
	p := pipeline.New(pipeline.Options{Counter: c, Stages: []pipeline.Stage{pipeline.ExactDedup{}}})
	for name, in := range map[string]pipeline.Input{
		"corte com estimativa": {Texts: []string{bigText(300 << 10)}},
		"passthrough":          {Texts: []string{para}, SkipReason: "is_error"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			o := p.Run(ctx, in)
			cancel() // o agente cancelou a requisição depois do envio
			if f := final(t, o); !f.Exact || f.TokensBefore == 0 {
				t.Fatalf("final = %+v, want Exact=true after the request was cancelled", f)
			}
		})
	}
}

func TestCounterErrorOnlyMarksNotExact(t *testing.T) {
	c := &slowCounter{Counter: realCounter(t), err: errors.New("tokenizer quebrou")}
	var logs bytes.Buffer
	p := pipeline.New(pipeline.Options{Counter: c, Stages: []pipeline.Stage{pipeline.ExactDedup{}}, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	o := run(t, p, para+"\n\n"+para)
	if o.Texts[0] != para+"\n\n" || o.Report.Mode != pipeline.ModeApplied || o.Report.Exact || o.Report.Err != "" {
		t.Fatalf("outcome = %+v, want the response still reduced, Exact=false and no Err", o)
	}
	if f := final(t, o); f.Exact {
		t.Fatalf("final = %+v, want Exact=false", f)
	}
	if !strings.Contains(logs.String(), "tokenizer quebrou") {
		t.Fatalf("counter error not logged: %q", logs.String())
	}
}
