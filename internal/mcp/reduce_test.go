package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Viniciuscarvalho/ctxgate/internal/pipeline"
	"github.com/Viniciuscarvalho/ctxgate/internal/testutil/mcpfake"
	"github.com/Viniciuscarvalho/ctxgate/internal/tokens"
)

// harness liga um Relay a pipes: o teste faz o papel do agente e do upstream.
type harness struct {
	t           testing.TB
	agentIn     *io.PipeWriter
	agentOut    *bufio.Reader
	upstreamIn  *bufio.Reader
	upstreamOut *io.PipeWriter
	served      <-chan struct{} // fecha quando o Serve volta
}

func newHarness(t *testing.T, r Relay) *harness {
	t.Helper()
	agentInR, agentInW := io.Pipe()
	agentOutR, agentOutW := io.Pipe()
	upInR, upInW := io.Pipe()
	upOutR, upOutW := io.Pipe()
	r.UpstreamIn, r.UpstreamOut = upInW, upOutR
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = r.Serve(ctx, agentInR, agentOutW)
		_ = agentOutW.Close()
		close(done)
	}()
	t.Cleanup(func() {
		_ = agentInW.Close()
		_ = upOutW.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("relay did not stop")
		}
		cancel()
	})
	// O fake lê tudo o que o relay manda ao upstream para o pipe nunca travar.
	return &harness{t: t, agentIn: agentInW, agentOut: bufio.NewReaderSize(agentOutR, 1<<20), upstreamIn: bufio.NewReaderSize(upInR, 1<<20), upstreamOut: upOutW, served: done}
}

func readLine(t testing.TB, r *bufio.Reader) string {
	t.Helper()
	type res struct {
		line string
		err  error
	}
	ch := make(chan res, 1)
	go func() {
		line, err := r.ReadString('\n')
		ch <- res{line, err}
	}()
	select {
	case got := <-ch:
		if got.err != nil {
			t.Fatalf("read: %v", got.err)
		}
		return got.line
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a line")
		return ""
	}
}

// roundTrip manda request ao upstream, responde response e devolve o que o agente recebeu.
func (h *harness) roundTrip(request, response string) string {
	h.t.Helper()
	go func() { _, _ = io.WriteString(h.agentIn, request+"\n") }()
	readLine(h.t, h.upstreamIn)
	go func() { _, _ = io.WriteString(h.upstreamOut, response+"\n") }()
	return strings.TrimSuffix(readLine(h.t, h.agentOut), "\n")
}

type fakeReducer struct {
	mu      sync.Mutex
	inputs  []pipeline.Input
	outcome func(pipeline.Input) pipeline.Outcome
}

func (f *fakeReducer) Run(_ context.Context, in pipeline.Input) pipeline.Outcome {
	f.mu.Lock()
	f.inputs = append(f.inputs, in)
	f.mu.Unlock()
	return f.outcome(in)
}

func (f *fakeReducer) calls() []pipeline.Input {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pipeline.Input(nil), f.inputs...)
}

func applied(texts ...string) func(pipeline.Input) pipeline.Outcome {
	return func(pipeline.Input) pipeline.Outcome {
		return pipeline.Outcome{Texts: texts, Report: pipeline.Report{Mode: pipeline.ModeApplied}}
	}
}

const callRequest = `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"search","arguments":{"query":"café"}}}`

func result(body string) string {
	return `{"jsonrpc":"2.0","id":7,"result":` + body + `}`
}

func TestReducerRewritesOnlyTheTextBlocks(t *testing.T) {
	red := &fakeReducer{outcome: applied("A", "", "C")}
	h := newHarness(t, Relay{Reducer: red})
	original := result(`{"content":[{"type":"text","text":"a"},{"type":"text","text":"b","annotations":{"audience":["user"]}},{"type":"text","text":"c","annotations":{"priority":1}}],"_meta":{"k":"v"},"extra":[1,2]}`)

	got := h.roundTrip(callRequest, original)

	var env struct {
		ID      json.RawMessage `json:"id"`
		JSONRPC string          `json:"jsonrpc"`
		Result  struct {
			Content []map[string]json.RawMessage `json:"content"`
			Meta    json.RawMessage              `json:"_meta"`
			Extra   json.RawMessage              `json:"extra"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(got), &env); err != nil {
		t.Fatalf("response is not JSON: %v\n%s", err, got)
	}
	if string(env.ID) != "7" || env.JSONRPC != "2.0" || string(env.Result.Meta) != `{"k":"v"}` || string(env.Result.Extra) != `[1,2]` {
		t.Fatalf("envelope or untouched fields changed: %s", got)
	}
	if len(env.Result.Content) != 2 || string(env.Result.Content[0]["text"]) != `"A"` || string(env.Result.Content[1]["text"]) != `"C"` ||
		string(env.Result.Content[1]["annotations"]) != `{"priority":1}` {
		t.Fatalf("content = %v, want blocks A and C (b removed) with their annotations", env.Result.Content)
	}
	in := red.calls()
	if len(in) != 1 || in[0].Call.Tool != "search" || string(in[0].Call.Args) != `{"query":"café"}` ||
		strings.Join(in[0].Texts, "|") != "a|b|c" || in[0].SkipReason != "" {
		t.Fatalf("reducer input = %+v", in)
	}
}

func TestIneligibleResponsesArriveIntactWithSkipReason(t *testing.T) {
	text := `{"type":"text","text":"olá"}`
	cases := []struct {
		name, body, reason string
		wantTexts          string
		nonText            bool
	}{
		{"isError", `{"content":[` + text + `],"isError":true}`, "is_error", "olá", false},
		{"structuredContent", `{"content":[` + text + `],"structuredContent":{"a":1}}`, "structured_content", "olá", false},
		{"input_required", `{"resultType":"input_required","content":[` + text + `]}`, "input_required", "olá", false},
		{"resultType desconhecido", `{"resultType":"streaming","content":[` + text + `]}`, "result_type", "olá", false},
		{"image", `{"content":[` + text + `,{"type":"image","data":"AAAA","mimeType":"image/png"}]}`, "non_text_block", "olá", true},
		{"audio", `{"content":[{"type":"audio","data":"AAAA","mimeType":"audio/wav"}]}`, "non_text_block", "", true},
		{"resource", `{"content":[{"type":"resource","resource":{"uri":"file:///a","text":"x"}}]}`, "non_text_block", "", true},
		{"resource_link", `{"content":[` + text + `,{"type":"resource_link","uri":"file:///a","name":"a"}]}`, "non_text_block", "olá", true},
		{"tipo desconhecido", `{"content":[{"type":"hologram","x":1}]}`, "non_text_block", "", true},
		{"sem content", `{"foo":1}`, "no_text", "", false},
		{"content vazio", `{"content":[]}`, "no_text", "", false},
		{"result que não é objeto", `"texto"`, "no_text", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			red := &fakeReducer{outcome: func(in pipeline.Input) pipeline.Outcome {
				return pipeline.Outcome{Texts: in.Texts, Report: pipeline.Report{Mode: pipeline.ModePassthrough, SkipReason: in.SkipReason}}
			}}
			h := newHarness(t, Relay{Reducer: red})
			original := result(tc.body)
			if got := h.roundTrip(callRequest, original); got != original {
				t.Fatalf("response changed:\n got %s\nwant %s", got, original)
			}
			in := red.calls()
			if len(in) != 1 || in[0].SkipReason != tc.reason || strings.Join(in[0].Texts, "|") != tc.wantTexts || (in[0].NonTextBytes > 0) != tc.nonText {
				t.Fatalf("reducer input = %+v, want skip %q texts %q nonText=%v", in, tc.reason, tc.wantTexts, tc.nonText)
			}
		})
	}
}

func TestResultTypeCompleteIsEligible(t *testing.T) {
	red := &fakeReducer{outcome: applied("curto")}
	h := newHarness(t, Relay{Reducer: red})
	got := h.roundTrip(callRequest, result(`{"resultType":"complete","content":[{"type":"text","text":"longo"}]}`))
	if !strings.Contains(got, `"curto"`) || red.calls()[0].SkipReason != "" {
		t.Fatalf("got %s, input %+v", got, red.calls())
	}
}

func TestUnappliedOutcomesKeepTheOriginalBytes(t *testing.T) {
	original := result(`{ "content" : [ {"text":"a","type":"text"} ] }`) // espaçamento e ordem de chaves próprios
	for name, outcome := range map[string]pipeline.Outcome{
		"passthrough":    {Texts: []string{"a"}, Report: pipeline.Report{Mode: pipeline.ModePassthrough}},
		"fallback":       {Texts: []string{"a"}, Report: pipeline.Report{Mode: pipeline.ModeFallback, Err: "boom"}},
		"tamanho errado": {Texts: []string{"a", "b"}, Report: pipeline.Report{Mode: pipeline.ModeApplied}},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, Relay{Reducer: &fakeReducer{outcome: func(pipeline.Input) pipeline.Outcome { return outcome }}})
			if got := h.roundTrip(callRequest, original); got != original {
				t.Fatalf("got %s", got)
			}
		})
	}
}

func TestReducerPanicFailsOpen(t *testing.T) {
	h := newHarness(t, Relay{Reducer: &fakeReducer{outcome: func(pipeline.Input) pipeline.Outcome { panic("kaboom") }}})
	original := result(`{"content":[{"type":"text","text":"a"}]}`)
	if got := h.roundTrip(callRequest, original); got != original {
		t.Fatalf("got %s", got)
	}
}

func TestOnlyToolCallResultsReachTheReducer(t *testing.T) {
	red := &fakeReducer{outcome: applied("X")}
	h := newHarness(t, Relay{Reducer: red})
	listing := `{"jsonrpc":"2.0","id":1,"result":{"tools":[],"content":[{"type":"text","text":"a"}]}}`
	if got := h.roundTrip(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, listing); got != listing {
		t.Fatalf("tools/list response changed: %s", got)
	}
	rpcErr := `{"jsonrpc":"2.0","id":7,"error":{"code":-32000,"message":"boom"}}`
	if got := h.roundTrip(callRequest, rpcErr); got != rpcErr {
		t.Fatalf("JSON-RPC error changed: %s", got)
	}
	if n := len(red.calls()); n != 0 {
		t.Fatalf("reducer called %d times, want 0", n)
	}
}

func TestResponsesAreMatchedToTheirCallsOutOfOrder(t *testing.T) {
	red := &fakeReducer{outcome: applied("X")}
	h := newHarness(t, Relay{Reducer: red})
	go func() {
		_, _ = io.WriteString(h.agentIn, `{"jsonrpc":"2.0","id":"a","method":"tools/call","params":{"name":"um"}}`+"\n")
		_, _ = io.WriteString(h.agentIn, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"dois"}}`+"\n")
	}()
	readLine(t, h.upstreamIn)
	readLine(t, h.upstreamIn)
	go func() {
		_, _ = io.WriteString(h.upstreamOut, `{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"r2"}]}}`+"\n")
		_, _ = io.WriteString(h.upstreamOut, `{"jsonrpc":"2.0","id":"a","result":{"content":[{"type":"text","text":"r1"}]}}`+"\n")
	}()
	readLine(t, h.agentOut)
	readLine(t, h.agentOut)
	got := map[string]string{} // as respostas são reduzidas em paralelo: a ordem não vale
	for _, in := range red.calls() {
		got[in.Call.Tool] = in.Texts[0]
	}
	if len(got) != 2 || got["dois"] != "r2" || got["um"] != "r1" {
		t.Fatalf("inputs = %+v", got)
	}
}

func TestFinalReportIsDeliveredAfterTheResponseWasSent(t *testing.T) {
	release := make(chan struct{})
	red := &fakeReducer{outcome: func(pipeline.Input) pipeline.Outcome {
		done := make(chan pipeline.Report, 1)
		go func() { <-release; done <- pipeline.Report{Mode: pipeline.ModeApplied} }()
		return pipeline.Outcome{Texts: []string{"X"}, Report: pipeline.Report{Mode: pipeline.ModeApplied}, Done: done}
	}}
	reports := make(chan pipeline.Report, 1)
	h := newHarness(t, Relay{Reducer: red, OnReport: func(r pipeline.Report) { reports <- r }})
	h.roundTrip(callRequest, result(`{"content":[{"type":"text","text":"a"}]}`)) // a resposta chegou
	select {
	case <-reports:
		t.Fatal("report delivered before the exact count finished")
	default:
	}
	close(release)
	select {
	case r := <-reports:
		if r.Mode != pipeline.ModeApplied {
			t.Fatalf("report = %+v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("final report never delivered")
	}
}

// ---- com o pipeline de verdade e o upstream fake ----

func realPipeline(t testing.TB) *pipeline.Pipeline {
	t.Helper()
	counter, err := tokens.New()
	if err != nil {
		t.Fatal(err)
	}
	return pipeline.New(pipeline.Options{Counter: counter, Stages: []pipeline.Stage{pipeline.ExactDedup{}}})
}

// fakeRelay devolve um harness cujo upstream é o mcpfake com uma ferramenta "t" de texto fixo.
func fakeRelay(t *testing.T, text string, reports chan<- pipeline.Report) *harness {
	t.Helper()
	h, _ := fakeRelayWith(t, text, realPipeline(t), reports)
	return h
}

// fakeRelayWith aceita qualquer Reducer (nil = passthrough) e devolve o cancel do ctx do relay.
func fakeRelayWith(t testing.TB, text string, red Reducer, reports chan<- pipeline.Report) (*harness, context.CancelFunc) {
	t.Helper()
	cfg := mcpfake.Config{Tools: []mcpfake.Tool{{Name: "t", Text: text}}}
	upInR, upInW := io.Pipe()
	upOutR, upOutW := io.Pipe()
	go func() {
		_ = cfg.Serve(upInR, upOutW)
		_ = upOutW.Close()
	}()
	agentInR, agentInW := io.Pipe()
	agentOutR, agentOutW := io.Pipe()
	r := Relay{UpstreamIn: upInW, UpstreamOut: upOutR, Reducer: red, OnReport: func(rep pipeline.Report) {
		if reports != nil {
			reports <- rep
		}
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- r.Serve(ctx, agentInR, agentOutW)
		_ = agentOutW.Close()
	}()
	t.Cleanup(func() {
		_ = agentInW.Close()
		<-done
		cancel()
	})
	return &harness{t: t, agentIn: agentInW, agentOut: bufio.NewReaderSize(agentOutR, 1<<20)}, cancel
}

func (h *harness) callTool(name string) (text string) {
	h.t.Helper()
	req := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":%q,"arguments":{}}}`, name)
	go func() { _, _ = io.WriteString(h.agentIn, req+"\n") }()
	var resp struct {
		Result struct {
			Content []struct{ Text string } `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(readLine(h.t, h.agentOut)), &resp); err != nil || len(resp.Result.Content) != 1 {
		h.t.Fatalf("bad response: %v %+v", err, resp)
	}
	return resp.Result.Content[0].Text
}

func finalReport(t testing.TB, reports <-chan pipeline.Report) pipeline.Report {
	t.Helper()
	select {
	case r := <-reports:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("no report")
		return pipeline.Report{}
	}
}

func TestEndToEndTripleParagraphArrivesOnce(t *testing.T) {
	const para = "O mesmo parágrafo, repetido pelo upstream."
	reports := make(chan pipeline.Report, 1)
	h := fakeRelay(t, para+"\n\n"+para+"\n\n"+para, reports)
	if got := h.callTool("t"); got != para+"\n\n" {
		t.Fatalf("agent received %q", got)
	}
	r := finalReport(t, reports)
	if r.Mode != pipeline.ModeApplied || !r.Exact || r.TokensAfter >= r.TokensBefore {
		t.Fatalf("report = %+v", r)
	}
}

func TestEndToEndResponseWithoutRepetitionIsIdentical(t *testing.T) {
	text := "um\n\ndois\n\ntrês ☕"
	reports := make(chan pipeline.Report, 1)
	h := fakeRelay(t, text, reports)
	if got := h.callTool("t"); got != text {
		t.Fatalf("agent received %q", got)
	}
	if r := finalReport(t, reports); r.Mode != pipeline.ModePassthrough || r.TokensAfter != r.TokensBefore || !r.Exact {
		t.Fatalf("report = %+v", r)
	}
}

// Resposta >256 KB: o corte usa bytes/3 e o trecho relevante, marcado no fixture, sobrevive.
func TestEndToEndLargeResponseKeepsTheRelevantPassage(t *testing.T) {
	const relevant = "RELEVANTE: o código da conta é 7731-X."
	var b strings.Builder
	for i := 0; b.Len() < 150<<10; i++ {
		fmt.Fprintf(&b, "Linha de enchimento %d que o upstream repete várias vezes.\n\n", i%40)
	}
	b.WriteString(relevant + "\n\n")
	for i := 0; b.Len() < 300<<10; i++ {
		fmt.Fprintf(&b, "Linha de enchimento %d que o upstream repete várias vezes.\n\n", i%40)
	}
	text := b.String()
	if len(text) <= 256<<10 {
		t.Fatal("fixture must exceed 256 KB")
	}
	reports := make(chan pipeline.Report, 1)
	h := fakeRelay(t, text, reports)

	got := h.callTool("t")
	if !strings.Contains(got, relevant) || len(got) >= len(text)/2 {
		t.Fatalf("delivered %d of %d bytes, relevant passage kept = %v", len(got), len(text), strings.Contains(got, relevant))
	}
	r := finalReport(t, reports)
	if r.Mode != pipeline.ModeApplied || !r.Exact || r.TokensAfter >= r.TokensBefore {
		t.Fatalf("report = %+v", r)
	}
}

func TestEndToEnd2MiBSingleLineArrivesIntact(t *testing.T) {
	text := strings.Repeat("uma palavra qualquer ", (2<<20)/21+1)
	reports := make(chan pipeline.Report, 1)
	h := fakeRelay(t, text, reports)
	if got := h.callTool("t"); got != text {
		t.Fatalf("2 MiB response changed (%d bytes)", len(got))
	}
	if r := finalReport(t, reports); r.Mode != pipeline.ModePassthrough || !r.Exact || r.TokensBefore != r.TokensAfter {
		t.Fatalf("report = %+v", r)
	}
}

// O contexto do relay (a requisição) é cancelado logo depois do envio; a contagem
// exata assíncrona precisa terminar mesmo assim.
func TestEndToEndCountSurvivesCancelAfterSend(t *testing.T) {
	text := strings.Repeat("uma palavra qualquer ", (2<<20)/21+1) // >256 KB: a contagem exata fica para depois
	reports := make(chan pipeline.Report, 1)
	h, cancel := fakeRelayWith(t, text, realPipeline(t), reports)
	if got := h.callTool("t"); got != text {
		t.Fatalf("response changed (%d bytes)", len(got))
	}
	cancel()
	if r := finalReport(t, reports); !r.Exact || r.TokensBefore == 0 {
		t.Fatalf("report after cancel = %+v, want Exact with tokens counted", r)
	}
}

func p95(d []time.Duration) time.Duration {
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[len(d)*95/100]
}

// roundTripTimes mede n idas e voltas de tools/call (agente → relay → mcpfake → relay → agente).
func roundTripTimes(t testing.TB, text string, red Reducer, n int) []time.Duration {
	h, _ := fakeRelayWith(t, text, red, nil)
	times := make([]time.Duration, 0, n)
	for i := 0; i < n+5; i++ {
		start := time.Now()
		_ = h.callTool("t")
		if i >= 5 { // descarta o aquecimento
			times = append(times, time.Since(start))
		}
	}
	return times
}

func mixedText(size int) string {
	var b strings.Builder
	for i := 0; b.Len() < size; i++ {
		fmt.Fprintf(&b, "Registro %d: status ok, latência estável e nenhuma pendência aberta.\n\n", i%60)
	}
	return b.String()
}

// Gate do ticket: overhead do relay com o pipeline, p95 < 20 ms até 100 KB.
// O overhead é p95(com pipeline) − mediana(sem pipeline) sobre o mesmo upstream.
func TestRelayOverheadP95Under20msUpTo100KB(t *testing.T) {
	if raceEnabled || testing.Short() {
		t.Skip("medição de latência: sem -race e sem -short")
	}
	for _, size := range []int{10 << 10, 50 << 10, 100 << 10} {
		text := mixedText(size)
		base := roundTripTimes(t, text, nil, 100)
		sort.Slice(base, func(i, j int) bool { return base[i] < base[j] })
		with := roundTripTimes(t, text, realPipeline(t), 100)
		overhead := p95(with) - base[len(base)/2]
		t.Logf("%3d KB: p95 com pipeline %v, mediana sem %v, overhead p95 %v", size>>10, p95(with), base[len(base)/2], overhead)
		if overhead >= 20*time.Millisecond {
			t.Errorf("%d KB: overhead p95 %v, want < 20ms", size>>10, overhead)
		}
	}
}

func BenchmarkRelayWithPipeline(b *testing.B) {
	for _, size := range []int{10 << 10, 100 << 10} {
		text := mixedText(size)
		b.Run(fmt.Sprintf("%dKB", size>>10), func(b *testing.B) {
			times := roundTripTimes(b, text, realPipeline(b), max(b.N, 20))
			b.ReportMetric(float64(p95(times).Microseconds())/1000, "p95-ms")
		})
	}
}

// ---- limite de chamadas em voo (max_in_flight, ADR-001 §3) ----

// sendCalls faz o agente mandar um tools/call por id e espera o upstream recebê-los.

func (h *harness) sendCalls(ids ...int) {
	go func() {
		for _, id := range ids {
			req := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":"t","arguments":{}}}`, id)
			_, _ = io.WriteString(h.agentIn, req+"\n")
		}
	}()
	for range ids {
		readLine(h.t, h.upstreamIn)
	}
}

// respond faz o upstream responder ao id; o canal fecha quando o relay leu a linha inteira.
func (h *harness) respond(id int, text string) <-chan struct{} {
	line := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{"content":[{"type":"text","text":%q}]}}`, id, text)
	done := make(chan struct{})
	go func() { _, _ = io.WriteString(h.upstreamOut, line+"\n"); close(done) }()
	return done
}

func (h *harness) receivedID() int {
	h.t.Helper()
	var resp struct{ ID int }
	if err := json.Unmarshal([]byte(readLine(h.t, h.agentOut)), &resp); err != nil {
		h.t.Fatal(err)
	}
	return resp.ID
}

func TestWithMaxInFlightCallsTheNextOneWaits(t *testing.T) {
	const limit = 16
	entered := make(chan struct{}, limit+1)
	release := make(chan struct{})
	red := &fakeReducer{outcome: func(in pipeline.Input) pipeline.Outcome {
		entered <- struct{}{}
		<-release
		return pipeline.Outcome{Texts: in.Texts, Report: pipeline.Report{Mode: pipeline.ModePassthrough}}
	}}
	h := newHarness(t, Relay{Reducer: red}) // MaxInFlight zero: padrão 16
	ids := make([]int, limit+1)
	for i := range ids {
		ids[i] = i + 1
	}
	h.sendCalls(ids...)
	for _, id := range ids {
		h.respond(id, "x")
	}
	for range limit {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the first 16 calls should be in flight")
		}
	}
	select {
	case <-entered:
		t.Fatal("the 17th call started while 16 were in flight")
	case <-time.After(100 * time.Millisecond):
	}
	release <- struct{}{} // uma sai (o agente lê a resposta)...
	h.receivedID()
	select {
	case <-entered: // ...e a 17ª entra
	case <-time.After(5 * time.Second):
		t.Fatal("the 17th call never started after one finished")
	}
	close(release)
	for range limit {
		h.receivedID()
	}
}

func TestMaxInFlightIsConfigurable(t *testing.T) {
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	red := &fakeReducer{outcome: func(in pipeline.Input) pipeline.Outcome {
		entered <- struct{}{}
		<-release
		return pipeline.Outcome{Texts: in.Texts, Report: pipeline.Report{Mode: pipeline.ModePassthrough}}
	}}
	h := newHarness(t, Relay{Reducer: red, MaxInFlight: 2})
	h.sendCalls(1, 2, 3)
	for id := 1; id <= 3; id++ {
		h.respond(id, "x")
	}
	<-entered
	<-entered
	select {
	case <-entered:
		t.Fatal("a third call started with max_in_flight=2")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	for range 3 {
		h.receivedID()
	}
}

// pendingCount devolve um reducer cujo Report final (a contagem exata) nunca termina
// até alguém mandar em done. O Report do caminho crítico diz Exact=true de propósito,
// para provar que quem sai com Exact=false é a regra do limite, não o reducer.
func pendingCount(done chan pipeline.Report) *fakeReducer {
	return &fakeReducer{outcome: func(in pipeline.Input) pipeline.Outcome {
		report := pipeline.Report{Mode: pipeline.ModePassthrough}
		report.Exact = true
		return pipeline.Outcome{Texts: in.Texts, Report: report, Done: done}
	}}
}

// A vaga volta assim que a resposta foi escrita ao agente: uma contagem lenta não a segura.
func TestSlowCountDoesNotHoldTheInFlightSlot(t *testing.T) {
	const limit = 16
	done := make(chan pipeline.Report, limit+4)
	h := newHarness(t, Relay{Reducer: pendingCount(done), OnReport: func(pipeline.Report) {}})
	ids := make([]int, limit+4) // mais que o limite, com todas as contagens ainda pendentes
	for i := range ids {
		ids[i] = i + 1
	}
	h.sendCalls(ids...)
	for _, id := range ids {
		h.respond(id, "x")
	}
	for range ids {
		h.receivedID() // a 17ª em diante chega sem esperar nenhuma contagem
	}
	for range ids {
		done <- pipeline.Report{}
	}
}

// Os Reports pendentes também têm o limite de max_in_flight. Passou dele, o Report sai
// na hora, sem esperar a contagem, com Exact=false; os demais saem com o valor final.
func TestReportsBeyondTheLimitAreNotExact(t *testing.T) {
	done := make(chan pipeline.Report, 2)
	reports := make(chan pipeline.Report, 4)
	h := newHarness(t, Relay{Reducer: pendingCount(done), MaxInFlight: 2, OnReport: func(r pipeline.Report) { reports <- r }})
	h.sendCalls(1, 2, 3, 4)
	for id := 1; id <= 4; id++ {
		h.respond(id, "x")
	}
	for range 4 {
		h.receivedID()
	}
	for range 2 { // as duas fora do limite
		if r := finalReport(t, reports); r.Exact {
			t.Fatalf("report beyond the limit = %+v, want Exact=false", r)
		}
	}
	select {
	case r := <-reports:
		t.Fatalf("report %+v delivered before its count ended", r)
	case <-time.After(100 * time.Millisecond):
	}
	for range 2 {
		done <- pipeline.Report{Mode: pipeline.ModeApplied, Measurement: tokens.Measurement{Exact: true}}
	}
	for range 2 {
		if r := finalReport(t, reports); !r.Exact || r.Mode != pipeline.ModeApplied {
			t.Fatalf("final report = %+v, want the counted one", r)
		}
	}
}

func TestLargeResponseDoesNotDelayTheSmallOnesBehindIt(t *testing.T) {
	release := make(chan struct{})
	red := &fakeReducer{outcome: func(in pipeline.Input) pipeline.Outcome {
		if len(in.Texts[0]) >= twoMiB {
			<-release // a resposta de 2 MiB demora
		}
		return pipeline.Outcome{Texts: in.Texts, Report: pipeline.Report{Mode: pipeline.ModePassthrough}}
	}}
	h := newHarness(t, Relay{Reducer: red})
	h.sendCalls(1, 2, 3, 4)
	<-h.respond(1, strings.Repeat("a", twoMiB)) // o relay lê a grande antes das pequenas
	for id := 2; id <= 4; id++ {
		h.respond(id, "pequena")
	}
	got := map[int]bool{}
	for range 3 { // as pequenas chegam com a grande ainda presa no reducer
		got[h.receivedID()] = true
	}
	if !got[2] || !got[3] || !got[4] {
		t.Fatalf("small responses received = %v", got)
	}
	close(release)
	if id := h.receivedID(); id != 1 {
		t.Fatalf("last response id = %d, want 1", id)
	}
}

const twoMiB = 2 << 20

// ---- Serve espera os OnReport pendentes no encerramento ----

type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// serveUntilUpstreamEOF manda três tools/call, entrega as três respostas ao agente, fecha o
// stdout do upstream e devolve quanto o Serve levou para voltar e o que ele logou.
func serveUntilUpstreamEOF(t *testing.T, onReport func(pipeline.Report)) (time.Duration, string) {
	t.Helper()
	logs := &syncBuffer{}
	red := &fakeReducer{outcome: applied("x")}
	h := newHarness(t, Relay{Reducer: red, OnReport: onReport, Logger: slog.New(slog.NewTextHandler(logs, nil))})
	h.sendCalls(1, 2, 3)
	for id := 1; id <= 3; id++ {
		h.respond(id, "x")
	}
	for range 3 {
		h.receivedID()
	}
	start := time.Now()
	_ = h.upstreamOut.Close()
	select {
	case <-h.served:
	case <-time.After(6 * time.Second):
		t.Fatal("Serve did not return: it must not wait more than the 2s grace for OnReport")
	}
	return time.Since(start), logs.String()
}

// Com OnReport lento, o Serve espera no máximo os 2s fixos do encerramento (shutdownGrace),
// descarta o que não terminou e loga quantos foram descartados.
func TestServeWaitsAtMost2sForSlowReportsAndLogsTheDiscarded(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var calls atomic.Int32
	elapsed, logs := serveUntilUpstreamEOF(t, func(pipeline.Report) {
		if calls.Add(1) > 1 { // o primeiro termina na hora; os outros dois travam
			<-release
		}
	})
	if elapsed < 1800*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("Serve returned after %v, want about %v", elapsed, shutdownGrace)
	}
	if !strings.Contains(logs, "discarded=2") {
		t.Fatalf("log = %q, want the count of discarded reports (discarded=2)", logs)
	}
}

// Sem Report pendente, o Serve volta na hora e não loga descarte.
func TestServeDoesNotWaitWhenNoReportIsPending(t *testing.T) {
	elapsed, logs := serveUntilUpstreamEOF(t, func(pipeline.Report) {})
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Serve returned after %v with nothing pending", elapsed)
	}
	if strings.Contains(logs, "discarded") {
		t.Fatalf("log = %q, want no discard line", logs)
	}
}

// Um Report que termina dentro do prazo é esperado, não descartado.
func TestServeWaitsForAReportThatEndsWithinTheGrace(t *testing.T) {
	var finished atomic.Int32
	elapsed, logs := serveUntilUpstreamEOF(t, func(pipeline.Report) {
		time.Sleep(600 * time.Millisecond)
		finished.Add(1)
	})
	if got := finished.Load(); got != 3 {
		t.Fatalf("%d of 3 slow reports finished before Serve returned (elapsed %v)", got, elapsed)
	}
	if strings.Contains(logs, "discarded") {
		t.Fatalf("log = %q, want no discard line", logs)
	}
}

// Com o limite dos Reports cheio, o OnReport roda inline na goroutine da resposta; esse
// também conta como pendente até terminar, e depois de terminar o Serve não espera por ele.
func TestServeDoesNotWaitForAnInlineReportThatAlreadyEnded(t *testing.T) {
	done := make(chan pipeline.Report, 2)
	reports := make(chan pipeline.Report, 3)
	logs := &syncBuffer{}
	h := newHarness(t, Relay{
		Reducer: pendingCount(done), MaxInFlight: 2, OnReport: func(r pipeline.Report) { reports <- r },
		Logger: slog.New(slog.NewTextHandler(logs, nil)),
	})
	h.sendCalls(1, 2, 3)
	for id := 1; id <= 3; id++ {
		h.respond(id, "x")
	}
	for range 3 {
		h.receivedID()
	}
	if r := finalReport(t, reports); r.Exact { // a 3ª, inline, fora do limite
		t.Fatalf("inline report = %+v, want Exact=false", r)
	}
	for range 2 {
		done <- pipeline.Report{Mode: pipeline.ModeApplied}
	}
	for range 2 {
		finalReport(t, reports)
	}
	start := time.Now()
	_ = h.upstreamOut.Close()
	select {
	case <-h.served:
	case <-time.After(6 * time.Second):
		t.Fatal("Serve did not return")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond || strings.Contains(logs.String(), "discarded") {
		t.Fatalf("Serve returned after %v with log %q, want no wait and no discard", elapsed, logs.String())
	}
}
