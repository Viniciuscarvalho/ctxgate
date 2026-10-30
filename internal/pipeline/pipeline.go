// Package pipeline reduz o texto de uma resposta tools/call: segmenta em trechos,
// passa por estágios plugáveis e devolve os trechos mantidos. Quem chama (mcp.Relay)
// só conhece Run; nunca vê estágios nem tokens. Nunca importa mcp, store nem metrics.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/Viniciuscarvalho/ctxgate/internal/tokens"
)

const (
	// exactCountMaxBytes: acima disso o corte usa estimativa bytes/3 e a contagem exata
	// roda depois, fora do caminho crítico (decisão do CTO; gate de p95 vale até 100 KB).
	exactCountMaxBytes = 256 << 10
	maxChunkTokens     = 512 // parágrafo acima disso é subdividido por linha (ADR-006)
	defaultTimeout     = 200 * time.Millisecond
	defaultMaxInFlight = 16 // ADR-001 §3
)

// Call descreve a chamada tools/call que originou a resposta.
type Call struct {
	Tool string
	Args json.RawMessage
}

// Input é o que o relay entrega: só os blocos de texto, na ordem original.
// SkipReason não vazio significa "resposta não elegível": Run a devolve intacta.
type Input struct {
	Call         Call
	Texts        []string
	NonTextBytes int // bytes dos blocos não textuais, que ficam fora de Texts
	SkipReason   string
}

// Outcome é o resultado de Run. len(Texts) == len(Input.Texts); um bloco removido por
// inteiro vira "" e o relay o tira da lista. Report tem o que o caminho crítico mediu.
// Done entrega o Report final (uma vez), depois que a contagem exata terminar: o relay
// o lê só depois de enviar a resposta.
type Outcome struct {
	Texts  []string
	Report Report
	Done   <-chan Report
}

type Mode string

const (
	ModePassthrough Mode = "passthrough" // não elegível ou nada a cortar
	ModeApplied     Mode = "applied"
	ModeFallback    Mode = "fallback" // falha interna; resposta original devolvida
)

// Report alimenta o Record (S2-04). A medição de tokens vem de tokens.Measurement.
type Report struct {
	Mode       Mode
	SkipReason string
	Err        string
	tokens.Measurement
	Stages       []StageReport
	PipelineTime time.Duration
}

type StageReport struct {
	Name          string
	Dropped       int
	TokensDropped int
}

// Doc é o estado que circula entre estágios.
type Doc struct {
	Chunks []Chunk // ordem original; estágios marcam, não reordenam
}

// Chunk é a unidade de dedup (ADR-006). Text inclui os separadores que o seguem.
type Chunk struct {
	Block  int
	Seq    int
	Text   string
	Tokens int
	Keep   bool   // true no início; estágios só podem mudar para false
	Reason string // por que foi removido: "dup:exact"
}

// Stage é o ponto de extensão. Só pode marcar Keep=false e Reason; não cria, não
// edita e não reordena trechos (Run confere). Erro ou panic => fallback da chamada.
type Stage interface {
	Name() string
	Apply(ctx context.Context, d *Doc) error
}

type Options struct {
	Counter tokens.Counter
	Stages  []Stage       // executados em ordem
	Timeout time.Duration // 0 => 200ms
	Logger  *slog.Logger  // nil => descarta
	// MaxInFlight limita as contagens exatas em segundo plano (pipeline.max_in_flight);
	// 0 => 16. Sem vaga livre a contagem não roda e o Report final sai com Exact=false.
	MaxInFlight int
}

// Pipeline é imutável depois de New, então Run é seguro para uso concorrente.
type Pipeline struct {
	counter tokens.Counter
	stages  []Stage
	timeout time.Duration
	log     *slog.Logger
	counts  tokens.Limiter
}

func New(o Options) *Pipeline {
	p := &Pipeline{counter: o.Counter, stages: slices.Clone(o.Stages), timeout: o.Timeout, log: o.Logger}
	p.counts = tokens.NewLimiter(defaultMaxInFlight)
	if o.MaxInFlight > 0 {
		p.counts = tokens.NewLimiter(o.MaxInFlight)
	}
	if p.timeout <= 0 {
		p.timeout = defaultTimeout
	}
	if p.log == nil {
		p.log = slog.New(slog.DiscardHandler)
	}
	return p
}

// Run nunca devolve erro nem entra em panic. Erro ou panic de estágio, timeout e
// violação de invariante devolvem a entrada intacta com Mode=ModeFallback.
func (p *Pipeline) Run(ctx context.Context, in Input) Outcome {
	start := time.Now()
	runCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// A goroutine garante que Run volte no prazo mesmo se um estágio ignorar ctx.
	type result struct {
		out Outcome
		err error
	}
	done := make(chan result, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- result{err: fmt.Errorf("panic: %v", r)}
			}
		}()
		out, err := p.reduce(runCtx, ctx, in)
		done <- result{out, err}
	}()

	var out Outcome
	select {
	case r := <-done:
		out = r.out
		if r.err != nil {
			out = p.fallback(ctx, in, r.err)
		}
	case <-runCtx.Done():
		out = p.fallback(ctx, in, runCtx.Err())
	}
	out.Report.PipelineTime = time.Since(start)
	return out
}

// reduce é o caminho crítico. ctx tem o prazo do pipeline; parent só alimenta a
// contagem em segundo plano, que continua depois que a resposta foi enviada.
func (p *Pipeline) reduce(ctx, parent context.Context, in Input) (Outcome, error) {
	report := p.newReport(in)
	report.SkipReason = in.SkipReason
	if in.SkipReason != "" {
		return p.intact(parent, in.Texts, report), nil
	}

	chunks, exact, err := p.segment(ctx, in)
	if err != nil {
		return Outcome{}, err
	}
	doc := &Doc{Chunks: chunks}
	for _, s := range p.stages {
		sr, err := applyStage(ctx, s, doc)
		if err != nil {
			return Outcome{}, err
		}
		report.Stages = append(report.Stages, sr)
	}

	blocks := make([]strings.Builder, len(in.Texts))
	keep := make([]bool, len(doc.Chunks))
	chunkTexts := make([]string, len(doc.Chunks))
	dropped := 0
	for i, c := range doc.Chunks {
		keep[i], chunkTexts[i] = c.Keep, c.Text
		report.TokensBefore += c.Tokens
		if c.Keep {
			report.TokensAfter += c.Tokens
			blocks[c.Block].WriteString(c.Text)
		} else {
			dropped++
		}
	}
	texts := in.Texts // sem corte, a resposta original sai como veio
	if dropped > 0 {
		report.Mode = ModeApplied
		texts = make([]string, len(blocks))
		for i := range blocks {
			texts[i] = blocks[i].String()
		}
		if totalBytes(texts) == 0 && totalBytes(in.Texts) > 0 {
			return Outcome{}, errors.New("invariant: every block was removed")
		}
	}
	report.Exact = exact
	if exact || totalBytes(in.Texts) <= exactCountMaxBytes {
		return Outcome{Texts: texts, Report: report, Done: ready(report)}, nil // contagem exata, ou ela falhou
	}
	return Outcome{Texts: texts, Report: report, Done: p.measureLater(parent, report, chunkTexts, keep)}, nil
}

// segment quebra os blocos em trechos e dá tokens a cada um. Até 256 KB a contagem é
// exata, uma passada por linha (parágrafos >512 tokens viram um trecho por linha, sem
// recontar). Acima disso, ou se o contador falhar, usa a estimativa bytes/3, que
// superestima: o corte fica maior, nunca menor. exact diz qual dos dois valeu.
func (p *Pipeline) segment(ctx context.Context, in Input) (chunks []Chunk, exact bool, err error) {
	type located struct {
		block int
		paragraph
	}
	var paras []located
	var lines []string
	for b, text := range in.Texts {
		for _, para := range paragraphs(text) {
			paras = append(paras, located{b, para})
			lines = append(lines, para.lines...)
		}
	}

	counts := make([]int, len(lines))
	if totalBytes(in.Texts) <= exactCountMaxBytes {
		c, err := tokens.CountAll(ctx, p.counter, lines)
		switch {
		case err == nil:
			counts, exact = c, true
		case ctx.Err() != nil:
			return nil, false, ctx.Err()
		default:
			p.log.Warn("pipeline: exact token count failed, using bytes/3", "tool", in.Call.Tool, "err", err)
		}
	}
	if !exact {
		for i, l := range lines {
			counts[i] = estimate(len(l))
		}
	}

	next := 0
	for _, para := range paras {
		lineTokens := counts[next : next+len(para.lines)]
		next += len(para.lines)
		text := strings.Join(para.lines, "")
		total := estimate(len(text))
		if exact {
			total = 0
			for _, n := range lineTokens {
				total += n
			}
		}
		if total > maxChunkTokens && len(para.lines) > 1 && !para.fenced {
			for i, l := range para.lines {
				chunks = append(chunks, Chunk{Block: para.block, Text: l, Tokens: lineTokens[i], Keep: true})
			}
			continue
		}
		chunks = append(chunks, Chunk{Block: para.block, Text: text, Tokens: total, Keep: true})
	}
	for i := range chunks {
		chunks[i].Seq = i
	}
	return chunks, exact, nil
}

// applyStage roda um estágio e confere que ele só desligou Keep.
func applyStage(ctx context.Context, s Stage, doc *Doc) (StageReport, error) {
	prev := slices.Clone(doc.Chunks)
	if err := s.Apply(ctx, doc); err != nil {
		return StageReport{}, fmt.Errorf("stage %s: %w", s.Name(), err)
	}
	if err := ctx.Err(); err != nil {
		return StageReport{}, err
	}
	sr := StageReport{Name: s.Name()}
	if len(doc.Chunks) != len(prev) {
		return sr, fmt.Errorf("invariant: stage %s changed the number of chunks", s.Name())
	}
	for i, a := range prev {
		b := doc.Chunks[i]
		if a.Block != b.Block || a.Seq != b.Seq || a.Text != b.Text || a.Tokens != b.Tokens || (b.Keep && !a.Keep) {
			return sr, fmt.Errorf("invariant: stage %s changed chunk %d beyond Keep/Reason", s.Name(), i)
		}
		if a.Keep && !b.Keep {
			sr.Dropped++
			sr.TokensDropped += a.Tokens
		}
	}
	return sr, nil
}

func (p *Pipeline) newReport(in Input) Report {
	return Report{Mode: ModePassthrough, Measurement: tokens.Measurement{Encoding: p.counter.Encoding(), NonTextBytes: in.NonTextBytes}}
}

// intact devolve texts sem mudança e mede o original em segundo plano.
func (p *Pipeline) intact(parent context.Context, texts []string, report Report) Outcome {
	return Outcome{Texts: texts, Report: report, Done: p.measureLater(parent, report, texts, nil)}
}

func (p *Pipeline) fallback(parent context.Context, in Input, err error) Outcome {
	p.log.Warn("pipeline fallback: original response kept", "tool", in.Call.Tool, "err", err)
	report := p.newReport(in)
	report.Mode, report.Err = ModeFallback, err.Error()
	return p.intact(parent, in.Texts, report)
}

// measureLater conta em segundo plano (before = todos os textos, after = os de keep) e
// entrega o Report final em Done. keep nil mantém todos.
func (p *Pipeline) measureLater(ctx context.Context, report Report, texts []string, keep []bool) <-chan Report {
	done := make(chan Report, 1)
	m := tokens.MeasureAsync(ctx, p.counts, p.counter, texts, keep, report.NonTextBytes)
	go func() {
		report.Measurement = <-m
		done <- report
	}()
	return done
}

func ready(report Report) <-chan Report {
	done := make(chan Report, 1)
	done <- report
	return done
}

func totalBytes(texts []string) (n int) {
	for _, t := range texts {
		n += len(t)
	}
	return n
}

func estimate(bytes int) int { return (bytes + 2) / 3 }
