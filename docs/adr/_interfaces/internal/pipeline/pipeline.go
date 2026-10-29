// Package pipeline: módulo profundo. Entrada: blocos de texto + chamada. Saída: blocos reduzidos + relatório.
// Quem chama (mcp.Relay) não conhece estágios, tokens nem BM25.
package pipeline

import (
	"context"
	"encoding/json"
	"time"
)

// Call descreve a chamada tools/call que originou a resposta.
type Call struct {
	Upstream string
	Tool     string
	Args     json.RawMessage // argumentos brutos, para QueryResolver e hash
}

// Input é o que o relay entrega ao pipeline: só os blocos de texto da resposta,
// na ordem original. Blocos não textuais nunca entram aqui.
type Input struct {
	Call  Call
	Texts []string
}

// Outcome é o resultado da Run. Texts tem sempre o mesmo comprimento de Input.Texts;
// um bloco totalmente removido vira "" (o relay decide removê-lo da lista).
type Outcome struct {
	Texts  []string
	Report Report
}

// Pipeline executa os estágios em ordem. Invariantes (verificados em Run, violação => fallback):
//  1. todo trecho da saída existe na entrada; 2. tokens_after <= tokens_before;
//  3. nunca devolve todos os blocos vazios se a entrada tinha texto; 4. Run nunca entra em panic nem devolve erro.
type Pipeline struct {
	// campos privados: contador, estágios, consulta, observer.
}

// Run é a única porta de entrada. Seguro para uso concorrente.
// Qualquer falha (erro, panic, timeout de ctx, invariante violada) devolve a entrada intacta
// com Report.Mode = ModeFallback e Report.Err preenchido.
func (p *Pipeline) Run(ctx context.Context, in Input) Outcome { return Outcome{} }

// Doc é o estado mutável que circula entre estágios. Cada estágio recebe um clone.
type Doc struct {
	Query  string  // vazio => estágios de ranking/orçamento devem ser no-op
	Budget int     // tokens; 0 => sem orçamento
	Chunks []Chunk // ordem original preservada; estágios marcam, não reordenam o slice
}

// Chunk é a unidade de dedup, ranking e orçamento (ADR-006).
type Chunk struct {
	Block  int     // índice do bloco de texto de origem
	Seq    int     // posição original global (estável)
	Text   string
	Tokens int     // contado uma vez pelo pipeline, antes dos estágios
	Score  float64 // preenchido por Rank
	Keep   bool    // true no início; estágios só podem mudar para false
	Reason string  // por que foi removido: "dup:exact", "dup:near", "budget"
}

// Stage é o ponto de extensão. Contrato:
//   - só pode marcar Keep=false/Reason/Score; não cria nem edita Text;
//   - deve respeitar ctx (cancelamento);
//   - erro ou panic => fallback do chamado inteiro (política em ADR-001).
type Stage interface {
	Name() string
	Apply(ctx context.Context, d *Doc) error
}

// QueryResolver deriva a consulta BM25 dos argumentos da chamada (ADR-002).
type QueryResolver interface {
	Resolve(c Call) (query string, source string) // source: "config" | "auto" | "none"
}

// Mode descreve o que aconteceu com a chamada.
type Mode string

const (
	ModePassthrough Mode = "passthrough" // desligado por config, não elegível ou nada a cortar
	ModeApplied     Mode = "applied"
	ModeFallback    Mode = "fallback" // falha interna; resposta original devolvida
)

// StageReport e Report alimentam o Observer.
type StageReport struct {
	Name    string
	Dropped int
	TokensDropped int
	Duration time.Duration
}

type Report struct {
	Mode          Mode
	SkipReason    string // ex.: "structured_content", "is_error", "input_required", "no_text"
	Encoding      string
	TokensBefore  int
	TokensAfter   int
	QuerySource   string
	Stages        []StageReport
	PipelineTime  time.Duration // tempo gasto em Run (parte do overhead)
	Err           string
}

// Record é a linha por chamada (store + metrics). Montado pelo relay.
type Record struct {
	Time        time.Time
	Upstream    string
	Tool        string
	ArgsHash    string // sha256 truncado dos argumentos
	Report      Report
	BytesBefore int
	BytesAfter  int
	UpstreamNS  int64 // tempo do upstream (req enviada -> resp recebida)
	TotalNS     int64 // agente->resposta entregue
	OverheadNS  int64 // TotalNS - UpstreamNS
	IsError     bool
	// Conteúdo bruto: só preenchido em modo captura (ADR-003); nil por padrão.
	RequestJSON  json.RawMessage
	ResponseJSON json.RawMessage
}

// Observer recebe um Record por tools/call. Contrato: Observe NUNCA bloqueia o chamador
// além de ~microssegundos e NUNCA falha para ele (descarta e conta em caso de pressão).
type Observer interface {
	Observe(Record)
}
