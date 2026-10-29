# ADR-001 — Estrutura do repositório, camada MCP e interface do pipeline

- **Status:** aceita (Vinicius, 2026-10-29) · **Data:** 2026-10-29 · **Resolve:** questões 1, 2, 6, 7 e YAML (decisoes-cto.md)
- **Vocabulário:** módulo, interface, seam, adapter, profundidade (skills `codebase-design` / `improve-codebase-architecture`).

## Contexto
O repositório (`github.com/Viniciuscarvalho/ctxgate`, público, MIT) está vazio: só README (título), LICENSE e `.gitignore` Go. Não há código nem ADRs a respeitar; é greenfield. Restrições: Go, ≤5h/semana, zero infra e zero custo. O PRD fixa os pacotes `mcp/ pipeline/ store/ metrics/` e a spec pede o SDK oficial *ou* protocolo próprio (decisão minha).

O produto é um proxy: quase tudo que passa por ele **não deve ser tocado** (initialize/discover, tools/list, resources, prompts, notificações, progress, cancelamento, pedidos do servidor ao cliente). Só o `result` de `tools/call` entra no pipeline.

## Decisão
### 1. Camada MCP = relay JSON-RPC próprio, opaco e agnóstico de versão
`internal/mcp` implementa um **relay** stdio (JSON delimitado por `\n`) que copia mensagens agente↔upstream **sem reserializar**, decodificando só o envelope (`id`, `method`, `result`). Mantém um mapa `id → tools/call pendente`; ao chegar a resposta com esse id (e `resultType` ausente/`complete`, sem `isError`, sem `structuredContent`), extrai os blocos `content[].type=="text"`, chama o `Reducer` e reescreve **apenas** esses blocos. Todo o resto do objeto (`_meta`, campos desconhecidos, blocos não textuais) vai como bytes originais.

SDK oficial `modelcontextprotocol/go-sdk` **fica fora do binário**; entra só como **dependência de teste** (fixado em `v1.8.0`) para um teste de conformância: upstream e cliente do SDK conversando através do relay nas versões de protocolo 2025-11-25 e 2026-07-28.

### 2. Layout (módulo `github.com/Viniciuscarvalho/ctxgate`, `go 1.25`)
```
cmd/ctxgate/        main: serve | stats | --version   (go install .../cmd/ctxgate@vX)
internal/config/    YAML -> Config validada (go.yaml.in/yaml/v3, KnownFields=true)
internal/mcp/       Relay, framing, envelope, ToolResultView, Upstream (subprocesso)
internal/pipeline/  Pipeline, Doc, Chunk, Stage, QueryResolver; estágios dedup.go rank.go budget.go
internal/tokens/    Counter (tiktoken-go/tokenizer, cl100k_base)
internal/store/     Observer SQLite assíncrono + leitura para `stats`
internal/metrics/   Observer Prometheus + handler HTTP
internal/testutil/  upstream fake, cliente fake, harness
bench/              harness do benchmark (go run ./bench), tarefas e relatórios
docs/adr/  docs/    ADRs, PRD, spec, tickets (hoje só em /workspace/ctxgate)
```
Tudo sob `internal/` (nenhuma API pública a manter). Estágios ficam no pacote `pipeline` (um pacote, não três): são **internal seams** do módulo, testados por tabela, e o **seam externo** é `Pipeline.Run`. Dependências: `cmd → mcp → pipeline`; `store` e `metrics` implementam `pipeline.Observer`; `cmd` faz a fiação. `mcp` não importa `store`/`metrics`.

### 3. Interfaces (compiladas com `go vet` num módulo de rascunho)
```go
// internal/pipeline — módulo profundo: 1 método útil para quem chama.
type Call struct { Upstream, Tool string; Args json.RawMessage }
type Input struct { Call Call; Texts []string }          // só blocos de texto, ordem original
type Outcome struct { Texts []string; Report Report }    // len(Texts)==len(Input.Texts); bloco removido => ""

func (p *Pipeline) Run(ctx context.Context, in Input) Outcome // nunca erra, nunca panica; thread-safe

type Doc struct { Query string; Budget int; Chunks []Chunk }
type Chunk struct {
	Block, Seq int; Text string; Tokens int
	Score float64; Keep bool; Reason string // "dup:exact" | "dup:near" | "budget"
}
type Stage interface {                    // ponto de extensão (2 adapters reais: dedup, rank/budget => seam real)
	Name() string
	Apply(ctx context.Context, d *Doc) error // só marca Keep/Score/Reason; não cria nem edita Text
}
type QueryResolver interface { Resolve(c Call) (query, source string) } // ADR-002

type Mode string // "passthrough" | "applied" | "fallback"
type Report struct {
	Mode Mode; SkipReason, Encoding, QuerySource, Err string
	TokensBefore, TokensAfter int
	Stages []StageReport; PipelineTime time.Duration
}
type Record struct { /* linha por chamada: ver ADR-003 */ }
type Observer interface { Observe(Record) } // nunca bloqueia, nunca falha para o chamador

// internal/mcp — interfaces definidas no consumidor.
type Reducer interface { Run(ctx context.Context, in pipeline.Input) pipeline.Outcome }
type Upstream interface {
	Name() string
	Streams() (io.WriteCloser, io.ReadCloser, error)
	Close(ctx context.Context) error // fecha stdin, SIGTERM, SIGKILL: sem zumbi
}
func (r *Relay) Serve(ctx context.Context, agentIn io.Reader, agentOut io.Writer) error

// internal/tokens
type Counter interface { Count(text string) int; Encoding() string }
```
Código completo compilável: ver `docs/adr/_interfaces/` (cópia dos arquivos validados).

### 4. Erro e fallback
`Run` captura erro de estágio, panic, `ctx` expirado (`pipeline.timeout`, padrão 200 ms) e violação de invariante (saída ⊄ entrada, `after>before`, todos os blocos vazios) → devolve **a entrada intacta** com `Mode=fallback` e `Err`. Política de falha é por chamada inteira (como no ticket S2-02); "pular só o estágio" fica como evolução. Estágios são **síncronos**; assíncrono só no que é observação (gravação/contagem em passthrough).

### 5. Concorrência
Um processo = uma sessão de agente = **1 upstream** (stdio). Várias chamadas em voo: o relay processa cada resposta de `tools/call` numa goroutine, limitada por semáforo (`max_in_flight`, padrão 16), com escritor único protegido por mutex para o stdout do agente. A **ordem de entrega não é garantida** (JSON-RPC correlaciona por `id`). `Pipeline` é imutável após `New` ⇒ sem estado compartilhado; cada `Run` trabalha em clones.

### 6. YAML
`upstreams:` é **lista** com validação "exatamente 1 no MVP" (mensagem clara) — compatível com multi-upstream futuro sem quebrar o formato. Chaves: `version`, `upstreams[]{name,command,args,env,startup_timeout}`, `pipeline{enabled,timeout,max_in_flight,budget_tokens,dedup{enabled,similarity},rank{enabled,k1,b}}`, `tools.<nome>.query_args`, `store{path,retention_days,capture_content}`, `metrics{listen}`. Desconhecidas ⇒ erro (`KnownFields`).

### 7. Métodos MCP
Passam direto, sem interpretação: tudo exceto `tools/call`. Entra no pipeline só o `result` de `tools/call` (`resources/read` e `prompts/get` ficam para depois: mesmo mecanismo, outra entrada na tabela de métodos). Versão do protocolo: **o relay não negocia**; aceita o que agente e upstream negociarem (testado: 2025-11-25 e 2026-07-28; 2025-06-18 como legado). Respostas `resultType:"input_required"` (MRTR, 2026-07-28) passam intactas.

## SDK: avaliação (consultado em 2026-10-29)
| | `modelcontextprotocol/go-sdk` | `mark3labs/mcp-go` | relay próprio |
|---|---|---|---|
| Versão | v1.8.0 (14/09/2026); v1.7.0 trouxe spec 2026-07-28 | v1.4.0 (22/10/2026) | n/a |
| Maturidade | oficial, co-mantido com Google; 5,3k★; v1.x estável, release a cada ~6 semanas; GitHub o usa em produção | comunitário; 9,2k★; v1.x, releases a cada 1–2 semanas; LATEST=2026-07-28 | ~300 linhas |
| Licença | Apache-2.0 (código novo), MIT (legado), CC-BY-4.0 (docs); `LICENSE` em transição | MIT | MIT (do projeto) |
| Go mínimo | 1.25 | 1.25.5 | 1.25 |
| Serve para proxy? | **Parcial**: modelo cliente/servidor tipado. Um proxy teria de **espelhar** tools, resources, templates, prompts, list_changed, progress, cancelamento, MRTR; o servidor negocia versão por conta própria; blocos `content` de tipo desconhecido **falham no decode** (`invalid content type`), o que viola "passar intacto" | mesmo problema estrutural | Transparente por construção |

Medição (spike descartável, in-memory, 100 chamadas): espelhar via SDK custa p50 ≈ +3,6 ms em 100 KB e +32 ms em 1 MB **sem** nenhum estágio (re-serialização e hops extras). O relay opaco evita isso e remove ~3 superfícies de mapeamento por versão do protocolo.

## Alternativas
1. **SDK oficial como camada principal (espelhar upstream):** menos bytes de protocolo escritos, mais código de mapeamento; quebra a transparência (item acima) e acopla o ciclo de releases ao SDK. Rejeitada.
2. **mark3labs/mcp-go:** mesma forma estrutural; sem ganho para um proxy. Rejeitada.
3. **SDK só no upstream/cliente de teste (escolhida para testes):** custo zero em runtime, conformância real.

## Consequências
- (+) Passthrough byte-exato, método-agnóstico, imune às mudanças do protocolo (stateless 2026-07-28) além do formato do `tools/call`; portfólio: módulo pequeno e profundo com concorrência real.
- (+) Interface do pipeline minúscula (`Run`) com estágios plugáveis (`Stage`).
- (−) Escrevo/mantenho framing e ciclo de vida do subprocesso (S1-02 e S1-04 ficam por minha conta).
- (−) `go.mod` carrega o go-sdk só para testes (licença mista; não vai no binário).

## Riscos
1. **S1-02 (2,0h) é apertado** para relay + processo + testes de ordem. *Gatilho:* se passar de 3h sem passar o teste de conformância, reabrir esta ADR e avaliar SDK espelhado só para `tools/*`.
2. **Framing:** `bufio.Scanner` (limite 64 KB) quebra respostas de 1 MB: usar `bufio.Reader` com `ReadBytes('\n')`; teste ≥1 MB obrigatório (S1-03). Confirmar em S1-02 que o stdio de 2026-07-28 segue newline-delimited.
3. **Interpretação mínima do envelope** pode divergir em casos raros (id como string/número, batch `[`): ids tratados como bytes brutos; linhas que começam com `[` passam opacas.
4. **Pendente de aprovação do Vinicius** (custo zero): dependência de teste `go-sdk` (Apache-2.0/MIT/CC-BY-4.0).
