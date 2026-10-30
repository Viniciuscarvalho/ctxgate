# Decisões do CTO — 13 questões abertas, ajustes de tickets, pendências

Data: 2026-10-29. ADRs: 001 (estrutura/SDK/pipeline), 002 (consulta BM25), 003 (SQLite), 004 (benchmark), 005 (tokens/latência/métricas), 006 (trecho/dedup).

## 1. Respostas às 13 questões
| # | Questão | Decisão (resumo) | Onde |
|---|---|---|---|
| 1 | SDK / versão do protocolo | **Relay JSON-RPC próprio, opaco**, agnóstico de versão. go-sdk v1.8.0 só como dependência de **teste** (conformância 2025-11-25 e 2026-07-28). | ADR-001 |
| 2 | Métodos no pipeline | Só o `result` de `tools/call` (não erro, sem `structuredContent`, `resultType` completo). Tudo mais passa byte a byte: listagens, resources, prompts, notificações, progress, cancelamento, MRTR. `resources/read`/`prompts/get`: depois do MVP. | ADR-001, 006 |
| 3 | Consulta do BM25 | Config por tool → heurística nos argumentos → sem consulta = **não corta** (só dedup). | ADR-002 |
| 4 | Unidade de trecho / não textual | Parágrafo (código cercado inteiro; subdivide >512 tokens por linha). Não textual e tipos desconhecidos passam intactos. | ADR-006 |
| 5 | Escopo do dedup | Dentro da mesma resposta. Sessão/persistido fora do MVP (alavanca se S5 <30%). | ADR-006 |
| 6 | Interface do pipeline e falha | `Pipeline.Run` (nunca erra) + `Stage.Apply(ctx,*Doc)`; falha/panic/timeout/invariante ⇒ **resposta original** da chamada inteira. Estágios síncronos. | ADR-001 |
| 7 | Concorrência / upstreams | 1 sessão e 1 upstream por processo; várias chamadas em voo (semáforo 16); ordem de entrega não garantida. YAML com lista `upstreams` (exatamente 1 no MVP). | ADR-001 |
| 8 | SQLite | `modernc.org/sqlite`, WAL/NORMAL, escritor único em lote via canal limitado; esquema v1; só metadados por padrão; conteúdo só em modo captura; retenção 30 dias. | ADR-003 |
| 9 | Replay vs. ao vivo | Replay determinístico (obrigatório) com sucesso = retenção de `required_evidence`; agente ao vivo só como amostra manual opcional. | ADR-004 |
| 10 | Métricas | `client_golang` v1.25.0; nomes em ADR-005; histograma no `/metrics`; **p50/p95 exatos no SQLite** no `stats`; endpoint off por padrão, `127.0.0.1`; porta ocupada ⇒ degrada com aviso. | ADR-005 |
| 11 | Tokenizer | `tiktoken-go/tokenizer` v0.10.0, `cl100k_base`, aproximado e declarado; uma passada por trecho (+paralelismo). | ADR-005 |
| 12 | Plataformas / CGO | `CGO_ENABLED=0`; linux, darwin (amd64/arm64) e windows/amd64 (+linux/arm64). Cruzamento sem CGO verificado em spike (linux/arm64, darwin/arm64, windows/amd64); CI só em ubuntu; Windows sem smoke test (declarar no README). | ADR-003 |
| 13 | Marcador de truncamento | **Não no MVP.** Sem ferramenta de expandir é ruído e quebra idempotência. Reavaliar após S5 (`ctxgate_expand`, fase 2). | ADR-006 |

Padrões iniciais (validados no S5, ajustáveis só por config): `budget_tokens: 6000` (respostas que cabem passam intactas), `dedup.similarity: 0.90`, `bm25 k1=1.2 b=0.75`, `pipeline.timeout: 200ms`, `max_in_flight: 16`, `startup_timeout: 30s` (cobre `npx` frio), encerramento do upstream com prazo fixo de 2s no código, sem campo no YAML (fecha stdin → SIGTERM → SIGKILL por grupo de processos; ver decisão de 2026-10-29 20h00), sem timeout por chamada (cancelamento é do agente).

## 2. Ajustes sugeridos nos tickets (sem mudar escopo)
**S1**
- **S1-01:** módulo `github.com/Viniciuscarvalho/ctxgate`, `go 1.25`, `main` em `cmd/ctxgate/`; layout do ADR-001; YAML com `KnownFields(true)` e lista `upstreams` validada para exatamente 1; `.gitignore` adiciona `*.db`, `*.db-wal`, `*.db-shm`, `ctxgate.yaml`, `bench/corpus/`. O repo já tem LICENSE e `.gitignore`.
- **S1-02:** AC "byte-equivalente" vale para o `content` quando o pipeline está desligado (o relay não reserializa). Trocar "ordem de respostas preservada" por "ids preservados e cada resposta correlacionada ao seu pedido; ordem de entrega não garantida". Resources/prompts/notificações passam **sempre** (deixa de ser "se o custo for baixo"). Adicionar AC: teste de conformância com go-sdk v1.8.0 (cliente e servidor) nas versões 2025-11-25 e 2026-07-28. Risco de estourar 2,0h: gatilho do ADR-001.
- **S1-03:** harness com upstream fake como subprocesso (re-exec do binário de teste) **e** variante `io.Pipe` em memória; resposta ≥1 MB exige leitura sem `bufio.Scanner`. Cobertura de 70% aplica a `internal/mcp`.
- **S1-04:** política de timeouts = ADR/decisões acima. "Comando inexistente ≤5s" = erro de `exec` imediato; "handshake" vira `startup_timeout` entre o 1º pedido do agente e a 1ª resposta (30s). Kill por grupo de processos (Unix); no Windows, melhor esforço, declarar. Upstream que morre ⇒ cada pedido pendente recebe erro JSON-RPC `-32603` e o `serve` encerra com código ≠0 (sem reinício).

**S2**
- **S2-01:** encoding `cl100k_base` com `tiktoken-go/tokenizer`; contagem por trecho, uma passada; bytes de blocos não textuais registrados, tokens não. Passthrough: contar depois de enviar.
- **S2-02:** unidade = parágrafo (ADR-006); escopo = resposta; falha de estágio ⇒ original da chamada. Trocar "sem alterar o router" por "sem alterar o relay nem o `Pipeline`".
- **S2-03:** simhash + Jaccard de shingles (0,90); trechos <20 tokens só por hash exato; chave `dedup.similarity`.
- **S2-04:** esquema v1 do ADR-003, gravação assíncrona em lote, `payloads` só em modo captura; AC extra: por padrão nenhuma tabela `payloads` populada. **Risco de horas:** esta semana soma exatamente 5,0h e S2-04 tende a ~1,25h. Rebalancear **dentro da semana**: S2-01 0,75h (biblioteca fina), S2-04 1,25h, mantendo 5,0h.

**S3**
- **S3-01:** incluir o `QueryResolver` (ADR-002) e a heurística na estimativa de 2,0h; AC de "consulta vazia ⇒ ordem original" passa a significar "sem corte" (também coberto no S3-02).
- **S3-02:** sem marcador. Política "nunca vazio" = mantém o trecho de maior score inteiro (único caso em que `tokens_after > N` é permitido; ajustar o AC). Ordem original preservada.
- **S3-03:** chaves do YAML do ADR-001 e padrões acima; a **única** configuração por tool é `tools.<nome>.query_args` (necessária ao ADR-002).
- **S3-04:** propriedades continuam válidas; idempotência depende de não haver marcador (decisão 13).

**S4**
- **S4-01:** `client_golang` v1.25.0; endpoint **desligado por padrão**; porta ocupada ⇒ degrada (AC já prevê). Usar `Flush` do store nos testes que comparam `/metrics` com SQLite.
- **S4-02:** percentis exatos (SQLite). AC "stats e /metrics concordam": contadores idênticos; percentis só dentro do bucket do histograma.
- **S4-03:** overhead = `total_ns − upstream_ns`. Aviso: contar tokens em 100 KB custa ~10 ms em uma passada; a meta <20 ms depende de contagem única e paralela (ADR-005).
- **S4-04:** verificar vazamento de goroutines com `runtime.NumGoroutine` antes/depois (stdlib), sem adicionar `goleak`.

**S5**
- **S5-01:** cada tarefa ganha `required_evidence` (substrings/regex). Corpus capturado em modo captura; confirmar privacidade do FitToday-cms. Critério de sucesso "comando pass/fail" (≥15 das 20) vira apêndice da camada B; o critério da camada A é a evidência retida (automatizável em 20/20).
- **S5-02:** `make bench-e2e` chama `go run ./bench` (replay). "Sucesso baseline/ctxgate" = proxy de retenção de evidência, rotulado como tal.
- **S5-03:** sem mudança; registrar fração de chamadas com `query_source=none`.

**S6**
- **S6-01:** declarar tokens aproximados (cl100k_base), proxy de sucesso e limitações (sem consulta ⇒ sem corte).
- **S6-02:** trocar `go install github.com/<owner>/ctxgate@v0.1.0` por `go install github.com/Viniciuscarvalho/ctxgate/cmd/ctxgate@v0.1.0`. Versão via `debug.ReadBuildInfo` (para `go install`) com `-ldflags` no goreleaser. LICENSE (MIT) já existe. Incluir texto das licenças das dependências redistribuídas (modernc BSD-3, client_golang Apache-2.0, tokenizer MIT) nos artefatos (~0,15h).
- **S6-03/S6-04:** sem mudança.

## 3. Pendências
**Pendente de aprovação do Vinicius** (todas custo zero; nenhuma bloqueia S1 exceto a primeira de teste):
1. `modernc.org/sqlite` v1.61.0 (BSD-3, dependência grande, ADR-003): necessária até o início da S2.
2. `github.com/tiktoken-go/tokenizer` v0.10.0 em vez de `pkoukk/tiktoken-go` (ADR-005): necessária até o início da S2.
3. `github.com/prometheus/client_golang` v1.25.0 (Apache-2.0, ADR-005): necessária até S4.
4. `github.com/modelcontextprotocol/go-sdk` v1.8.0 **só em testes** (Apache-2.0/MIT/CC-BY-4.0, ADR-001): necessária na S1-02/S1-03.
5. `go.yaml.in/yaml/v3` v3.0.5 (continuação oficial do yaml.v3, que está parado desde 2022): necessária na S1-01.
**Perguntas ao Vinicius/PO:** (a) FitToday-cms é público? Define se o corpus do benchmark pode ser versionado (ADR-004); (b) o PO aceita "sucesso = retenção de evidência" na camada A (ADR-004)?; (c) o PO aceita que chamadas sem consulta não sejam cortadas (ADR-002)? Se a fração for grande, a meta de 30% pode falhar.
**Ações externas não feitas:** nada foi commitado nem enviado ao GitHub; PRD, spec, tickets e estas ADRs existem só em `/workspace/ctxgate`. Para o repo refleti-los, é preciso aprovação para commit/push.

## 4. Divergências entre o repositório e PRD/spec/tickets (verificado em 2026-10-29)
O repo `Viniciuscarvalho/ctxgate` (público, branch `main`, 1 commit `3cc3278`) contém apenas `README.md` (título "# ctxgate"), `LICENSE` (MIT, 2026) e `.gitignore` (template Go). Portanto:
1. **Sem go.mod, código, CI, `docs/`, `docs/adr/` ou ADRs prévias.** Os pacotes `mcp/ pipeline/ store/ metrics/` do PRD não existem; tudo é greenfield. Divergência de forma: coloquei os pacotes sob `internal/` e `main` em `cmd/ctxgate/`.
2. **PRD, spec e tickets não estão no repo** (só em `/workspace/ctxgate`, que não é repositório git). Sugestão: versionar em `docs/`.
3. **`go install github.com/<owner>/ctxgate@v0.1.0` (S6-02) não funciona** com `main` em `cmd/`; o caminho certo inclui `/cmd/ctxgate`, e `<owner>` = `Viniciuscarvalho`.
4. **"Licença" (S6-02) já está cumprida** (MIT no repo).
5. **README vazio** além do título (S6-01 continua válido).
6. **`.gitignore` não cobre** `*.db*`, config local e corpus do benchmark (risco de vazar conteúdo; ver S1-01).
7. **Go:** o PRD não fixa versão; `go 1.25` (mínimo do go-sdk/mcp-go) é a recomendação.
8. **Divergência PRD × decisão:** PRD cita "SDK oficial de Go" como mitigação de risco ("acompanhar o SDK") e a spec deixa livre; escolhi relay próprio (ADR-001). Isso muda a mitigação para "conformância contra o SDK nas versões do protocolo".

## Aprovações de dependências (Vinicius, 2026-10-29, horário de Brasília, repassadas pelo Product)
- 19:09: `modelcontextprotocol/go-sdk`, só em testes; `modernc.org/sqlite`.
- 19:10: `go.yaml.in/yaml/v3`, `tiktoken-go/tokenizer` v0.10.0 e `prometheus/client_golang` v1.25.0.

- 2026-10-29 20h00 (UTC-3): O prazo de encerramento do upstream fica fixo em 2s no código e não entra no YAML como `shutdown_grace`. Só vira configurável se algum upstream real precisar de mais tempo. O encerramento normal é do S1-02, e as falhas do upstream ficam com o S1-04.
