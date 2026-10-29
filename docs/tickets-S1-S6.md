# Tickets ctxgate MVP — S1 a S6

Origem: `PRD-ctxgate-mvp.md`, `spec-mvp.md`, ADRs 001–006 e `docs/adr/decisoes-cto.md` (todas aceitas por Vinicius em 2026-10-29), `docs/benchmark-plan.md` (QA, §10). Não publicado em tracker externo.
Revisão: v2 (Product, 2026-10-29). Substitui a v1 (`.bak/tickets-S1-S6.md.orig`).
Status: `ready-for-agent`, arquitetura validada pelas ADRs. Exceções marcadas em **Pendente de ok do Vinicius**.
Regra: cada semana soma no máximo 5,0h (hoje exatamente 5,0h, sem folga). Decisões técnicas vêm das ADRs; o Product não as reabre.

## Regras do Product (valem para todos os tickets)
1. **Chamadas sem consulta não são cortadas** (`query_source=none`): só dedup atua (ADR-002). A fração dessas chamadas é reportada no benchmark.
2. **Sucesso do benchmark = retenção de `required_evidence` em replay** (ADR-004). É um **proxy** e é rotulado assim em relatório, README e post: "sucesso (proxy: retenção de evidência)", nunca "tarefa concluída". Declaração no README: aprovada por Vinicius (ADR-004). Mesma declaração no post: **pendente de ok do Vinicius** (junto com a aprovação do post).
3. **Dependências novas dependem do ok do Vinicius** (lista abaixo).
4. Sem marcador de truncamento, sem dedup entre chamadas, sem `resources/read`/`prompts/get` no pipeline no MVP (ADR-006, ADR-001).
5. **Corpus do FitToday-cms é privado**: fica em `bench/corpus/` (no `.gitignore`), nunca no repo. O repo versiona harness, schema das tarefas, agregados e hash do corpus (ADR-004, adendo). Proibido anonimizar código do FitToday-cms. O corpus sintético público (`bench/public/`) é obrigatório.

## Dependências novas (go.mod)
| Dependência | Uso | Ticket | Situação |
|---|---|---|---|
| `modernc.org/sqlite` v1.61.0 | store | S2-04 | **Aprovada** (ADR-003) |
| `github.com/tiktoken-go/tokenizer` v0.10.0 (troca o `tiktoken-go` do PRD) | tokens | S2-01 | **Aprovada** (ADR-005) |
| `github.com/prometheus/client_golang` v1.25.0 | métricas | S4-01 | **Aprovada** (ADR-005) |
| `go.yaml.in/yaml/v3` v3.0.5 | YAML | S1-01 | **Aprovada** (Vinicius, 2026-10-29 19:10 BRT; registrada na ADR-001) |
| `github.com/modelcontextprotocol/go-sdk` v1.8.0, **só em testes** | conformância | S1-02, S1-03 | **Aprovada** (Vinicius, 2026-10-29 19:09 BRT; registrada na ADR-001) |

Todas as dependências acima estão aprovadas por Vinicius (2026-10-29).

## Resumo

| Semana | Tickets | Horas | Entrega demoável |
|---|---|---|---|
| S1 | 4 | 5,0 | Relay passthrough com 1 upstream, testado |
| S2 | 4 | 5,0 | Contagem de tokens, dedup (hash + similar), log SQLite |
| S3 | 4 | 5,0 | Consulta + BM25 + orçamento, padrão conservador |
| S4 | 4 | 5,0 | Prometheus, `ctxgate stats`, p95 medido |
| S5 | 5 | 4,75 | Benchmark em replay: corpus privado (agregados + hash) e corpus sintético público (`make bench-public`) |
| S6 | 4 | 5,0 | README, release goreleaser, rascunho do post |
| **Total** | **25** | **29,75** | |

Grafo de dependências (resumo): S1-01 → S1-02 → S1-03/S1-04 → S2-01 → S2-02 → S2-03; S2-01 + S1-02 → S2-04; S2-02 → S3-01 → S3-02 → S3-03 → S3-04; S3-* → S4-*; S2-04 → S5-01 e S5-05; S5-01 + S5-05 + S4-02 → S5-02 → S5-04 → S5-03; S5-* → S6-*.

---

# S1 — Relay MCP passthrough (5,0h)

## CTX-S1-01 — Esqueleto do projeto, CLI `serve` e config YAML
- **Semana:** S1 | **Estimativa:** 1,0h (sem mudança)
- **Referências:** ADR-001 (§2 layout, §6 YAML).
- **Contexto/porquê:** base de tudo; o usuário configura o ctxgate no agente como um servidor MCP e aponta o upstream por YAML.
- **Escopo:** módulo `github.com/Viniciuscarvalho/ctxgate`, `go 1.25`, `main` em `cmd/ctxgate/`, layout `internal/` do ADR-001; `ctxgate serve --config <arquivo>`; YAML com `go.yaml.in/yaml/v3` e `KnownFields(true)`; chave `upstreams` é lista validada para exatamente 1; CI (GitHub Actions, ubuntu) com `go vet`, `go test -race ./...` e lint básico; `.gitignore` ganha `*.db`, `*.db-wal`, `*.db-shm`, `ctxgate.yaml`, `bench/corpus/` (LICENSE e `.gitignore` já existem no repo).
- **Fora de escopo:** qualquer lógica de proxy; mais de 1 upstream; HTTP/SSE.
- **Dependências:** nenhuma de ticket. `go.yaml.in/yaml/v3` aprovada (Vinicius, 2026-10-29).
- **Critérios de aceite:**
  - [ ] Given um YAML válido com 1 upstream (`name`, `command`, `args`, `env`), When `ctxgate serve --config ctxgate.yaml` roda, Then inicia e registra o upstream em stderr; Given o stdin fechado (EOF), Then o contexto é cancelado e o processo sai com exit 0 (teste unitário com `strings.NewReader("")` e teste do binário com stdin fechado). O encerramento do upstream é critério do S1-02.
  - [ ] Given YAML inválido, chave desconhecida, zero upstream ou 2 upstreams, When o comando roda, Then sai com código ≠0 e mensagem apontando o campo (teste tabular cobre os 4 casos).
  - [ ] `go build ./... && go vet ./... && go test -race ./...` passam localmente e no CI.
  - [ ] `ctxgate.example.yaml` comentado, com as chaves do ADR-001 §6 e os padrões do `decisoes-cto.md` §1.
  - [ ] `go.mod` declara o módulo e `go 1.25`; `git check-ignore` confirma as 5 entradas novas do `.gitignore`.

## CTX-S1-02 — Relay stdio com 1 upstream (initialize, tools/list, tools/call)
- **Semana:** S1 | **Estimativa:** 2,0h (sem mudança; ver gatilho)
- **Referências:** ADR-001 (§1, §5, §7).
- **Contexto/porquê:** prova de que o proxy é transparente; sem isso o corte de contexto não tem onde entrar.
- **Escopo:** `internal/mcp`: relay JSON-RPC opaco (newline-delimited, sem reserializar), upstream como subprocesso stdio, decodificação só do envelope (`id`, `method`, `result`); tudo passa byte a byte, **sempre**: initialize, `tools/list`, resources, prompts, notificações, progress, cancelamento, pedidos do servidor ao cliente, MRTR. Com o pipeline desligado, `tools/call` também passa intacto. Leitura com `bufio.Reader` (não `Scanner`).
- **Fora de escopo:** dedup, ranking, orçamento, métricas, SQLite; mais de 1 upstream; HTTP/SSE; negociar versão de protocolo (o relay não negocia).
- **Dependências:** CTX-S1-01. `go-sdk` v1.8.0 só em testes, aprovado (Vinicius, 2026-10-29).
- **Gatilho de risco (ADR-001):** se passar de 3h sem passar o teste de conformância, o Staff avisa o Product e o CTO reabre a ADR-001. Nenhuma hora extra é planejada.
- **Critérios de aceite:**
  - [ ] Given um upstream MCP real (ex.: servidor de exemplo/filesystem), When um cliente conecta ao ctxgate, Then `tools/list` retorna os mesmos nomes e schemas do upstream.
  - [ ] Given uma `tools/call` válida e pipeline desligado, When passa pelo ctxgate, Then o `content` da resposta é byte-equivalente ao do upstream.
  - [ ] Given resources, prompts, notificações, progress e cancelamento, When passam, Then chegam sem alteração (teste com ≥1 mensagem de cada tipo).
  - [ ] Given 2 chamadas em voo, When respondem, Then os ids são preservados e cada resposta é correlacionada ao seu pedido (**a ordem de entrega não é garantida**; o teste não assume ordem).
  - [ ] Teste de conformância com o `go-sdk` v1.8.0 (cliente e servidor) através do relay nas versões de protocolo 2025-11-25 e 2026-07-28.
  - [ ] Given o upstream rodando, When o stdin do agente fecha (EOF), Then o upstream recebe SIGTERM e, se o prazo (`shutdown_grace`) estourar, kill; o `serve` sai com 0 e nenhum processo filho fica órfão (verificado em teste do binário).
  - [ ] Given o ctxgate configurado num agente real (Claude Code ou Cursor), When uso uma ferramenta do upstream, Then funciona igual a conectar direto (evidência: captura/log anexado ao ticket).

## CTX-S1-03 — Testes de integração com upstream fake
- **Semana:** S1 | **Estimativa:** 1,25h (sem mudança)
- **Referências:** ADR-001 (§5, riscos 2).
- **Contexto/porquê:** seam principal de teste (stdio); as semanas seguintes reaproveitam o harness.
- **Escopo:** upstream fake configurável (respostas fixas, latência, erro, saída grande) em `internal/testutil`, em **duas variantes**: subprocesso (re-exec do binário de teste) e `io.Pipe` em memória; cliente de teste; suíte de integração do passthrough.
- **Fora de escopo:** testes dos estágios do pipeline (S2+).
- **Dependências:** CTX-S1-02. `go-sdk` (só testes) aprovado.
- **Critérios de aceite:**
  - [ ] `go test -race ./...` roda a suíte sem rede nem servidores MCP externos, nas duas variantes do upstream fake.
  - [ ] Cobre: listagem de tools, chamada com sucesso, erro do upstream (chega intacto), resposta ≥1 MB (lida sem `bufio.Scanner`).
  - [ ] 20 chamadas concorrentes passam com `-race`, sem falhas nem respostas trocadas (correlação por id).
  - [ ] Cobertura de `internal/mcp` ≥70% (`go test -cover`).

## CTX-S1-04 — Ciclo de vida e erros do upstream
- **Semana:** S1 | **Estimativa:** 0,75h (sem mudança)
- **Referências:** ADR-001 (§5), `decisoes-cto.md` §1 (padrões) e §2 (S1-04).
- **Contexto/porquê:** proxy local que vaza processo ou trava quando o upstream cai destrói a confiança do único usuário.
- **Escopo:** encerramento ao fechar o stdin do agente (`shutdown_grace: 2s`: fecha stdin → SIGTERM → SIGKILL por grupo de processos; no Windows, melhor esforço, declarado); `startup_timeout: 30s` entre o 1º pedido do agente e a 1ª resposta; erro imediato se o `exec` falha; propagação de erro ao agente; sem timeout por chamada (cancelamento é do agente).
- **Fora de escopo:** reinício automático/retry do upstream.
- **Dependências:** CTX-S1-02, CTX-S1-03 (usa o harness).
- **Critérios de aceite:**
  - [ ] Given o agente fecha o stdio, When o ctxgate encerra, Then o subprocesso e seus filhos não ficam vivos (`pgrep` vazio em ≤2s; teste automatizado, Unix).
  - [ ] Given o upstream morre durante uma chamada, When o agente tinha pedidos pendentes, Then cada pedido recebe erro JSON-RPC `-32603`, o `serve` encerra com código ≠0 e não há panic nem reinício.
  - [ ] Given comando de upstream inexistente, When `serve` inicia, Then falha de imediato (erro de `exec`) com mensagem legível e exit ≠0.
  - [ ] Given upstream que não responde ao 1º pedido, When passam `startup_timeout` (configurável, teste com valor curto), Then o agente recebe erro e o processo encerra.
  - [ ] Logs só em stderr; stdout carrega só protocolo (teste verifica).

---

# S2 — Contagem de tokens, dedup e log SQLite (5,0h)

## CTX-S2-01 — Contagem de tokens antes/depois
- **Semana:** S2 | **Estimativa:** 0,75h (**era 1,0h**; −0,25h, biblioteca fina; horas vão para S2-04)
- **Referências:** ADR-005 (§1–§4), ADR-006.
- **Contexto/porquê:** sem contagem, não há economia nem benchmark; é o dado base das métricas.
- **Escopo:** `internal/tokens` com `Counter` sobre `tiktoken-go/tokenizer` v0.10.0, encoding `cl100k_base` registrado em cada `Record`; contagem **por trecho, em uma única passada**, em lotes com até `min(GOMAXPROCS,4)` workers e cache por hash do trecho na chamada; bytes de blocos não textuais registrados em coluna própria, sem tokens; em passthrough, a resposta é enviada primeiro e contada depois.
- **Fora de escopo:** tokenizers por modelo exato; exposição por métricas (S4).
- **Dependências:** CTX-S1-02. Dependência `tokenizer` **aprovada** (ADR-005).
- **Pergunta aberta ao CTO (P1):** o atalho do ADR-005 §4 (resposta com `bytes ≤ budget_tokens` pula a contagem síncrona) deixa `tokens_before/after` do `Record` com que valor? Sem contagem exata, o denominador do M1 no `stats` fica subestimado. Proposta do Product: contar fora do caminho crítico, como no passthrough. O critério abaixo assume essa proposta até a resposta.
- **Critérios de aceite:**
  - [ ] Teste tabular com ≥5 casos de referência (vazio, ASCII, pt-BR com acentos, código, JSON), resultado `cl100k_base` esperado fixo.
  - [ ] Given uma chamada pelo ctxgate, When termina, Then `tokens_before`, `tokens_after` e `encoding` ficam disponíveis no `Report`/`Record` (teste de integração), inclusive para respostas pequenas que pulam o pipeline.
  - [ ] Em passthrough, `tokens_before == tokens_after` e o envio ao agente não espera a contagem (teste com contagem artificialmente lenta).
  - [ ] Bloco não textual: bytes registrados, tokens não.
  - [ ] README/NOTES declara "tokens aproximados (cl100k_base)".

## CTX-S2-02 — Interface de estágio do pipeline + dedup por hash exato
- **Semana:** S2 | **Estimativa:** 1,75h (sem mudança)
- **Referências:** ADR-001 (§3, §4), ADR-006 (trecho, dedup).
- **Contexto/porquê:** primeira economia real e primeiro uso da interface de pipeline extensível (ponto central do portfólio).
- **Escopo:** `internal/pipeline`: `Pipeline.Run` (nunca erra, nunca panica, thread-safe), `Stage.Apply(ctx, *Doc)`, `Doc`/`Chunk`; segmentação em **parágrafos** (blocos de código cercados nunca partidos; trecho >512 tokens subdividido por linha; linha única gigante vira 1 trecho); dedup **exato** (sha256 truncado do trecho normalizado), mantendo a primeira ocorrência, **escopo = mesma resposta**; só o `result` de `tools/call` (sem erro, sem `structuredContent`, `resultType` completo) entra; falha/panic/timeout (`pipeline.timeout: 200ms`)/violação de invariante ⇒ resposta original da chamada inteira (`Mode=fallback`).
- **Fora de escopo:** similaridade (S2-03), ranking, orçamento; dedup entre chamadas ou persistido.
- **Dependências:** CTX-S1-03, CTX-S2-01.
- **Critérios de aceite:**
  - [ ] Given uma resposta com o mesmo parágrafo 3 vezes, When passa pelo ctxgate, Then o agente recebe o parágrafo 1 vez (a primeira ocorrência) e `tokens_after < tokens_before`.
  - [ ] Given resposta sem repetição, When passa, Then o conteúdo sai idêntico ao original.
  - [ ] Given bloco de código cercado com linhas repetidas internas, When passa, Then o bloco não é partido.
  - [ ] Given estágio injetado que erra, dá panic ou estoura o timeout, When a chamada ocorre, Then o agente recebe a resposta original, `Mode=fallback` e o erro é logado.
  - [ ] Given `isError:true`, `structuredContent`, `resultType:"input_required"`, bloco `image`/`audio`/`resource` ou tipo desconhecido, When passa, Then a resposta chega intacta, com `skip_reason`.
  - [ ] Um segundo estágio de teste registra-se na interface `Stage` sem alterar o relay nem o `Pipeline` (teste demonstra).
  - [ ] Invariantes testadas: saída ⊆ entrada, `tokens_after ≤ tokens_before`; testes tabulares + `-race` passam.

## CTX-S2-03 — Dedup por similaridade simples
- **Semana:** S2 | **Estimativa:** 1,25h (sem mudança)
- **Referências:** ADR-006 (dedup similar).
- **Contexto/porquê:** o PRD pede "hash e similaridade simples"; pega repetições com pequenas variações.
- **Escopo:** pré-filtro simhash de 64 bits (Hamming ≤6) + confirmação por Jaccard de shingles de 3 palavras ≥ `dedup.similarity` (padrão **0,90**; `1.0` desliga); só trechos ≥20 tokens (menores: só hash exato); mantém a primeira ocorrência.
- **Fora de escopo:** embeddings, LLM, similaridade semântica.
- **Dependências:** CTX-S2-02.
- **Critérios de aceite:**
  - [ ] Given dois trechos ≥20 tokens que diferem só em espaços/pontuação/1 palavra (casos listados em teste), When dedup, Then o duplicado é removido.
  - [ ] Given ≥5 pares de trechos distintos do mesmo tema, When dedup, Then ambos permanecem (0 falsos positivos no conjunto; qualquer falso positivo ⇒ limiar sobe).
  - [ ] Given trecho <20 tokens quase igual a outro, When dedup, Then só o hash exato age.
  - [ ] `dedup.similarity` é configurável no YAML (`1.0` desliga) e o padrão 0,90 está em `ctxgate.example.yaml`.
  - [ ] Benchmark Go do estágio com resposta de 100 KB roda e o resultado fica registrado no ticket (referência para o p95 de S4).

## CTX-S2-04 — Log SQLite por chamada
- **Semana:** S2 | **Estimativa:** 1,25h (**era 1,0h**; +0,25h, vindo de S2-01)
- **Referências:** ADR-003 (driver, esquema v1, gravação, privacidade, retenção).
- **Contexto/porquê:** base do replay e do benchmark reproduzível (S5) e do `stats` (S4).
- **Escopo:** `internal/store` com `modernc.org/sqlite` (`CGO_ENABLED=0`), WAL, `synchronous=NORMAL`, `busy_timeout=5000`; esquema v1 (`calls`, `payloads`; `PRAGMA user_version=1`; colunas `NOT NULL`); `Observer` que só faz `select { case ch <- rec: default: dropped++ }` (canal 1024) e um escritor único em lote (100 linhas ou 200 ms); `Flush`/`Close(ctx)` com prazo de 2s; **por padrão só metadados** (hash dos argumentos, contagens, tempos); conteúdo bruto (`payloads`, gzip) **só** com `store.capture_content: true` (modo captura); `session_id` aleatório; diretório 0700 e arquivo 0600; `retention_days` 30 aplicado na inicialização; caminho configurável (`store.path`).
- **Fora de escopo:** UI, sync, consultas além do `stats`; rotação sofisticada.
- **Dependências:** CTX-S2-01, CTX-S2-02. Dependência `modernc.org/sqlite` **aprovada** (ADR-003).
- **Critérios de aceite:**
  - [ ] Given 10 chamadas e `Flush`/encerramento, When consulto `select count(*) from calls`, Then retorna 10.
  - [ ] Cada linha tem tokens antes/depois, `overhead_ns`, `upstream_ns`, `total_ns`, `query_source`, `mode` e `config_hash` preenchidos (sem NULL).
  - [ ] Given 20 chamadas concorrentes com `-race`, Then todas são gravadas sem `database is locked` nem perda.
  - [ ] Given banco somente leitura ou fila cheia, When chamadas ocorrem, Then o agente recebe a resposta, o descarte incrementa o contador de descartes e é logado (teste), sem bloquear a chamada.
  - [ ] **Por padrão, a tabela `payloads` fica vazia** após N chamadas; com `capture_content: true`, `payloads` tem 1 linha por chamada com a resposta do upstream **antes** do pipeline.
  - [ ] Arquivo do banco com permissão 0600 e diretório 0700 (teste verifica); `retention_days` remove linhas mais antigas (teste com relógio injetado).
  - [ ] `PRAGMA integrity_check` = `ok` após encerramento por SIGINT.

---

# S3 — Consulta, ranking BM25 e orçamento (5,0h)

## CTX-S3-01 — `QueryResolver` e estágio de ranking BM25
- **Semana:** S3 | **Estimativa:** 2,0h (sem mudança; o `QueryResolver` e a heurística entram nesta estimativa)
- **Referências:** ADR-002 (consulta), ADR-006 (trecho).
- **Contexto/porquê:** decide o que manter de mais relevante quando precisa cortar, sem LLM. **Desbloqueado** pela ADR-002.
- **Escopo:** `QueryResolver` em 3 camadas: (1) `tools.<nome>.query_args` (fonte `config`); (2) heurística nos argumentos (strings de 1º nível e aninhadas até profundidade 2, ignorando chaves `id|ids|path|uri|url|cursor|limit|offset|format|lang`, valores <3 caracteres, teto 512 caracteres, termos únicos; fonte `auto`); (3) sem consulta (fonte `none`). Estágio `rank`: BM25 sobre os trechos da própria resposta (IDF variante Lucene, `k1=1,2`, `b=0,75`, tokenização por `unicode.IsLetter/IsDigit` minúscula, sem stopwords), desempate determinístico.
- **Fora de escopo:** embeddings, reranking por LLM, índice persistente, ler a conversa do agente.
- **Dependências:** CTX-S2-02.
- **Critérios de aceite:**
  - [ ] Given corpus de teste com resposta esperada conhecida (≥5 consultas), When ranqueado, Then o trecho relevante fica no top-3 em ≥4 dos 5 casos.
  - [ ] Given empate de score, When ranqueado, Then a mesma entrada dá a mesma saída em 100 execuções.
  - [ ] Given `query_args` configurado, When resolve, Then `query_source=config` e a consulta usa só esses argumentos; given só heurística, `query_source=auto` e as chaves ignoradas não entram; given nenhum argumento textual, `query_source=none`.
  - [ ] **Given consulta vazia (`none`) ou sem termos em comum, When passa pelo pipeline, Then não há corte (só dedup) e `query_source=none` fica no `Record`.**
  - [ ] O estágio implementa `Stage` sem alterar o relay nem o `Pipeline`.
  - [ ] Benchmark Go registrado com 100 KB / 200 trechos.

## CTX-S3-02 — Orçamento de tokens por chamada
- **Semana:** S3 | **Estimativa:** 1,5h (sem mudança)
- **Referências:** ADR-006 (itens 2, 4, 5), ADR-005 (§4).
- **Contexto/porquê:** é o corte propriamente dito, o que gera a economia ≥30%.
- **Escopo:** estágio `budget` que mantém os trechos de maior score até `budget_tokens` (padrão **6000**; resposta que cabe passa intacta); **ordem original dos trechos mantidos preservada**; **sem marcador de truncamento**; corte só quando há consulta (`query_source≠none`); política "nunca vazio".
- **Fora de escopo:** orçamento dinâmico por modelo; resumo do conteúdo cortado; marcador (reavaliar após S5, fase 2).
- **Dependências:** CTX-S3-01, CTX-S2-01.
- **Critérios de aceite:**
  - [ ] Given orçamento N, consulta presente e resposta com M>N tokens, When passa, Then `tokens_after <= N` (mesma contagem de S2-01) e os trechos mantidos são os de maior score.
  - [ ] Given M<=N, When passa, Then sai idêntica.
  - [ ] Given `query_source=none` e M>N, When passa, Then **não há corte por orçamento** (só dedup) e o `Report` registra o motivo.
  - [ ] Given orçamento menor que o menor trecho, When passa, Then mantém o trecho de maior score **inteiro** (único caso em que `tokens_after > N` é permitido) e registra `Reason`; a resposta nunca fica vazia.
  - [ ] A ordem original dos trechos mantidos é preservada (teste), e a saída não contém marcador nem texto novo.

## CTX-S3-03 — Configuração e padrões conservadores
- **Semana:** S3 | **Estimativa:** 0,75h (sem mudança)
- **Referências:** ADR-001 (§6), `decisoes-cto.md` §1 (padrões).
- **Contexto/porquê:** o risco nº1 do PRD é piorar a tarefa; o padrão tem de cortar pouco.
- **Escopo:** chaves YAML do ADR-001 §6 (`pipeline{enabled,timeout,max_in_flight,budget_tokens,dedup{enabled,similarity},rank{enabled,k1,b}}`); padrões: `budget_tokens: 6000`, `dedup.similarity: 0.90`, `k1=1.2`, `b=0.75`, `pipeline.timeout: 200ms`, `max_in_flight: 16`; **a única configuração por tool é `tools.<nome>.query_args`**; modo passthrough por `pipeline.enabled: false`.
- **Fora de escopo:** qualquer outra configuração por tool ou por upstream.
- **Dependências:** CTX-S3-02.
- **Critérios de aceite:**
  - [ ] Given `pipeline.enabled: false`, When rodo a suíte de integração de S1, Then ela passa sem alteração.
  - [ ] Given config sem a seção `pipeline`, When inicia, Then aplica os padrões acima e loga a config efetiva em stderr (com `config_hash`).
  - [ ] Valor inválido (`budget_tokens` ≤0, `similarity` fora de [0,1], `k1`/`b` negativos) falha na inicialização com mensagem apontando a chave.
  - [ ] `ctxgate.example.yaml` documenta cada chave e seu padrão.

## CTX-S3-04 — Testes de pipeline completo e de propriedade
- **Semana:** S3 | **Estimativa:** 0,75h (sem mudança)
- **Referências:** ADR-001 (§4, §5), ADR-006.
- **Contexto/porquê:** o corte nunca pode corromper o protocolo nem inventar conteúdo; é o ponto de "testes de concorrência" do PRD.
- **Escopo:** testes ponta a ponta com dedup→rank→budget, propriedades, concorrência.
- **Fora de escopo:** fuzzing extenso.
- **Dependências:** CTX-S3-02, CTX-S3-03.
- **Critérios de aceite:**
  - [ ] Propriedade (≥1000 casos, seed fixa): todo trecho da saída existe na entrada e `tokens_after <= tokens_before`, **exceto** o caso "nunca vazio" documentado em S3-02.
  - [ ] Propriedade: aplicar o pipeline duas vezes dá o mesmo resultado que uma (vale por não haver marcador).
  - [ ] 50 chamadas concorrentes com respostas diferentes passam com `-race`, sem mistura de conteúdo entre chamadas.
  - [ ] Resposta com blocos não textuais, `_meta` e campos desconhecidos chega com a estrutura MCP original (cliente de teste faz parse sem erro; campos não textuais byte a byte).

---

# S4 — Métricas, `stats` e p95 (5,0h)

## CTX-S4-01 — Coleta de métricas e endpoint Prometheus
- **Semana:** S4 | **Estimativa:** 1,75h (sem mudança)
- **Referências:** ADR-005 (§6, §7), ADR-003.
- **Contexto/porquê:** observabilidade é um dos pontos de portfólio; Prometheus é o formato esperado.
- **Escopo:** `internal/metrics` com `client_golang` v1.25.0, registry próprio e sem collectors Go/process por padrão; nomes: `ctxgate_calls_total{upstream,tool,mode}`, `ctxgate_tokens_before_total`, `ctxgate_tokens_after_total`, `ctxgate_tokens_saved_total`, `ctxgate_overhead_seconds` (histograma, buckets 0,5/1/2,5/5/10/15/20/30/50/100/250/500 ms e 1 s), `ctxgate_upstream_seconds`, `ctxgate_stage_errors_total{stage}`, `ctxgate_store_dropped_total`; endpoint **desligado por padrão** (`metrics.listen: ""`), bind em `127.0.0.1` quando ligado; porta ocupada ⇒ **degrada** (aviso em stderr, proxy segue).
- **Fora de escopo:** dashboards, autenticação, transporte MCP HTTP.
- **Dependências:** CTX-S2-04, CTX-S3-02. `client_golang` **aprovada** (ADR-005).
- **Critérios de aceite:**
  - [ ] Given 10 chamadas e `Flush` do store, When `curl 127.0.0.1:<porta>/metrics`, Then os contadores de tokens batem com o SQLite.
  - [ ] Given `metrics.listen` vazio, When `serve` inicia, Then não abre porta (teste).
  - [ ] Given a porta ocupada, When `serve` inicia, Then loga aviso claro e o proxy MCP continua funcionando.
  - [ ] `promtool check metrics` (ou equivalente) valida a saída.
  - [ ] Bind apenas em 127.0.0.1 (teste verifica).

## CTX-S4-02 — Comando `ctxgate stats`
- **Semana:** S4 | **Estimativa:** 1,5h (sem mudança)
- **Referências:** ADR-005 (§6).
- **Contexto/porquê:** resumo sem precisar de Prometheus; é a prova que vai para o README e o post.
- **Escopo:** `ctxgate stats [--db ... --since ...]` lendo o SQLite em conexão `mode=ro`: tokens economizados, % de economia, nº de chamadas, fração por `query_source` e `mode`, latência (overhead) p50/p95 **exatos** (nearest-rank no SQLite); saída legível e `--json`.
- **Fora de escopo:** gráficos, TUI, consultas arbitrárias.
- **Dependências:** CTX-S2-04, CTX-S4-01.
- **Critérios de aceite:**
  - [ ] Given banco de teste com valores conhecidos, When `ctxgate stats`, Then tokens economizados, %, p50/p95 e a fração `query_source=none` batem com o calculado à mão (teste golden; p95 por nearest-rank).
  - [ ] Given banco vazio, Then mensagem clara e exit 0; given banco inexistente, Then mensagem clara e exit ≠0 (documentado).
  - [ ] `--json` produz JSON válido (`jq .` passa) com os mesmos campos.
  - [ ] `stats` e `/metrics` concordam para a mesma sessão: **contadores idênticos**; percentis do `/metrics` apenas dentro do bucket do histograma.

## CTX-S4-03 — Medição de overhead e verificação do p95 <20ms
- **Semana:** S4 | **Estimativa:** 1,0h (sem mudança)
- **Referências:** ADR-005 (§3, §5, §6, riscos).
- **Contexto/porquê:** meta técnica do PRD; precisa separar o tempo do ctxgate do tempo do upstream.
- **Escopo:** overhead = `total_ns − upstream_ns` (relógio monotônico no relay); benchmark/teste de carga com upstream fake de latência controlada e payloads de 10 KB, 100 KB e 1 MB com pipeline completo e store ligado; relatório curto. Atenção (ADR-005): contar tokens de 100 KB custa ~10 ms em uma passada; a meta depende de contagem única e paralela.
- **Fora de escopo:** otimização profunda; se estourar, o plano B do ADR-005 (cache + 4 workers → estimar `tokens_before` por bytes → elevar o piso de tamanho) só entra com aviso ao PO antes de gastar horas.
- **Dependências:** CTX-S3-04, CTX-S4-01.
- **Critérios de aceite:**
  - [ ] Comando documentado (`make bench` ou `go test -bench`) imprime p50/p95 de overhead por tamanho de payload.
  - [ ] Overhead p95 <20ms para payloads de até 100 KB com pipeline completo e ≥1000 amostras (gate, C7 fechado); acima de 100 KB (ex.: 1 MB) só reportado.
  - [ ] Se a meta falhar, o ticket registra o estágio dominante (`pprof` anexado) e o PO é avisado antes de gastar mais horas.

## CTX-S4-04 — Testes de concorrência e robustez de métricas/log
- **Semana:** S4 | **Estimativa:** 0,75h (sem mudança)
- **Referências:** ADR-003 (riscos), ADR-001 (§5).
- **Contexto/porquê:** "testes de concorrência" é item explícito do diferencial de sênior.
- **Escopo:** testes sob `-race` com métricas, SQLite e pipeline ativos; carga curta (500 chamadas, 20 paralelas); shutdown durante carga; vazamento de goroutines por `runtime.NumGoroutine` antes/depois (stdlib, **sem adicionar `goleak`**).
- **Fora de escopo:** soak.
- **Dependências:** CTX-S4-01, CTX-S4-02.
- **Critérios de aceite:**
  - [ ] 500 chamadas com 20 paralelas: 0 erros, 0 data races, contagem em `/metrics` == linhas no SQLite == 500 (com `Flush`; descartes = 0).
  - [ ] SIGINT durante carga: termina em ≤5s, `NumGoroutine` volta ao valor inicial (±tolerância documentada) e `PRAGMA integrity_check` = `ok`.
  - [ ] O CI roda essa suíte com `-race`.

---

# S5 — Benchmark em replay: corpus privado e corpus público (4,75h)

> Semana de maior risco. Nada foi movido para S4/S6: o ajuste foi **cortar escopo dentro do PRD** (ver "Ajuste de horas" abaixo). Com o corpus privado em 10 tarefas (decisão do Vinicius, 2026-10-29), a S5 fica com 4,75h e 0,25h de folga.
> Duas faixas: **privada** (FitToday-cms: 10 tarefas reais, corpus em `bench/corpus/`, só agregados + hash no repo) e **pública** (corpus sintético em `bench/public/`, obrigatório, reproduzível por qualquer pessoa). O **gate da meta do PRD (≥30% de tokens, 10/10, p95 <20ms até 100 KB)** é medido na faixa privada; a faixa pública prova reprodutibilidade e roda no CI, e seus números não substituem o gate.
> Ordem de corte se atrasar (nesta ordem, e só dentro do PRD): (1) camada B (agente ao vivo) fora, sem horas planejadas; (2) métricas informativas do harness (IC por bootstrap, faixas extras) fora; (3) ~~corpus privado de 20 para 10~~ já aplicado (aprovado por Vinicius em 2026-10-29); nunca remover a medição de sucesso (proxy) nem o corpus público.
> Rótulos obrigatórios nos relatórios: "tokens (cl100k_base, aproximado)" e "sucesso (proxy: retenção de evidência)".

## CTX-S5-01 — Tarefas, `required_evidence` e captura do corpus privado
- **Semana:** S5 | **Estimativa:** 1,0h (**era 1,5h**; −0,5h: captura fora das 5h e 10 tarefas em vez de 20)
- **Referências:** ADR-004 (camada A, adendo), ADR-003 (modo captura), `docs/benchmark-plan.md` §3, §10 (C2, C3, C4).
- **Contexto/porquê:** a meta "≥30% menos tokens sem perder a tarefa" só vale com tarefas reais e critério objetivo; o FitToday-cms é privado, então a parte pública do repo não pode conter conteúdo dele.
- **Escopo:**
  - Schema `bench/tasks/T01.yaml`…`T10.yaml` (versionado): `id`, `category`, `prompt` genérico (sem nomes de arquivo, símbolo, regra de negócio ou **código**), `upstream`, `recorded_calls` (só metadados: `tool`, `args_hash`, `query_source` esperado, bytes, tokens, `corpus_ref`) e `required_evidence` (1–4 por tarefa) com `description` abstrata e hash do padrão; **nenhum código nem trecho de resposta no repo**. O padrão literal (substring/regex) fica só em `bench/corpus/evidence/`.
  - Captura: as 10 tarefas são trabalhadas por Vinicius no **trabalho normal do FitToday-cms** com o ctxgate em `pipeline.enabled=false` e `store.capture_content: true` (disponível desde a S2-04), de S3 a S5; **essas horas de trabalho real não contam nas 5h do S5**. Nas 1,0h entram: schema, escrita das 10 tarefas e evidências, e a triagem do que foi capturado.
  - Corpus em `bench/corpus/` (no `.gitignore`); nada de anonimizar código do FitToday-cms (proibido, ADR-004).
- **Fora de escopo:** automação da execução (S5-02); camada B (agente ao vivo, opcional e fora do comando automatizado); tarefas de outros projetos; critério "comando pass/fail" (vira apêndice da camada B, não é da camada A).
- **Dependências:** CTX-S2-04 (modo captura e formato do log).
- **Pendente de ok do Vinicius / CTO:** formato do hash da evidência (HMAC com salt privado, proposta do QA, conflito C2) e `description`-só no repo (ADR-004 adendo: "escrito sem colar código"). O CTO confirma o formato; o critério abaixo assume HMAC-SHA256.
- **Critérios de aceite:**
  - [ ] `bench/tasks/` com exatamente 10 arquivos; cada um com `id`, `category`, `prompt` genérico, `upstream`, `recorded_calls` (metadados) e ≥1 `required_evidence` (`description` + hash do padrão).
  - [ ] **10/10 tarefas** com `required_evidence` verificável automaticamente (a camada A não depende de revisão humana nem de comando pass/fail).
  - [ ] Cada evidência é verificada no corpus gravado **antes** de qualquer rodada com ctxgate (casa no baseline; evidência que não casa é corrigida) e tem ≥12 caracteres no padrão literal, sem casar em outra tarefa.
  - [ ] Nenhum arquivo versionado contém código, caminho de arquivo, símbolo ou trecho de resposta do FitToday-cms (revisão do Vinicius; checklist no ticket); `git ls-files bench/corpus` retorna vazio e `.gitignore` contém `bench/corpus/`.
  - [ ] As 10 tarefas cobrem as categorias do plano do QA (com consulta e corte esperado, evidências espalhadas, repetição, consulta curta, sem consulta, abaixo do orçamento, código cercado), incluindo as que **não** cortam.
  - [ ] Vinicius aprova a lista de tarefas e as evidências antes do S5-02 rodar com o ctxgate; as evidências ficam **congeladas** (hash commitado) a partir daí.

## CTX-S5-05 — Exportador do SQLite e hash do corpus *(novo)*
- **Semana:** S5 | **Estimativa:** 0,5h (novo; vem do conflito C4 do QA)
- **Referências:** ADR-003 (`payloads`), ADR-004, `docs/benchmark-plan.md` §2.4 e §8.
- **Contexto/porquê:** o exportador não estava em nenhum ticket; sem ele a captura (SQLite) não vira corpus replayável, e sem hash não há prova de que todas as rodadas usaram o mesmo corpus.
- **Escopo:** `go run ./bench export --db <capture.db> --out bench/corpus/` gera `T01.jsonl.gz`… (`{task_id, seq, upstream, tool, args, content, is_error}`; `content` = resposta do upstream antes do pipeline); `go run ./bench hash --corpus <dir>` imprime o hash do corpus (sha256 de cada arquivo **descomprimido**; manifesto ordenado por nome; hash = sha256 do manifesto); o harness aborta se o hash diferir do informado.
- **Fora de escopo:** qualquer UI; anonimização (proibida para o FitToday-cms); exportar para o repo.
- **Dependências:** CTX-S2-04.
- **Critérios de aceite:**
  - [ ] Given um `capture.db` de teste gerado em modo captura (corpus sintético, não o privado), When `export`, Then gera 1 arquivo por tarefa com as respostas na ordem de `seq`.
  - [ ] `hash` é idêntico em duas execuções e em duas máquinas para o mesmo corpus; mudar 1 byte muda o hash.
  - [ ] `export` recusa gravar dentro de caminho versionado fora de `bench/corpus/` e `bench/public/` (teste).

## CTX-S5-02 — Harness de benchmark em replay (baseline vs. ctxgate)
- **Semana:** S5 | **Estimativa:** 1,5h (**era 2,0h**; −0,5h: cortadas as métricas informativas)
- **Referências:** ADR-004, ADR-005 (§6), `docs/benchmark-plan.md` §1, §2, §5–§7.
- **Contexto/porquê:** o benchmark é a prova de portfólio e o gate de qualidade do ranking conservador.
- **Escopo:** `make bench-e2e` chama `go run ./bench`: **A1** `pipeline.Run` offline por chamada gravada nas configs `passthrough` e `ctxgate` (tokens e sucesso); **A2** relay completo contra upstream fake que serve as respostas gravadas (overhead = `total_ns − upstream_ns`, warmup descartado, p50/p95 por nearest-rank, ≥1000 amostras, senão "indicativo"); checagem cruzada A1×A2 (texto entregue idêntico); `make bench-verify` (2 execuções); relatório `report.json` (separa `deterministic{}` de `timing{}`), `report.csv`, `report.md` em `bench/results/<data>-<commit>/` **sem** texto de resposta, prompt real nem padrão de evidência; versões, commit (`dirty` ⇒ "não publicável"), `config_hash` e hash do corpus no relatório.
- **Fora de escopo:** CI do benchmark de latência, comparação com outros proxies, dashboards; IC por bootstrap, mediana/macro e faixas extras de tamanho (informativos, cortados nesta versão); camada B.
- **Dependências:** CTX-S5-01, CTX-S5-05, CTX-S4-02.
- **Critérios de aceite:**
  - [ ] Given o repo limpo e o corpus informado, When rodo `make bench-e2e`, Then gera o relatório para as 10 tarefas do corpus privado (e as 20 do público, S5-04) nas 2 configs sem passos manuais ocultos.
  - [ ] Validade do baseline: em `passthrough`, todas as tarefas do corpus (10/10 no privado, 20/20 no público) com 100% das evidências retidas e `tokens_after == tokens_before` em toda chamada; senão a rodada é inválida.
  - [ ] Determinismo: `make bench-verify` (2 execuções seguidas) dá o mesmo `deterministic{}` (tokens, sucesso, hash do corpus), variância 0.
  - [ ] O relatório traz por tarefa: tokens baseline, tokens ctxgate, % economia, sucesso baseline/ctxgate (**"sucesso (proxy: retenção de evidência)"**), evidências retidas/total e, para evidência perdida, só o `id`, a chamada, `mode`, `query_source` e o motivo (`dup:exact|dup:near|budget`); e totais com p50/p95 de overhead, `Σ tokens`, fração de chamadas por `query_source` e contagem de `mode=fallback`.
  - [ ] A falha de uma tarefa não aborta as demais; `mode=fallback` >0 sem explicação reprova a rodada.
  - [ ] O hash do corpus é recalculado ao iniciar e a rodada aborta se divergir do esperado.

## CTX-S5-04 — Corpus sintético público e `make bench-public` *(novo)*
- **Semana:** S5 | **Estimativa:** 1,0h (novo; ADR-004 adendo; C10 do QA)
- **Referências:** ADR-004 (adendo), `docs/benchmark-plan.md` §8 e §10 (C1, C10).
- **Contexto/porquê:** o corpus do FitToday-cms é privado; sem um corpus público terceiros não reproduzem nada e o CI não testa o harness. O PRD pede benchmark "reproduzível" e o README precisa de uma tabela que qualquer pessoa rode.
- **Escopo:** gerador (`go run ./bench/public/gen`, seed fixa) que produz, em `bench/public/` (versionado, fora do `.gitignore`), um repo Go **fictício** e respostas de upstream sintéticas com **as mesmas 20 formas de tarefa** (as categorias do S5-01, incluindo sem consulta, abaixo do orçamento e repetição), 20 tarefas com `required_evidence` e o corpus no mesmo formato do S5-05; `make bench-public` roda gerador (ou verifica) + harness sobre esse corpus; relatório público em `bench/results/public/`.
- **Fora de escopo:** qualquer conteúdo, estrutura ou nomes derivados do FitToday-cms; ajustar o gerador para atingir 30% (o corpus mostra o comportamento, não é o gate).
- **Dependências:** CTX-S5-02 (harness e formato), CTX-S5-05.
- **Critérios de aceite:**
  - [ ] Given o repo limpo e Go ≥1.25, When `make bench-public`, Then gera o relatório sem corpus privado e sem rede.
  - [ ] Dois runs (e duas máquinas) dão o **mesmo hash do corpus e os mesmos totais de tokens e sucesso**; regenerar com a seed fixa não altera nenhum arquivo versionado (`git diff` vazio).
  - [ ] O corpus público tem 20 tarefas (as 20 formas de tarefa, cobrindo as categorias do S5-01) e `required_evidence` verificável, e `passthrough` dá 20/20.
  - [ ] Nenhum dado do FitToday-cms aparece em `bench/public/` (checagem contra a `denylist` local do Vinicius, executada por ele).
  - [ ] Roda no CI (ubuntu) em tokens, sucesso e determinismo; **a latência não é gate no CI**.
  - [ ] O relatório é rotulado "corpus sintético, não representa o FitToday-cms".

## CTX-S5-03 — Rodada oficial no corpus privado, config congelada e relatório
- **Semana:** S5 | **Estimativa:** 0,75h (**era 1,5h**; −0,75h)
- **Referências:** ADR-002, ADR-004 (adendo), ADR-006 (alavanca de sessão), `docs/benchmark-plan.md` §7 e §10 (C5, C7, C8).
- **Contexto/porquê:** fecha o gate técnico: ≥30% menos tokens, sucesso 10/10 (proxy) no corpus privado e overhead p95 <20ms em respostas até 100 KB.
- **Escopo:** (1) exploração limitada: **no máximo 3 configurações** (só `budget_tokens`, `dedup.similarity`, `k1`/`b`), todas registradas no relatório; (2) **congelar a config** (commit e `config_hash`) **antes** da rodada oficial; (3) rodada oficial no corpus privado; (4) relatório versionado só com agregados + hash; (5) checagem de privacidade.
- **Fora de escopo:** novos estágios ou algoritmos; mudar a arquitetura; ajustar evidências; camada B. Se a meta não for atingida, o resultado honesto é publicado e o PO decide o próximo passo (primeira alavanca: dedup de sessão, nova ADR com aprovação do PO).
- **Dependências:** CTX-S5-02, CTX-S5-04.
- **Critérios de aceite:**
  - [ ] A config da rodada oficial está commitada e o `config_hash` do relatório bate com ela; as configs exploradas (≤3) estão listadas com valores e resultado.
  - [ ] Relatório em `bench/results/<data>-<commit>/` com data, commit (`dirty=false`), `config_hash`, hash do corpus, versões, máquina e **só agregados** (nenhum trecho de resposta, prompt real ou padrão de evidência).
  - [ ] Economia total de tokens declarada: `1 − Σafter/Σbefore` ≥30% (sim/não, valor), com a fração de chamadas `query_source=none` e a economia só nas chamadas elegíveis.
  - [ ] **Sucesso (proxy: retenção de evidência) do ctxgate = 10/10** tarefas do corpus privado com 100% das evidências retidas (sim/não, valor); o baseline é 10/10 por construção e serve só de checagem de validade (S5-02).
  - [ ] **Gate:** overhead p95 <20ms nas chamadas com resposta ≤100 KB, com ≥1000 amostras (sim/não, valor, nº de amostras). Chamadas >100 KB: p95 só reportado, não é gate (C7 fechado, decisão do Vinicius em 2026-10-29).
  - [ ] Se qualquer meta falhar, o relatório traz na primeira linha qual meta falhou, as tarefas que regrediram, causa provável e recomendação, e o PO é avisado.
  - [ ] `git ls-files bench/corpus` vazio e busca dos termos da `denylist` local nos arquivos versionados e no relatório sem ocorrências (executada por Vinicius ou pelo QA).
  - [ ] Os padrões finais ficam em `ctxgate.example.yaml`.

### Ajuste de horas da S5 (v1 → v2)
| Ticket | v1 | v2 | Motivo |
|---|---|---|---|
| S5-01 tarefas, evidências, captura | 1,5 | 1,00 | captura é trabalho normal do Vinicius com `capture_content` (fora das 5h); corpus privado com 10 tarefas |
| S5-05 exportador + hash (novo) | — | 0,50 | C4 |
| S5-02 harness | 2,0 | 1,50 | informativos cortados |
| S5-04 corpus sintético público (novo) | — | 1,00 | ADR-004 adendo, C10 |
| S5-03 rodada oficial | 1,5 | 0,75 | ≤3 configs; sem análise longa; relatório só agregados |
| **Total** | **5,0** | **4,75** | 0,25h de folga |

---

# S6 — README, release e post (5,0h)

## CTX-S6-01 — README com as duas tabelas do benchmark
- **Semana:** S6 | **Estimativa:** 2,0h (sem mudança)
- **Referências:** ADR-004 (adendo), ADR-005, ADR-002, ADR-003, `docs/benchmark-plan.md` §11.
- **Contexto/porquê:** o README é a vitrine do portfólio; precisa convencer um recrutador/dev Staff em 2 minutos.
- **Escopo:** problema, saída do `stats`, instalação, config mínima para Claude Code/Codex/Cursor, diagrama do pipeline, **duas tabelas**: privada (FitToday-cms: só agregados, hash do corpus, commit, `config_hash`) e pública (corpus sintético, reproduzível); como reproduzir; limitações honestas; fora de escopo/roadmap (fase 2: sync, `ctxgate_expand`, dedup de sessão).
- **Fora de escopo:** site de docs, tutoriais longos, UI.
- **Dependências:** CTX-S5-03, CTX-S5-04.
- **Critérios de aceite:**
  - [ ] Os números das duas tabelas batem com os relatórios do S5-03 e do S5-04 (checagem linha a linha).
  - [ ] **A reprodução por terceiros vale para o corpus sintético:** um terceiro (ou agente em ambiente limpo) segue o README do zero, roda `make bench-public` e obtém o mesmo hash de corpus e os mesmos totais de tokens e sucesso, e chega ao `ctxgate stats` com dados, em ≤15 min (registrar no ticket).
  - [ ] O README diz que os números do FitToday-cms **não** são reproduzíveis por terceiros (corpus privado), que só se verificam por hash e harness, e como capturar o próprio corpus (`store.capture_content`).
  - [ ] Declarações obrigatórias presentes: **"sucesso (proxy: retenção de evidência), não equivale a tarefa concluída por um agente"**; "tokens aproximados (cl100k_base)"; "chamadas sem consulta não são cortadas" com a fração medida; "p95 <20ms medido para respostas até 100 KB (≥1000 amostras); acima disso só reportado"; "o log guarda só metadados por padrão; replay exige modo captura"; "Windows sem smoke test"; "o replay não vê mudança de comportamento do agente; 10 tarefas de um só projeto (corpus privado)".
  - [ ] Contém: instalação, config de pelo menos 1 agente, diagrama do pipeline, comandos de reprodução, seção de limitações; links e comandos testados (nenhum link quebrado).

## CTX-S6-02 — Release com goreleaser e `go install`
- **Semana:** S6 | **Estimativa:** 1,5h (sem mudança; ~0,15h já consumidas pelas licenças, dentro da estimativa)
- **Referências:** ADR-003 (§12 plataformas), `decisoes-cto.md` §2 (S6-02).
- **Contexto/porquê:** instalação sem atrito é requisito do PRD.
- **Escopo:** goreleaser com `CGO_ENABLED=0`, alvos linux (amd64/arm64), darwin (amd64/arm64) e windows/amd64; checksums; release `v0.1.0` por tag; versão via `debug.ReadBuildInfo` (para `go install`) e `-ldflags` no goreleaser; **texto das licenças das dependências redistribuídas** (modernc BSD-3, client_golang Apache-2.0, tokenizer MIT) nos artefatos; LICENSE (MIT) **já existe**.
- **Fora de escopo:** Homebrew/apt/Docker, assinatura de binários, smoke test no Windows (declarado no README).
- **Dependências:** CTX-S6-01, CTX-S4-04.
- **Critérios de aceite:**
  - [ ] `goreleaser release --snapshot --clean` gera binários dos 5 alvos e checksums sem erro, sem CGO.
  - [ ] Os artefatos incluem o texto das licenças das 3 dependências redistribuídas e a LICENSE do projeto.
  - [ ] A tag `v0.1.0` publica a release no GitHub (**publicar só com aprovação do Vinicius**).
  - [ ] `go install github.com/Viniciuscarvalho/ctxgate/cmd/ctxgate@v0.1.0` instala e `ctxgate --version` mostra `v0.1.0` (diretório limpo); o binário do goreleaser mostra a mesma versão.
  - [ ] Binário baixado da release roda `ctxgate serve` com o `ctxgate.example.yaml`.

## CTX-S6-03 — Rascunho do post no X com números
- **Semana:** S6 | **Estimativa:** 0,75h (sem mudança)
- **Referências:** ADR-004, regras do Product.
- **Contexto/porquê:** o objetivo de visibilidade do PRD; a meta de ≥100 stars em 60 dias depende disso.
- **Escopo:** rascunho do post/thread em pt-BR (e opcional em inglês) com números do benchmark, link do repo e saída do `stats`; checklist de números vs. relatórios.
- **Fora de escopo:** publicar; mídia paga; outros canais.
- **Dependências:** CTX-S5-03, CTX-S6-02.
- **Critérios de aceite:**
  - [ ] Rascunho em `docs/post-x.md`, todos os números rastreados aos relatórios do S5-03/S5-04; 0 números sem fonte (checklist).
  - [ ] O post diz que o sucesso é um **proxy (retenção de evidência)** e que os números do FitToday-cms vêm de corpus privado (verificável por hash), apontando o corpus sintético público para reprodução. **Redação do proxy no post: pendente de ok do Vinicius.**
  - [ ] Respeita o limite de caracteres (ou está em thread) e tem link do repo.
  - [ ] Aprovação do Vinicius registrada antes de qualquer publicação (**o ticket não publica**).

## CTX-S6-04 — Verificação final do MVP contra o PRD
- **Semana:** S6 | **Estimativa:** 0,75h (sem mudança)
- **Contexto/porquê:** garante que o MVP entregue bate com a seção 5 e as métricas da seção 7 antes de divulgar.
- **Escopo:** checklist da seção 5 e da seção 7, smoke test de instalação limpa, lista de pendências para depois (fase 2).
- **Fora de escopo:** novas features; medir stars/usuários.
- **Dependências:** CTX-S6-01, CTX-S6-02.
- **Critérios de aceite:**
  - [ ] Checklist com cada item da seção 5 e evidência (comando ou link): `serve`, dedup, BM25, orçamento, contagem de tokens (`tiktoken-go/tokenizer`, troca registrada em ADR-005), Prometheus, `stats`, SQLite, `go install`, goreleaser.
  - [ ] Métricas técnicas com valor e fonte: ≥30% de economia (privado), sucesso 10/10 **(proxy)** no privado, p95 <20ms para respostas ≤100 KB (≥1000 amostras) — ou desvio declarado.
  - [ ] `bench/corpus/` fora do repo (`git ls-files bench/corpus` vazio) e `make bench-public` verde no commit da tag.
  - [ ] `go test -race ./...` verde no commit da tag.
  - [ ] Arquivo de pendências/roadmap criado (marcador de truncamento/`ctxgate_expand`, dedup de sessão, `resources/read`, camada B); nada fora da seção 5 foi implementado.

---

# Decisões do CTO e pendências

## Questões do CTO: resolvidas (ver ADRs)
| # | Questão | Onde |
|---|---|---|
| 1 | Relay próprio; go-sdk só em testes | ADR-001 |
| 2 | Só `result` de `tools/call` no pipeline | ADR-001, ADR-006 |
| 3 | Consulta: config → heurística → sem consulta = não corta | ADR-002 |
| 4 | Trecho = parágrafo; não textual passa intacto | ADR-006 |
| 5 | Dedup dentro da resposta | ADR-006 |
| 6 | `Pipeline.Run` + `Stage`; falha ⇒ resposta original | ADR-001 |
| 7 | 1 sessão, 1 upstream, várias chamadas em voo | ADR-001 |
| 8 | `modernc.org/sqlite`, WAL, escritor em lote, só metadados | ADR-003 |
| 9 | Replay determinístico; sucesso = retenção de evidência; corpus privado + público | ADR-004 (+ adendo) |
| 10 | `client_golang`; p50/p95 exatos no SQLite | ADR-005 |
| 11 | `cl100k_base`, `tiktoken-go/tokenizer` | ADR-005 |
| 12 | `CGO_ENABLED=0`; 5 alvos | ADR-003 |
| 13 | Sem marcador de truncamento | ADR-006 |

## Pendências abertas
1. **Vinicius:** ok da redação do proxy no post (S6-03). No README já aprovado (ADR-004).
2. **Staff:** push dos docs (PRD, spec, tickets, ADRs, plano) para `docs/` no repo, aprovado por Vinicius em 2026-10-29.
3. **CTO (P1):** valor de `tokens_before/after` quando o atalho do ADR-005 §4 pula a contagem (S2-01).
4. **CTO (C2):** formato do hash da evidência no repo (HMAC com salt privado) (S5-01).
5. **QA/CTO:** nomes: o ADR-004 usa `bench/public/` e `make bench-public`; o plano do QA usa `bench/corpus-synthetic/` e `make bench-smoke`. Os tickets seguem o ADR; o QA ajusta o plano. Limpeza do texto de riscos do ADR-004 (C9).

## Riscos
1. **Quase sem folga:** 29,75h de 30h (só 0,25h na S5). Se S1 ou S2 estourarem >1h, o PO reordena e corta dentro do PRD (primeira candidata a adiar: S2-03, similaridade). Decisão com o Vinicius.
2. **S5 é a semana mais incerta** e foi apertada para caber duas faixas de corpus: harness 1,5h e rodada oficial 0,75h. Cortes na ordem descrita no topo da S5.
3. **Meta ≥30% pode falhar** porque (a) respostas ≤6000 tokens passam intactas, (b) `query_source=none` não corta e (c) o dedup é só dentro da resposta. Resultado honesto é publicado; alavanca: dedup de sessão (nova ADR, aprovada pelo PO).
4. **Sucesso é um proxy:** retenção de evidência não garante que o agente acerta. Declarado em relatório, README e post.
5. **Meta p95 <20ms:** a mais apertada do projeto; gate só para respostas ≤100 KB com ≥1000 amostras (C7 fechado); S4-03 mede e S5-03 reporta.
6. **S1-02 (2,0h) é apertado:** gatilho de 3h do ADR-001.
7. **Contagem de tokens aproximada** (cl100k_base); só a redução relativa é comparável.
8. **Privacidade:** corpus do FitToday-cms nunca vai ao repo; `.gitignore`, verificação no S5-03 e revisão do Vinicius. Log SQLite guarda só metadados por padrão.
9. **Sobreajuste:** config ajustada nas mesmas 10 tarefas privadas; mitigado por ≤3 configs registradas, config congelada antes da rodada oficial e corpus sintético independente.
10. **Metas de portfólio e produto** (≥100 stars, ≥10 usuários) são pós-MVP; só post e README entram.
11. **Publicações externas** (release no GitHub, post no X, push dos docs) exigem aprovação explícita do Vinicius.
