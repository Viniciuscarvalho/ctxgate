# Spec — ctxgate MVP

Origem: `PRD-ctxgate-mvp.md`. Gerado com a skill to-spec, **sem publicar em tracker externo** (pedido do PO/IdeaValidator). Tickets em `tickets-S1-S6.md`.
Revisão v2 (Product, 2026-10-29): incorpora as ADRs 001–006 (aceitas por Vinicius), `docs/adr/decisoes-cto.md` e `docs/benchmark-plan.md` (QA). A v1 está em `.bak/spec-mvp.md.orig`.

## Problem Statement

Sou um dev solo que usa vários agentes de código (Claude Code, Codex, Cursor) com servidores MCP de memória e contexto. Esses servidores devolvem contexto demais ou repetido. Isso gasta tokens, polui o prompt do agente e eu não tenho como medir o desperdício nem saber se cortar contexto piora a tarefa.

## Solution

Um proxy MCP local em Go (`ctxgate`) que fica entre o agente e os servidores MCP upstream. A cada chamada ele:
1. remove trechos repetidos (hash exato e similaridade simples);
2. quando a chamada tem uma consulta (config por tool ou heurística nos argumentos, ADR-002), ranqueia o que sobrou por relevância (BM25 local, sem LLM) e corta pelo orçamento de tokens da chamada. **Chamadas sem consulta não são cortadas: só o dedup atua**;
3. mede tokens antes/depois e latência, e expõe isso por endpoint Prometheus e pelo comando `ctxgate stats`.

Cada chamada fica registrada em SQLite (por padrão só metadados; o conteúdo só em modo captura, ADR-003), para replay e benchmark. O resultado é provado por um benchmark em replay de 10 tarefas reais no FitToday-cms (**corpus privado**: o repo guarda só harness, schema, agregados e hash do corpus) e por um **corpus sintético público** que qualquer pessoa reproduz com `make bench-public` (ADR-004). Meta: ≥30% menos tokens, overhead p95 <20ms em respostas até 100 KB (≥1000 amostras; acima disso só reportado) e **sucesso = retenção de `required_evidence` (proxy, não "tarefa concluída")**. O gate é medido no corpus privado (sucesso 10/10); o público (10 formas de tarefa, ADR-004 Adendo 3) prova a reprodutibilidade.

## User Stories

Configuração e execução
1. Como dev, quero rodar `ctxgate serve` como um servidor MCP via stdio, para configurá-lo no meu agente como se fosse qualquer outro servidor MCP.
2. Como dev, quero apontar os servidores upstream num arquivo YAML, para trocar de upstream sem recompilar.
3. Como dev, quero uma mensagem de erro clara quando o YAML é inválido ou o upstream não sobe, para corrigir rápido.
4. Como dev, quero que o ctxgate encerre limpo (sem processo zumbi do upstream) quando o agente fecha o stdio, para não vazar processos.
5. Como dev, quero que o ctxgate funcione em modo passthrough puro (sem cortes), para ter uma linha de base e poder desligar o corte sem remover o proxy.
6. Como dev, quero que o agente continue enxergando as ferramentas, recursos e prompts do upstream com os mesmos nomes e schemas, para não mudar meus fluxos.

Corte de contexto
7. Como dev, quero que trechos idênticos sejam removidos da resposta, para não pagar tokens duas vezes pelo mesmo conteúdo.
8. Como dev, quero que trechos quase idênticos (similaridade simples) também sejam deduplicados, para pegar repetições com pequenas variações.
9. Como dev, quero que o que sobrar seja ranqueado por relevância (BM25) em relação à consulta da chamada, para o mais útil ficar dentro do orçamento. Quando não há consulta, quero que nada seja cortado (só dedup), para não arriscar a tarefa.
10. Como dev, quero definir um orçamento de tokens por chamada, para limitar o contexto injetado no agente.
11. Como dev, quero que o padrão seja conservador (corta pouco e nunca devolve resposta vazia), para não quebrar a tarefa por padrão.
12. Como dev, quero que uma resposta que já cabe no orçamento passe sem alteração, para não perder informação à toa.
13. Como dev, quero que respostas não textuais ou de formato desconhecido passem intactas, para não corromper o protocolo.
14. Como dev, quero que erros do upstream cheguem ao agente sem alteração, para o agente decidir o que fazer.

Medição e observabilidade
15. Como dev, quero ver os tokens antes e depois de cada chamada, para saber quanto foi economizado.
16. Como dev, quero um endpoint Prometheus com tokens economizados, % e latência, para plugar em dashboard.
17. Como dev, quero rodar `ctxgate stats` e ver tokens economizados, % e latência p50/p95, para ter o resumo sem subir Prometheus.
18. Como dev, quero que o overhead de latência do ctxgate seja medido separadamente da latência do upstream, para provar o p95 <20ms.
19. Como dev, quero um log SQLite de cada chamada (só metadados por padrão; conteúdo apenas em modo captura que eu liguei), para reexecutar (replay) chamadas passadas e comparar configurações sem vazar conteúdo por padrão.

Benchmark e distribuição
20. Como dev, quero um benchmark em replay de 10 tarefas reais do FitToday-cms comparando baseline e ctxgate, para medir economia e taxa de sucesso, onde sucesso = retenção das evidências obrigatórias (proxy, rotulado assim).
21. Como dev, quero rodar o benchmark com um comando e obter os mesmos números; e como terceiro, quero rodar `make bench-public` (corpus sintético versionado) e obter os mesmos números, já que o corpus do FitToday-cms é privado e só se verifica por hash.
22. Como visitante do GitHub, quero um README com instalação, config, as duas tabelas do benchmark (privada: agregados e hash; pública: reproduzível) e as limitações (sucesso é proxy, tokens aproximados, sem consulta não corta), para avaliar o projeto em 2 minutos.
23. Como usuário, quero instalar com `go install` ou baixar um binário nos Releases, para começar sem toolchain específica.
24. Como dev, quero um post no X com os números e a ressalva de que o sucesso é um proxy, para dar visibilidade ao projeto (publicação e redação do proxy só com minha aprovação).

Qualidade
25. Como mantenedor, quero testes de concorrência (`-race`) no proxy e no pipeline, para provar que chamadas simultâneas não corrompem estado.
26. Como mantenedor, quero um upstream fake nos testes, para validar o proxy sem depender de servidores reais.

## Implementation Decisions

Já fixadas no PRD (não reabrir sem o PO):
- Linguagem Go; transporte apenas stdio; config em YAML.
- Pipeline conceitual: router → dedup → rank → budget → métricas, com estágios plugáveis via interface.
- Pacotes citados no PRD: protocolo MCP, pipeline, store (SQLite), metrics.
- BM25 local, sem LLM e sem embeddings; contagem de tokens aproximada via tiktoken-go.
- Métricas via endpoint Prometheus e `ctxgate stats`; log SQLite por chamada.
- Distribuição por `go install` e goreleaser (GitHub Releases).
- Padrão conservador: o benchmark mede taxa de sucesso, não só tokens.

**Decisões do CTO** (aceitas por Vinicius em 2026-10-29; o Product não as reabre; detalhe em `docs/adr/`):

| Tema | Decisão | ADR |
|---|---|---|
| SDK e protocolo | Relay JSON-RPC próprio e opaco (sem reserializar); `go-sdk` v1.8.0 só em testes de conformância (2025-11-25 e 2026-07-28); o relay não negocia versão | ADR-001 |
| Métodos | Só o `result` de `tools/call` (sem erro, sem `structuredContent`, `resultType` completo) entra no pipeline; todo o resto passa byte a byte; `resources/read` e `prompts/get` ficam para depois | ADR-001, 006 |
| Pipeline e falha | `Pipeline.Run` (nunca erra) + `Stage.Apply`; falha, panic, timeout (200 ms) ou invariante violada ⇒ resposta original da chamada; estágios síncronos | ADR-001 |
| Concorrência | 1 sessão e 1 upstream por processo, várias chamadas em voo (16), ordem de entrega não garantida | ADR-001 |
| YAML | `upstreams` é lista com exatamente 1 no MVP; `KnownFields`; chaves do ADR-001 §6; única config por tool: `tools.<nome>.query_args` | ADR-001, 002 |
| Consulta do BM25 | `query_args` por config → heurística nos argumentos → **sem consulta = não corta (só dedup)** | ADR-002 |
| Trecho | Parágrafo; código cercado inteiro; subdivide >512 tokens por linha; não textual passa intacto; ordem original preservada | ADR-006 |
| Dedup | Hash exato + simhash/Jaccard de shingles (0,90); trechos <20 tokens só por hash; **escopo = mesma resposta** | ADR-006 |
| Truncamento | **Sem marcador** no MVP; reavaliar após S5 (`ctxgate_expand`, fase 2) | ADR-006 |
| SQLite | `modernc.org/sqlite`, WAL, escritor único em lote, esquema v1, só metadados por padrão, conteúdo só em modo captura, retenção 30 dias | ADR-003 |
| Tokens | `tiktoken-go/tokenizer` v0.10.0 (troca o `tiktoken-go` citado no PRD), `cl100k_base`, aproximado, uma passada por trecho | ADR-005 |
| Métricas | `client_golang` v1.25.0; endpoint desligado por padrão e em 127.0.0.1; porta ocupada ⇒ degrada; p50/p95 exatos no SQLite | ADR-005 |
| Plataformas | `CGO_ENABLED=0`; linux amd64/arm64, darwin amd64/arm64, windows/amd64 (sem smoke test no Windows) | ADR-003 |
| Benchmark | Replay determinístico; sucesso = retenção de `required_evidence` (proxy); corpus privado em `bench/corpus/` (no `.gitignore`; no repo só agregados + hash); corpus sintético público em `bench/public/`; camada B (agente ao vivo) opcional e manual | ADR-004 |

Padrões iniciais (validados no S5, ajustáveis só por config): `budget_tokens: 6000`, `dedup.similarity: 0.90`, `bm25 k1=1.2 b=0.75`, `pipeline.timeout: 200ms`, `max_in_flight: 16`, `startup_timeout: 30s`, `shutdown_grace: 2s`.

**Regras do Product**
- Chamadas sem consulta (`query_source=none`) não são cortadas; a fração é reportada.
- O sucesso do benchmark é a retenção de `required_evidence` em replay, e é sempre rotulado como **proxy**. Declaração no README: aprovada (ADR-004). A mesma declaração no post: **pendente de ok do Vinicius**.
- Dependências novas dependem do ok do Vinicius. Aprovadas (Vinicius, 2026-10-29): `modernc.org/sqlite`, `tiktoken-go/tokenizer`, `client_golang`, `go.yaml.in/yaml/v3` e `go-sdk` (só testes).
- O FitToday-cms é privado: nada do corpus, nenhum trecho de código nem prompt real vai ao repo; anonimizar o código dele é proibido.

**Pendências abertas com o CTO:** P1 (valor de `tokens_before/after` quando o atalho do ADR-005 §4 pula a contagem), C2 (formato do hash da evidência no repo). C7 fechado: o gate do p95 <20ms vale para respostas ≤100 KB com ≥1000 amostras.

## Testing Decisions

- Um bom teste verifica comportamento externo (o que o agente vê no stdio e o que sai em métricas/SQLite), não detalhes internos.
- Seam principal (o mais alto possível): a fronteira stdio MCP. Teste de integração com um **upstream fake** e um **cliente fake** conversando com o binário/processo do ctxgate. Seams adicionais só onde necessário: a interface de estágio do pipeline (testes tabulares por estágio) e o schema SQLite (leitura direta nos testes). Confirmado no ADR-001 (seam externo: `Pipeline.Run`; seams internos: estágios, testados por tabela).
- Módulos com teste obrigatório: proxy/protocolo, cada estágio do pipeline, store, metrics, CLI (`stats`).
- Concorrência: `go test -race` no CI e testes com chamadas paralelas.
- Desempenho: benchmark Go (`go test -bench`) do overhead do pipeline, com orçamento p95 <20ms (alvo do ADR-005: 100 KB; 1 MB é informativo). Overhead = `total_ns − upstream_ns`.
- Benchmark do produto: replay determinístico (`make bench-e2e`, corpus privado) e `make bench-public` (corpus sintético, roda no CI para tokens, sucesso e determinismo; a latência não é gate no CI).
- Prior art: não há código existente (repositório novo). Referência de design: ai-memory (akitaonrails).

## Out of Scope

Sync entre máquinas (fase 2, agentsync), embeddings e LLM, UI web, multi-usuário e times, transporte HTTP/SSE. Qualquer item fora da seção 5 do PRD fica para depois. Também fora do MVP por decisão das ADRs: marcador de truncamento, dedup entre chamadas ou persistido, `resources/read`/`prompts/get` no pipeline, mais de 1 upstream, agente ao vivo no comando automatizado do benchmark (só camada B manual, opcional).

## Further Notes

- Capacidade total: 6 semanas × 5h = 30h, sem folga (25 tickets). Cada semana soma exatamente 5h; atraso numa semana empurra a seguinte. Ver riscos em `tickets-S1-S6.md`.
- Ajustes de horas da v2: S2-01 1,0→0,75h e S2-04 1,0→1,25h (dentro da S2); S5 reequilibrada para caber o exportador do SQLite, o corpus sintético público e o corpus privado (ver tabela na S5 dos tickets). A captura das 10 tarefas reais é trabalho normal do Vinicius no FitToday-cms e fica fora das 5h; S5-01 cai para 1,0h e a S5 fica com 4,75h (decisões do Vinicius, 2026-10-29).
- Gate de produto (≥10 usuários externos ou pedidos para times) é medido depois do S6 e não gera ticket no MVP.
- Meta de ≥100 stars em 60 dias é pós-lançamento; só o post e o README entram no S6.
- Publicar o post no X exige aprovação explícita do Vinicius; o ticket entrega o rascunho. Publicar a release e fazer push dos docs no repo também.
- Limitações a declarar no README: sucesso é proxy (replay não vê mudança de comportamento do agente); tokens aproximados (cl100k_base); sem consulta não corta; 10 tarefas de um só projeto privado; Windows sem smoke test.
