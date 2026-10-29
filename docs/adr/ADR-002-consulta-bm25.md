# ADR-002 — Origem da consulta BM25 (desbloqueia CTX-S3-01)

- **Status:** aceita (Vinicius, 2026-10-29) · **Resolve:** questão 3

## Contexto
O proxy MCP não vê a conversa do agente nem o prompt do usuário: só o `tools/call` (nome + argumentos) e a resposta. BM25 precisa de uma consulta. Servidores de memória/contexto costumam ter ferramentas do tipo `search(query)`, `recall(topic)`, `get_context(task)`; as que não têm argumento textual (ex.: `list_recent`) não oferecem sinal de relevância.

## Decisão
Um `QueryResolver` em 3 camadas, na ordem:
1. **Config explícita** por ferramenta: `tools.<nome>.query_args: [query, q]` → concatena os valores string desses argumentos (fonte `config`).
2. **Heurística automática** (padrão): concatena valores string dos argumentos de primeiro nível (e aninhados até profundidade 2), ignorando chaves que casam `(?i)(^|_)(id|ids|path|uri|url|cursor|limit|offset|format|lang)$` e valores <3 caracteres; teto de 512 caracteres, termos únicos (fonte `auto`).
3. **Sem consulta** (resolver devolve vazio, fonte `none`): **os estágios `rank` e `budget` não cortam nada**; só o dedup (exato/similar) atua. A chamada é registrada com `query_source=none`.

Sem LLM, sem embeddings, sem ler o prompt do agente. O corpus do BM25 é o conjunto de trechos da própria resposta (IDF calculado ali, variante Lucene `ln(1+(N-n+0,5)/(n+0,5))`, sempre ≥0); `k1=1,2`, `b=0,75`; tokenização por `unicode.IsLetter/IsDigit`, minúsculas, sem stopwords.

## Alternativas
1. **Última mensagem do agente:** o MCP não a expõe; exigiria hook/plugin por agente. Fora do MVP (futuro: hook opcional do Claude Code gravando um arquivo que o ctxgate lê).
2. **Ranquear "por centralidade" na resposta quando não há consulta** (sugestão de fallback do PO): sem sinal de relevância, vira corte arbitrário e arrisca a tarefa. Rejeitada para o MVP; o benchmark informa a fração de chamadas sem consulta.
3. **Parâmetro de orçamento sem ranking (cortar a cauda):** mesmo problema. Rejeitada.

## Consequências
- (+) Desbloqueia S3-01 hoje; comportamento previsível e conservador (sem consulta ⇒ não corta).
- (+) `query_source` por chamada em SQLite/métricas mostra quanto do tráfego é elegível ao corte.
- (−) Economia só nas chamadas com consulta; se for pequena, a meta ≥30% falha (resultado honesto, regra do S5-03).

## Riscos
- Heurística pode incluir argumento irrelevante e distorcer o ranking → mitigação: override por config e lista de exclusão; revisar no S5 as tools reais do FitToday-cms.
- Consultas curtas/ambíguas dão BM25 fraco → o orçamento padrão só corta respostas grandes (ver ADR-005/decisoes-cto).
