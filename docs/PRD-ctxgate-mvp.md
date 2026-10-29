# PRD — ctxgate MVP

## 1. Contexto
Projeto de portfólio Senior/Staff backend (Go), inspirado no ai-memory (akitaonrails). Dev solo, até 5h/semana. Vira produto só se tiver tração.

## 2. Problema
Agentes de código (Claude Code, Codex, Cursor) recebem contexto demais ou repetido das fontes de memória e MCP: gasta tokens, polui o prompt e ninguém mede isso.

## 3. Solução
Proxy MCP local em Go que fica entre o agente e os servidores MCP de memória e contexto. Ele:
1. Deduplica trechos repetidos (hash e similaridade simples).
2. Ranqueia por relevância (BM25 local, sem LLM) e corta por um orçamento de tokens.
3. Mede tokens antes/depois e a latência, e expõe métricas.

## 4. Usuário
Dev solo usando vários agentes em várias máquinas. Primeiro usuário: o próprio Vinicius, no FitToday-cms.

## 5. Escopo do MVP (in)
- `ctxgate serve`: proxy MCP via stdio, configurado por YAML apontando os servidores upstream.
- Dedup + ranking BM25 + orçamento de tokens por chamada.
- Contagem de tokens com tokenizer aproximado (tiktoken-go).
- Métricas: endpoint Prometheus e `ctxgate stats` (tokens economizados, % e latência p50/p95).
- Log local em SQLite de cada chamada, para replay e benchmark.
- Instalação: `go install` e binários no GitHub Releases (goreleaser).

## 6. Fora do MVP
Sync entre máquinas (fase 2, ideia agentsync), embeddings e LLM, UI web, multi-usuário e times, transporte HTTP/SSE.

## 7. Métricas de sucesso
- Técnicas: ≥30% menos tokens sem perder a tarefa (benchmark com 10 tarefas reais, sucesso 10/10 e redução também reportada por tarefa; corte de 20 para 10 aprovado pelo Vinicius em 2026-10-29); overhead p95 <20ms.
- Portfólio: README com benchmark reproduzível, post no X com números, ≥100 stars em 60 dias.
- Gate de produto: ≥10 usuários externos ativos ou pedidos de recursos para times.

## 8. Arquitetura
Agente → (stdio MCP) → ctxgate [router → pipeline: dedup → rank → budget → métricas] → servidores MCP upstream.
Pacotes: `mcp/` (protocolo), `pipeline/` (estágios plugáveis via interface), `store/` (SQLite), `metrics/`.
O que mostra de sênior: protocolo e proxy, design de pipeline extensível, observabilidade, benchmark rigoroso e testes de concorrência.

## 9. Plano (5h/semana, ~6 semanas)
- S1: proxy MCP passthrough funcionando com 1 upstream e testes.
- S2: dedup + contagem de tokens + log SQLite.
- S3: ranking BM25 + orçamento.
- S4: métricas, `stats` e p95.
- S5: benchmark reproduzível no FitToday-cms.
- S6: README, release com goreleaser e post no X.

## 10. Riscos
- Cortar o contexto pode piorar a tarefa → o benchmark mede a taxa de sucesso, não só os tokens; ranking conservador por padrão.
- Mudanças na especificação do MCP → acompanhar o SDK oficial de Go.
- Tempo curto → escopo travado; tudo o que não está na seção 5 fica para depois.

## 11. Custo
Zero de infraestrutura (tudo local) e zero de LLM. Os únicos custos são o tempo do Vinicius e os tokens dos agentes de código usados durante o desenvolvimento.
