# ADR-005 — Contagem de tokens e orçamento de latência (p95 <20 ms)

- **Status:** aceita (Vinicius, 2026-10-29) · **Resolve:** questões 10 (parte p50/p95), 11 · `tiktoken-go/tokenizer` v0.10.0 e `prometheus/client_golang` v1.25.0 aprovados

## Contexto
O PRD pede tiktoken-go e overhead p95 <20 ms. Medição de spike (Go 1.27, 8 cores, texto misto pt-BR/código): contar tokens `cl100k_base` custa **≈1,1 ms/10 KB, ≈9,9 ms/100 KB, ≈84 ms/1 MB** em uma passada. Contar "antes" e "depois" ingenuamente (2 passadas) já gasta ~20 ms em 100 KB: estoura a meta sozinho.

## Decisão
1. **Biblioteca:** `github.com/tiktoken-go/tokenizer` v0.10.0 (MIT, 21/10/2026, vocabulário embutido, offline). A alternativa `pkoukk/tiktoken-go` (última versão v0.1.8, set/2025) baixa o BPE em runtime ou exige loader extra: ruim para binário offline. **Encoding padrão `cl100k_base`**, gravado em cada `Record`. Contagem é **aproximada**: Claude/Cursor usam outros tokenizers; o README declara e o benchmark reporta "tokens (cl100k_base, aprox.)". Margem de erro não é prometida; S5 compara economia relativa (%), que é menos sensível ao tokenizer que o valor absoluto.
2. **Uma passada só:** o pipeline conta tokens **por trecho** (ADR-006), uma vez; `tokens_before` = soma dos trechos; `tokens_after` = soma dos trechos mantidos (separadores contados por constante, tolerância documentada). Nenhuma segunda tokenização.
3. **Paralelismo:** contagem em lotes de trechos com até `min(GOMAXPROCS,4)` workers (spike: 40 mil tokens, 12,9 ms → 5,6 ms com 4 workers); cache por hash do trecho dentro da chamada.
4. **Atalhos:** resposta com `bytes ≤ budget_tokens` (tokens ≤ bytes) pula contagem síncrona e rank/budget; **modo passthrough** (`pipeline.enabled=false`) envia a resposta primeiro e conta depois, fora do caminho crítico.
5. **Orçamento de latência (alvo p95, 100 KB):** relay ≤4 ms + contagem paralela ≤4 ms + dedup ≤3 ms + BM25 ≤4 ms + gravação ~0 (canal) ≈ 15 ms. 1 MB é meta informativa (provável >20 ms); `pipeline.timeout` (200 ms) garante fallback.
6. **Como medir:** overhead = `total_ns − upstream_ns`, com relógios monotônicos no relay (agente→resposta entregue menos upstream→relay). `stats` calcula p50/p95 **exatos** no SQLite (nearest-rank: `ORDER BY overhead_ns LIMIT 1 OFFSET (n-1)*0,95`); `/metrics` expõe histograma (buckets 0,5/1/2,5/5/10/15/20/30/50/100/250/500 ms e 1 s). Contadores de `stats` e `/metrics` devem coincidir; percentis do histograma são estimados por bucket.
7. **Métricas:** `github.com/prometheus/client_golang` v1.25.0 (Apache-2.0), registry próprio, sem collectors Go/process por padrão. Nomes: `ctxgate_calls_total{upstream,tool,mode}`, `ctxgate_tokens_before_total`, `ctxgate_tokens_after_total`, `ctxgate_tokens_saved_total`, `ctxgate_overhead_seconds` (histograma), `ctxgate_upstream_seconds`, `ctxgate_stage_errors_total{stage}`, `ctxgate_store_dropped_total`. Endpoint **desligado por padrão** (`metrics.listen: ""`); ligado, bind em `127.0.0.1`. **Porta ocupada ⇒ degrada:** aviso em stderr e o proxy segue.

## Alternativas
Contar com heurística `len/4` (rápido, impreciso: ruim para o argumento de portfólio); contar tudo com `pkoukk` (rede/loader); escrever o formato de texto Prometheus à mão (zero dependência, mas menos "senior" e sem histograma pronto); contagem aproximada por bytes só para o orçamento (plano B se o p95 falhar em S4-03).

## Consequências
(+) Meta de latência viável com margem pequena; contagem única reaproveitada por dedup/rank/budget/log. (−) Modo passthrough tem atraso no registro; separadores contados por constante geram erro de poucos tokens. 

## Riscos
**p95 <20 ms em 100 KB é a meta mais apertada do projeto.** Plano B (ordem): (1) cache + 4 workers, (2) estimar `tokens_before` por bytes e só contar exato os candidatos a corte, (3) elevar o piso de tamanho para engajar o pipeline (respostas <8 KB passam sem pipeline). Se estourar, S4-03 documenta o estágio dominante (pprof) e o PO é avisado, como no ticket.
