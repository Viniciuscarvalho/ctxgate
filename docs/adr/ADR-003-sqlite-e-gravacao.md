# ADR-003 — Driver SQLite, esquema, privacidade e gravação fora do caminho crítico

- **Status:** aceita (Vinicius, 2026-10-29) · **Resolve:** questões 8 e 12 (parte SQLite) · `modernc.org/sqlite` aprovado

## Contexto
O log por chamada alimenta `stats`, métricas e o replay do benchmark. A meta é overhead p95 <20 ms e o binário precisa sair por goreleaser multiplataforma sem infra paga. CGO complicaria o release: compilação cruzada precisaria de toolchain C por alvo (zig/runners por SO).

## Decisão
1. **Driver:** `modernc.org/sqlite` (v1.61.0, 21/10/2026, licença BSD-3, Go puro, `CGO_ENABLED=0`). Verificado em spike: o build cruza para `darwin/arm64`, `windows/amd64` e `linux/arm64` sem C.
2. **Modo:** WAL, `synchronous=NORMAL`, `busy_timeout=5000`, `foreign_keys=ON`. Um único escritor (`SetMaxOpenConns(1)` no pool de escrita); `stats` abre outra conexão `mode=ro` (WAL permite leitor concorrente).
3. **Fora do caminho crítico:** `store.Observer` só faz `select { case ch <- rec: default: dropped++ }` (canal limitado, 1024). Uma goroutine escreve em **lotes** (100 linhas ou 200 ms numa transação). Nunca bloqueia, nunca falha para o relay; descarte incrementa `ctxgate_store_dropped_total` e loga em stderr. `Close(ctx)` drena com prazo de 2 s (SIGINT ⇒ banco íntegro, `PRAGMA integrity_check=ok`).
4. **Esquema v1** (`PRAGMA user_version=1`; todas as colunas `NOT NULL`, atende "sem NULL"):
```sql
CREATE TABLE calls(
  id INTEGER PRIMARY KEY, ts_unix_ms INTEGER NOT NULL, session_id TEXT NOT NULL,
  upstream TEXT NOT NULL, tool TEXT NOT NULL, args_hash TEXT NOT NULL,
  query_source TEXT NOT NULL, mode TEXT NOT NULL, skip_reason TEXT NOT NULL DEFAULT '',
  encoding TEXT NOT NULL, tokens_before INTEGER NOT NULL, tokens_after INTEGER NOT NULL,
  bytes_before INTEGER NOT NULL, bytes_after INTEGER NOT NULL,
  upstream_ns INTEGER NOT NULL, overhead_ns INTEGER NOT NULL, total_ns INTEGER NOT NULL,
  is_error INTEGER NOT NULL, stages_json TEXT NOT NULL, err TEXT NOT NULL DEFAULT '',
  config_hash TEXT NOT NULL);
CREATE INDEX calls_ts ON calls(ts_unix_ms);
CREATE TABLE payloads(call_id INTEGER PRIMARY KEY REFERENCES calls(id) ON DELETE CASCADE,
  request BLOB NOT NULL, response BLOB NOT NULL);  -- gzip (compress/gzip), só em modo captura
```
5. **Privacidade:** por padrão **só metadados** (hash dos argumentos, contagens, tempos). Conteúdo bruto (`payloads`) só com `store.capture_content: true` (modo captura, usado para montar o corpus do benchmark; ADR-004). Diretório `~/.ctxgate/` com 0700 e arquivo 0600; `*.db*` no `.gitignore`. `session_id` = 8 bytes aleatórios em hex por `serve`.
6. **Retenção:** `retention_days` (padrão 30), `DELETE` na inicialização; sem rotação além disso.

## Medição (spike, máquina de desenvolvimento, 2000 inserts)
Insert síncrono em WAL/NORMAL: p50 12 µs, p95 18 µs, p99 31 µs. Ou seja, no caso comum a gravação síncrona já caberia no orçamento; o canal assíncrono existe para o **pior caso** (checkpoint WAL, disco lento/NFS, lock de leitor do `stats`), que é o que quebraria o p95 e o requisito "falha ao gravar não derruba a chamada". (A variante assíncrona não foi medida no spike por bug do script; o custo do lado do relay é um envio em canal.)

## Alternativas
1. **`mattn/go-sqlite3` (CGO):** mais rápido e batido, mas quebra `go install` sem compilador C e complica goreleaser. Rejeitada.
2. **`ncruces/go-sqlite3` (WASM, sem CGO):** viável, menos adoção/avaliação aqui. Plano B se o modernc der problema.
3. **Gravação síncrona:** simples, mas expõe o p95 ao disco. Rejeitada.
4. **Arquivo JSONL em vez de SQLite:** contradiz o PRD (SQLite) e dificulta `stats`. Rejeitada.

## Consequências
(+) Release de um `CGO_ENABLED=0` por plataforma; relay isolado do disco. (−) Binário maior (modernc), mais lento que CGO (não medido; irrelevante com escrita em lote); contagem em `/metrics` pode ficar à frente do SQLite até o flush (testes chamam `Flush`).

## Riscos
Descarte sob pressão (só ocorre se >1024 pendentes) quebraria "contagem == linhas": o teste S4-04 (500 chamadas, 20 paralelas) valida. Dependência grande: transitivas do modernc entram no `go.sum`.
