# ADR-004 — Benchmark: replay do SQLite vs. agente ao vivo

- **Status:** aceita (Vinicius, 2026-10-29; sucesso = retenção de evidência em replay, declarado como proxy no README) · **Resolve:** questão 9 · FitToday-cms é privado (Vinicius, 2026-10-29)

## Contexto
Meta do PRD: ≥30% menos tokens **sem perder a tarefa**, 10 tarefas reais (era 20; ver Adendo 3), reproduzível por terceiros, 5h na S5 e zero custo. Agente ao vivo mede sucesso real, mas é não determinístico, gasta tokens/tempo de Vinicius e não é reproduzível por outros. Replay é determinístico e grátis, mas só mede tokens e latência, a menos que se defina um critério de sucesso que ele consiga verificar.

## Decisão
Benchmark em **duas camadas**, a primeira obrigatória:
1. **Camada A (obrigatória, `make bench-e2e` → `go run ./bench`): replay determinístico.**
   - *Captura (uma vez):* Vinicius trabalha nas 10 tarefas com o ctxgate em passthrough e `store.capture_content: true`; as respostas **do upstream, antes do pipeline** ficam na tabela `payloads` (ADR-003) e são exportadas para `bench/corpus/*.jsonl.gz`.
   - *Execução:* o harness reaplica `pipeline.Run` offline, por tarefa, nas configs `passthrough` e `ctxgate`, e também roda o relay completo contra um upstream fake que serve as respostas gravadas (para medir overhead p50/p95 real). Seed e versões fixas ⇒ variância 0 em tokens.
   - *Sucesso da tarefa (proxy honesto):* cada tarefa tem `required_evidence` (lista de substrings/regex que **precisam estar** no contexto entregue, definida por Vinicius na S5-01 a partir do que a solução realmente usou). `sucesso = 100% das evidências retidas`. O relatório rotula "sucesso (proxy: retenção de evidência)", não "tarefa concluída".
2. **Camada B (opcional, se sobrar tempo no S5-03): agente ao vivo em amostra.** 5 tarefas × 2 configs × 3 execuções com o Claude Code/Cursor que Vinicius já usa (sem custo novo), critério de sucesso do S5-01, **manual**, reportado com variância como apêndice. Nunca entra no comando automatizado.

## Alternativas
1. **Só agente ao vivo (10×2×N):** caro, não reproduzível, estoura 5h. Rejeitada como camada principal.
2. **Só replay medindo tokens:** não diz se cortar piorou a tarefa; contradiz o PRD ("mede a taxa de sucesso"). Rejeitada.
3. **LLM como juiz:** custo e não determinismo. Rejeitada (restrição zero LLM/custo).

## Consequências
(+) 1 comando, determinístico, 0 custo, mede o risco real (perder evidência). (+) Reaproveita `Pipeline.Run` offline: testa o seam central. (−) Evidência retida ≠ agente acerta; o README precisa dizer isso e mostrar a camada B quando existir. (−) Exige capturar o corpus antes do S5-02 (feito dentro do S5-01).

## Riscos
- **Privacidade/reprodutibilidade:** resolvido. O FitToday-cms é privado; ver os Adendos 1 e 2 (corpus privado local + corpus sintético público obrigatório em `bench/public/`).
- Tarefas sem consulta (`query_source=none`) não são cortadas (ADR-002): reportar fração.

## Adendo (2026-10-29): corpus privado
O FitToday-cms é privado, porque vai virar produto vendido. Os replays gravados e qualquer trecho de código dele ficam fora do repo do ctxgate: ficam locais, em `bench/corpus/`, que entra no `.gitignore`. O repo versiona o harness, o schema das tarefas (`required_evidence` escrito sem colar código), os resultados agregados (tokens, %, p50/p95, retenção) e o hash do corpus de cada rodada, para provar que todas usaram o mesmo corpus. Quem quiser reproduzir roda o harness no próprio repositório. **Reprodutibilidade pública (obrigatória):** o repo versiona também um corpus sintético em `bench/public/`, com um repo Go fictício e as mesmas 10 formas de tarefa, gerado por script com seed fixa. Qualquer pessoa roda `make bench-public` e chega aos mesmos números. O README publica as duas tabelas: a privada (FitToday-cms, só agregados e hash) e a pública (reproduzível). Fica proibido anonimizar o código do FitToday-cms, porque o risco de vazar é alto e o ganho é baixo.

## Adendo 2 (2026-10-29): decisões para o plano do QA
- As `required_evidence` literais ficam só no corpus privado. O repo guarda a descrição mais um HMAC-SHA256 calculado com uma chave local, que nunca entra no repo.
- O gate de latência é p95 <20 ms nas respostas de até 100 KB, e o recorte por faixa de tamanho também é reportado. Acima de 100 KB, a latência é só reportada, sem gate.
- A config é congelada antes da rodada final, e o hash dela entra no resultado.
- A chave HMAC fica em `~/.ctxgate/bench-hmac.key`, com modo 0600, e pode ser sobrescrita por `CTXGATE_BENCH_HMAC_KEY`. Se a chave não existir, o harness falha com erro claro em vez de gerar uma sozinho. A geração é explícita (`ctxgate bench keygen`), e esse caminho entra no `.gitignore` por precaução.

## Adendo 3 (2026-10-29): 10 tarefas — pendente de confirmação do Vinicius no grupo
O Vinicius aprovou cortar o corpus privado para 10 tarefas reais (repassado pelo Product). O gate passa a ser 10/10, e o corpus sintético público usa as mesmas 10 formas de tarefa. Com menos tarefas, a redução de tokens é reportada também por tarefa, e não só no agregado, para que uma tarefa sozinha não distorça o resultado.
