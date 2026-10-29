# Plano do benchmark do ctxgate (S5)

- **Autor:** ctxgate · QA · **Data:** 2026-10-29 · **Status:** rev. 3 (2026-10-29), corte para 10 tarefas e gate 10/10 confirmado pelo Vinicius no grupo (ADR-004 Adendo 3); antes rev. 2, com as decisões do CTO (C2, C6, C7, C10) incorporadas; Product aceitou C1, C3, C4, C5, C8 e C10 e está atualizando S5-01, S5-03 e S6-01 · **Pendente:** aprovação do Vinicius
- **Base:** PRD §7, `spec-mvp.md` (histórias 20–21), `tickets-S1-S6.md` (S5-01..03), ADR-001, 002, 003, 004 (aceita, com adendo de corpus privado e corpus público obrigatório), 005 (**aceita**), 006.
- **Regra deste documento:** nada aqui descreve conteúdo real do FitToday-cms (privado). Onde falta dado real, está marcado **[a preencher com gravação real]**. Flags de CLI e nomes de arquivo do harness são **propostas** até o S5-02 existir.

---

## 1. Objetivo e metas

Provar, de forma reproduzível, que o ctxgate reduz o contexto entregue ao agente sem perder o que a tarefa precisa e sem custo perceptível de latência.

| # | Meta | Fonte | Aprovado | Reprovado |
|---|------|-------|----------|-----------|
| M1 | Redução de tokens | PRD §7 | `1 − Σtokens_after / Σtokens_before ≥ 30%`, somando as 10 tarefas (config `ctxgate` vs `passthrough`) | `< 30%` (resultado honesto é publicado; S5-03) |
| M2 | Sucesso da tarefa (**proxy: retenção de `required_evidence`**) | PRD §7, ADR-004 | **10/10** tarefas com 100% das evidências retidas no contexto entregue pelo ctxgate | ≥1 tarefa com evidência perdida. A tarefa entra na lista de regressões |
| M3 | Overhead p95 (**gate: respostas de até 100 KB**) | PRD §7, ADR-005, decisão do CTO (C7) | p95 de `total_ns − upstream_ns` **< 20 ms** na config `ctxgate`, sobre as chamadas com resposta ≤100 KB agrupadas, com ≥1000 amostras nesse recorte | `≥ 20 ms` ou menos de 1000 amostras no recorte ≤100 KB (⇒ "não medido", não "aprovado"). **Acima de 100 KB só se reporta; não é gate** |
| M4 | Determinismo | S5-02 | Duas execuções seguidas geram `report.json` idêntico nos campos de tokens, sucesso e hash do corpus (variância 0) | Qualquer diferença |
| M5 | Validade do baseline | QA | `passthrough` = 10/10 evidências e `tokens_after == tokens_before` em toda chamada, texto byte a byte igual ao gravado | Baseline que perde evidência ⇒ a tarefa/evidência é inválida e é corrigida **antes** de rodar o ctxgate |

Veredito final = M1 ∧ M2 ∧ M3 ∧ M4 ∧ M5. A faixa >100 KB entra no relatório com n, p50 e p95, mas **não** entra no veredito; ter poucas amostras nela é um dado do corpus, não uma exceção nem uma falha. Se M1 ou M3 falharem, o relatório diz isso na primeira linha (S5-03). Não há "aprovado com ressalva" para M2.

**Rótulos obrigatórios nos relatórios:** "tokens (cl100k_base, aproximado)" e "sucesso (proxy: retenção de evidência)", nunca "tarefa concluída".

---

## 2. Metodologia de replay

### 2.1 Fluxo em três fases

1. **Captura (uma vez, local, privada).** Vinicius executa as 10 tarefas reais com o ctxgate em `pipeline.enabled=false` e `store.capture_content: true`. As respostas **do upstream, antes do pipeline** ficam em `payloads` (ADR-003) num SQLite local. Elas são exportadas para `bench/corpus/*.jsonl.gz` (diretório no `.gitignore`).
2. **Congelamento.** Antes da primeira execução com o ctxgate: (a) o corpus é hasheado (§2.4); (b) as `required_evidence` de cada tarefa são commitadas em forma de HMAC (§3.3); (c) a **config de benchmark é congelada** (commitada, com `config_hash`) antes da rodada final. Isso impede ajustar a evidência ou a config depois de ver o resultado.
3. **Replay (repetível, offline, sem custo).** `go run ./bench` roda:
   - **Camada A1 (tokens e sucesso):** `pipeline.Run` offline, por chamada gravada, nas configs `passthrough` e `ctxgate`.
   - **Camada A2 (latência):** o relay completo (`internal/mcp`) com um cliente fake que reenvia os `tools/call` gravados e um **upstream fake** que serve as respostas gravadas. É o único jeito de medir o overhead real do relay (framing, envelope, goroutine, escrita).
   - **Checagem cruzada A1×A2:** o texto entregue pelo relay tem que ser **idêntico** ao `Outcome.Texts` do `pipeline.Run` offline. Divergência é falha do harness.

### 2.2 Baseline vs. ctxgate

| Config | `pipeline.enabled` | Esperado |
|--------|--------------------|----------|
| `passthrough` (baseline) | false | Bytes entregues = bytes gravados; `tokens_after == tokens_before` |
| `ctxgate` | true, com os padrões fixados no commit (`budget_tokens: 6000`, `dedup.similarity: 0.90`, `k1=1.2`, `b=0.75`, `pipeline.timeout: 200ms`) | Cortes só onde há consulta e a resposta passa do orçamento; dedup sempre |

As duas configs leem **o mesmo corpus, na mesma ordem de tarefas e chamadas**. Só `pipeline.enabled` e os parâmetros do pipeline mudam.

### 2.3 Seeds e determinismo

- O pipeline é determinístico por construção (ADR-006: BM25 com desempate estável; simhash com função fixa). Nenhum estágio usa aleatoriedade.
- `--seed` (padrão `42`) controla só (a) a ordem de execução das tarefas na fase de latência (embaralhada para não viciar cache) e (b) o bootstrap do intervalo de confiança do M1. Tokens e sucesso **não dependem** do seed.
- Versões fixas e gravadas no relatório: Go (`go version`), `go.sum` (hash), `tiktoken-go/tokenizer` v0.10.0 com `cl100k_base` **desde a primeira rodada** (ADR-005, aceita), commit do ctxgate (`git rev-parse HEAD`, e `dirty` sim/não), e **`config_hash`** (mesmo campo de `calls`, ADR-003), que **entra no resultado** (`report.json` e `report.md`) para provar qual config produziu os números.
- Relatório com `dirty=true`, ou cujo `config_hash` difira do congelado, é marcado "não publicável".
- O arquivo `report.json` separa `deterministic{}` (tokens, sucesso, hashes) de `timing{}` (latência, data). M4 compara só o primeiro bloco.

### 2.4 Hash do corpus

- Por arquivo: `sha256` do conteúdo **descomprimido** (gzip não é estável entre versões do Go).
- Manifesto: linhas `"<nome-do-arquivo> <sha256>\n"` ordenadas por nome; **hash do corpus = sha256(manifesto)**.
- O harness recalcula o hash ao iniciar e **aborta** se ele diferir do `corpus.sha256` informado (ou imprime o hash, na primeira rodada).
- O repo versiona: o hash do corpus de cada rodada, em `bench/results/<data>-<commit>/report.json`, e o manifesto **sem** nomes que revelem o projeto (nomes opacos `T01.jsonl.gz`…).
- Mesma saída em duas máquinas ⇒ mesmo corpus. Isso prova que "as rodadas usaram o mesmo corpus" sem expô-lo.

---

## 3. Formato da tarefa

O repo é público e o FitToday-cms é privado. Por isso cada tarefa tem **duas metades**.

### 3.1 Metade pública (versionada): `bench/tasks/T01.yaml` … `T10.yaml`

```yaml
id: T01                     # T01..T10, estável
category: C1-busca-consulta-grande   # ver §4
prompt: >                   # descrição GENÉRICA do objetivo, escrita à mão; não é código anonimizado (ADR-004 proíbe anonimizar código do FitToday-cms)
  Descrição genérica do que o agente tinha que descobrir.
upstream: <nome-do-upstream-mcp>     # ex.: tipo do servidor de memória/contexto, sem endpoint privado
recorded_calls:             # só metadados; o conteúdo está no corpus privado
  - seq: 1
    tool: <nome-da-ferramenta>
    args_hash: <sha256>     # igual ao args_hash de `calls` (ADR-003)
    query_source: config|auto|none   # esperado (ADR-002)
    bytes: 0                # [a preencher com gravação real]
    tokens_baseline: 0      # [a preencher com gravação real]
    corpus_ref: T01.jsonl.gz#1
required_evidence:          # ver §3.3
  - id: E1
    kind: substring|regex
    description: "Descrição abstrata do fato exigido (sem código nem nomes reais)"
    call_seq: 1             # em qual resposta deve aparecer (opcional: omitido = qualquer chamada da tarefa)
    pattern_hmac: <hex>     # HMAC-SHA256(chave local fora do repo, padrão literal)
```

### 3.2 Metade privada (**não versionada**): `bench/corpus/`

- `T01.jsonl.gz`: uma linha por chamada gravada: `{task_id, seq, upstream, tool, args, content:[{type,text}], is_error}`. `content` é a resposta do upstream **antes** do pipeline.
- `evidence/T01.yaml`: os padrões literais (substring/regex RE2) de cada `required_evidence`. **Os padrões literais ficam só aqui**, nunca no repo.
- **Chave do HMAC:** arquivo local fora do repo (proposta: `~/.ctxgate/bench-hmac.key`, modo 0600, ou variável `CTXGATE_BENCH_HMAC_KEY`). Nunca em `bench/` versionado. Fazer backup: sem a chave não dá para reverificar o congelamento.
- `prompts/T01.md`: o prompt real, se existir.
- `capture.db`: o SQLite bruto da captura.
- `denylist.txt`: termos que nunca podem aparecer em arquivo versionado (nomes de arquivo, símbolos, regras de negócio, segredos).

### 3.3 `required_evidence`: regras

1. Definida por Vinicius no S5-01 a partir do que a solução realmente usou, **antes** de qualquer rodada com ctxgate.
2. Cada tarefa tem de 1 a 4 evidências. **Todas** têm de estar presentes (`sucesso = 100%`, ADR-004).
3. Casamento: substring exata ou regex RE2 sobre o texto entregue (`Outcome.Texts` unidos por `\n`), sem normalização. Escopo: a resposta da `call_seq` indicada.
4. **Evidência fraca é rejeitada:** padrão com menos de 12 caracteres, ou que também casa em mais de 1 tarefa, ou que casa em texto "de enchimento" (cabeçalho, boilerplate). Revisão do QA antes do congelamento.
5. **Validade (M5):** a evidência tem de casar no baseline. Se não casar, é erro de definição da evidência.
6. **Anti-vazamento (decisão do CTO, C2):** os padrões literais ficam só no corpus privado local. No repo vão só `description` e `pattern_hmac` = **HMAC-SHA256** com **chave local fora do repo**. A chave evita ataque de dicionário sobre padrões curtos. O harness recalcula o HMAC do padrão privado com a chave local e exige que bata com o público: isso prova que a evidência não mudou depois do congelamento. Quem não tem a chave não consegue verificar o HMAC, só conferir que o hash do corpus e o `config_hash` batem.
7. **Corpus público (`bench/public/`):** as evidências do corpus sintético são **em claro** no repo (o código é fictício), e o harness as usa sem HMAC.

> C2 (§10): resolvido pelo CTO. A ADR diz "escrito sem colar código"; a verificação automática usa o padrão literal só no corpus local.

---

## 4. Esboço das 10 tarefas por categoria

> **rev. 3:** a tabela abaixo ainda tem a distribuição de 20; a redistribuição para 10 (T01..T10) é feita no S5-01, cobrindo todas as categorias C1..C8.

**Todo o conteúdo abaixo é genérico e marcado como a preencher.** Este esboço vale para o corpus **privado**. O corpus **público** (`bench/public/`, ADR-004) tem as mesmas 10 formas de tarefa sobre um repo Go fictício, gerado por script com seed fixa; seu conteúdo é sintético e totalmente versionado. Nenhum prompt, resposta, nome de arquivo ou regra do FitToday-cms é inventado aqui. A distribuição existe para cobrir os caminhos do pipeline, incluindo os que **não** cortam, para o relatório mostrar a fração honesta.

| Cat. | Descrição genérica | Qtd | IDs | Caminho do pipeline exercitado |
|------|--------------------|-----|-----|-------------------------------|
| C1 | Busca com consulta textual, resposta grande, fato relevante em 1 trecho | 5 | T01–T05 | BM25 + orçamento (corte esperado) |
| C2 | Busca com consulta, fatos relevantes **espalhados** (2–4 evidências) | 3 | T06–T08 | BM25 + orçamento; risco de perder evidência secundária |
| C3 | Resposta com trechos repetidos ou quase repetidos | 3 | T09–T11 | Dedup exato e quase-duplicado (0,90) |
| C4 | Recuperação pontual de um dado numa lista longa | 2 | T12–T13 | BM25 com consulta curta/ambígua (ADR-002, risco) |
| C5 | Ferramenta sem argumento textual (ex.: listagem "recentes") | 2 | T14–T15 | `query_source=none`: **não corta**, só dedup |
| C6 | Resposta abaixo do orçamento | 2 | T16–T17 | Passa intacta (atalho ADR-005 §4) |
| C7 | Blocos de código cercados ou linha única gigante (JSON minificado) | 2 | T18–T19 | Trecho indivisível (ADR-006) |
| C8 | Tarefa com várias chamadas encadeadas (3+) misturando categorias | 1 | T10 | Fluxo multi-chamada, soma por tarefa |

Cada linha: **[a preencher com gravação real]** — prompt, upstream, `recorded_calls`, bytes/tokens baseline e `required_evidence`.

Regras de composição:

- Tarefas **reais**, tiradas do trabalho que Vinicius já faria no FitToday-cms (S5-01). Se uma categoria não aparecer no trabalho real, **não se fabrica tarefa**: reporta-se a categoria como vazia e o PO decide.
- A distribuição pode mudar com a realidade da gravação, mas o relatório tem de mostrar a fração de chamadas por `query_source` e por faixa de tamanho (S5-03, ADR-004).
- Nenhuma tarefa com segredo ou dado pessoal (checklist de S5-01, revisão manual de Vinicius + `denylist.txt`).

---

## 5. Medição de tokens

- **Biblioteca:** `github.com/tiktoken-go/tokenizer` v0.10.0, `cl100k_base` (ADR-005). O PRD cita "tiktoken-go"; a ADR-005 foi aceita e fixa essa biblioteca (C6 resolvido).
- **O que se conta:** só blocos `content[].type=="text"`. Blocos não textuais: contar **bytes** em coluna separada, sem tokens.
- **Por chamada:** `tokens_before` e `tokens_after` (campos de `Report`/`calls`), `bytes_before/after`, `mode` (`applied|passthrough|fallback`), `query_source`, `skip_reason`.
- **Por tarefa:** `Σ tokens_before`, `Σ tokens_after`, `% = 1 − after/before`.
- **Agregado (métrica da meta M1):** `reducao_total = 1 − (Σ_tarefas tokens_after) / (Σ_tarefas tokens_before)`. Peso por token, não por tarefa.
- **Por tarefa (obrigatório, ADR-004 Adendo 3):** redução de cada tarefa no relatório, e o agregado é apresentado junto com ela, porque com 10 tarefas uma só pode distorcer o total.
- **Secundárias (informativas):** média das reduções por tarefa (macro); mediana; tarefas com redução 0%; redução só nas chamadas elegíveis (`query_source≠none` e acima do orçamento); intervalo de confiança de 95% da `reducao_total` por bootstrap sobre tarefas (10 000 reamostragens, `--seed`).
- **Sanidade:** `tokens_after ≤ tokens_before` em toda chamada (invariante do pipeline); `mode=fallback` conta e é listado. Fallback conta como 0% de redução naquela chamada.
- **Aviso fixo no relatório:** tokens são aproximados (cl100k_base ≠ tokenizer do Claude/Cursor); vale a redução relativa.

---

## 6. Medição de overhead

- **Definição (ADR-005 §6):** `overhead = total_ns − upstream_ns`, com relógio monotônico no relay. É o tempo que o ctxgate adiciona, não o tempo do upstream (que no replay é um fake instantâneo).
- **Cenário:** fase A2, relay completo, upstream fake em memória (`io.Pipe`) e também como subprocesso (re-exec do binário de teste, S1-03). **A meta M3 usa o subprocesso**, que é o caminho real de produção. Store SQLite **ligado** (`capture_content=false`, arquivo em disco local temporário), como em uso real.
- **Warmup:** 1 passagem completa descartada, mais pelo menos 20 chamadas por processo. Não entram no percentil.
- **Amostras (recorte do gate: respostas ≤100 KB):** cada chamada gravada é reenviada `R` vezes (padrão `R=30`), em 5 execuções de processo. Meta de **≥1000 amostras** no total, e se as 10 tarefas tiverem poucas chamadas o harness aumenta `R` automaticamente. Abaixo de 1000 amostras no recorte ≤100 KB, o p95 sai como "indicativo" e M3 não é declarado aprovado.
- **Estatística:** p50 e p95 pelo método **nearest-rank**, igual ao `stats` (ADR-005): `valor[ceil(0,95·n)−1]` na lista ordenada. Também p99 e máximo (informativos). Calculado sobre o conjunto agrupado das chamadas ≤100 KB nas 5 execuções (**o gate**), e **por execução** (reporta o pior p95 e a dispersão).
- **Faixas de tamanho:** ≤10 KB e 10–100 KB (compõem o gate, cada uma com n, p50 e p95) e **>100 KB (só reportada, não é gate)**. Se houver poucas ou nenhuma amostra >100 KB, o relatório diz "n = X" e segue: isso descreve o corpus, não é exceção. C7 resolvido pelo CTO.
- **Baseline de overhead:** a mesma medição em `passthrough` dá o custo do relay sozinho. O custo do pipeline = diferença entre as duas.
- **Máquina documentada** (em `report.json.machine` e no README): modelo da CPU, nº de núcleos físicos/lógicos, RAM, SO e kernel, versão do Go, `GOMAXPROCS`, governor/modo de energia, tipo de disco, e o resultado de `uptime` (carga) antes e depois. Sem outros processos pesados. Notebook na bateria não vale.
- **CI:** o p95 **não é gate** em runner compartilhado (ruído). O CI roda só o corpus público (`bench/public/`, `make bench-public`) para tokens, sucesso e determinismo. A latência oficial vem da máquina documentada.
- **Pass/fail:** M3 (§1). Se estourar, o relatório traz o estágio dominante (pprof), como pede S4-03.

---

## 7. Taxa de sucesso

- **Definição:** tarefa tem sucesso se **100%** das suas `required_evidence` casam no texto entregue naquela config (ADR-004).
- **Regra de aprovação:** ctxgate **10/10**. Equivale ao critério do S5-03 ("sucesso do ctxgate ≥ baseline"), porque o baseline é 10/10 por construção (M5).
- **Se o PO ou o CTO quiserem um limite menor que 10/10**, isso é mudança de meta: precisa de aprovação do Vinicius e fica registrada. O ADR-004 não define outro limite, então vale 10/10.
- **Relatório por tarefa:** `sucesso_baseline`, `sucesso_ctxgate`, evidências retidas/total, e para cada evidência perdida: `id`, a chamada, `mode`, `query_source` e o motivo do corte (`dup:exact|dup:near|budget`). Não imprime o conteúdo, só os ids.
- **Regressão:** lista de tarefas com evidência perdida, causa provável e recomendação (S5-03). Falha de uma tarefa não aborta as demais (S5-02).
- **Ajuste de config (S5-03):** só dentro do que existe (orçamento, similaridade). Cada tentativa fica registrada no relatório (nº de configs testadas, valores e resultado), porque ajustar nas mesmas 10 tarefas é sobreajuste (C8). A config final é **congelada antes da rodada final** (commit + `config_hash`), e o `config_hash` entra no resultado (§2.3). Mudar a config depois exige nova rodada e novo congelamento. Aceito pelo Product (C8); o S5-03 está sendo atualizado.
- **Camada B (opcional, manual, apêndice):** 5 tarefas × 2 configs × 3 execuções com o agente que Vinicius já usa, critério binário do S5-01, reportado com variância. Nunca entra no comando automatizado nem nas metas M1–M5.

---

## 8. Como reproduzir

> Os comandos abaixo são a **proposta** de interface do S5-02. `make bench-e2e` já está decidido (ADR-004); os flags são sugestão.

**Pré-requisitos:** Go ≥ 1.25, `CGO_ENABLED=0`, repo no commit do relatório, corpus em `bench/corpus/` (privado) ou outro corpus no mesmo formato.

```bash
# 0. Ambiente e integridade
go version && git rev-parse HEAD && git status --porcelain      # tem de estar limpo
git ls-files bench/corpus | wc -l                                # tem de ser 0 (corpus não versionado)

# 1. (Uma vez, local) Capturar: ctxgate em passthrough com captura de conteúdo
#    config: pipeline.enabled=false, store.capture_content=true, store.path=bench/corpus/capture.db
ctxgate serve --config bench/capture.yaml                        # trabalhar as 10 tarefas no agente

# 2. (Uma vez) Exportar o corpus (exportador SQLite→jsonl.gz: ticket em revisão, C4) e congelar
go run ./bench export --db bench/corpus/capture.db --out bench/corpus/
go run ./bench hash   --corpus bench/corpus/                     # imprime o hash do corpus

# 3. Rodar o benchmark completo (A1 + A2)
make bench-e2e                                                   # = go run ./bench
# equivalente explícito:
go run ./bench --corpus bench/corpus --tasks bench/tasks \
  --config bench/ctxgate.bench.yaml --corpus-sha256 <hash> \
  --seed 42 --runs 5 --reps 30 --warmup 20 \
  --out bench/results/$(date +%F)-$(git rev-parse --short HEAD)/

# 4. Verificar determinismo (M4)
make bench-verify          # roda 2x e compara o bloco deterministic{} do report.json

# 5. Corpus público (obrigatório, ADR-004; sem corpus privado; roda no CI e na máquina de qualquer pessoa)
make bench-public          # usa bench/public/ (corpus sintético versionado, seed fixa); roda no CI
```

**Saídas** (`bench/results/<data>-<commit>/`): `report.json`, `report.csv`, `report.md` (tabela). Contém: data, commit, `dirty`, hash do corpus, `config_hash`, versões, máquina, e as linhas por tarefa e agregadas. **Não contém** texto de resposta, prompt real nem padrão de evidência.

**O que terceiros conseguem reproduzir:**
- Com o **corpus público** (`bench/public/`, `make bench-public`): tudo (tokens, sucesso, determinismo e, na própria máquina, latência).
- Com o **próprio corpus** (capturado com `store.capture_content: true` no formato acima): idem.
- Os números do **FitToday-cms** não são reproduzíveis por terceiros (corpus privado). O que o repo oferece é o harness, o hash do corpus e os agregados. Ver C1 (aceito pelo Product, S6-01 em revisão). O README publica as duas tabelas: a privada (só agregados e hash) e a pública (reproduzível).

**Checks de privacidade (QA, a cada PR que toca `bench/`):**
1. `git ls-files bench/corpus` vazio; `.gitignore` contém `bench/corpus/`.
2. `make bench-privacy-check` (local): procura cada termo de `bench/corpus/denylist.txt` em `git ls-files` e no `report.*`.
3. `bench/tasks/*.yaml` sem blocos de código e sem caminhos de arquivo.
4. `report.*` sem trechos de resposta.

---

## 9. Riscos

| # | Risco | Impacto | Mitigação |
|---|-------|---------|-----------|
| R1 | Redução fica <30% porque respostas ≤6000 tokens passam intactas e `query_source=none` não corta (ADR-002, decisoes-cto) | M1 reprovado | Reportar fração por faixa e por `query_source`; relatório honesto (S5-03); alavanca documentada: dedup de sessão (ADR-006, fora do MVP) |
| R2 | Evidência retida ≠ agente acerta; replay não mostra mudança de comportamento do agente | M2 otimista | Rótulo "proxy" em todo relatório e no README; camada B como apêndice |
| R3 | Evidência definida depois de ver o resultado, ou frouxa demais | Aprovação falsa | Congelamento por HMAC antes da 1ª rodada; revisão do QA (§3.3 regra 4) |
| R4 | Sobreajuste ao ajustar config nas mesmas 10 tarefas | Resultado não generaliza | Registrar tentativas; corpus sintético e (se sobrar tempo) camada B como checagem independente |
| R5 | Latência ruidosa (SO, energia, disco) | M3 instável | Warmup, ≥1000 amostras no recorte ≤100 KB, 5 processos, máquina documentada, p95 por execução; CI sem gate de latência |
| R6 | Corpus capturado vaza dado privado no repo | Incidente de privacidade | `.gitignore`, `bench-privacy-check`, `denylist`, HMAC, revisão de Vinicius |
| R7 | Tokenizer aproximado | Números absolutos diferem do que o agente cobra | Declarar no README; comparar só %; redução relativa é estável |
| R8 | Corpus pequeno (10 tarefas) e de um só projeto | Pouca generalização | Dizer no README; intervalo de confiança por bootstrap; corpus público com as mesmas 10 formas de tarefa |
| R9 | Gravação em modo captura mexe no caminho de teste (upstream mudou depois, MCP versão nova) | Replay não bate com produção | Registrar versão do protocolo e do upstream na captura; fixar na rodada |
| R10 | 5h do S5 não cobrem captura de 10 tarefas reais + evidências + harness + análise | Escopo cortado | Ver C4; ordem de corte do ticket (≥10 tarefas só com aprovação), nunca tirar a taxa de sucesso |
| R11 | Fallback do pipeline (timeout, panic) mascara problema | Redução parece boa sem pipeline atuar | Contar `mode=fallback` e falhar a rodada se >0 sem explicação |

---

## 10. Conflitos e lacunas encontrados

**Legenda de status:** *Resolvido* = decisão do CTO incorporada neste plano · *Aceito, ticket em revisão* = Product aceitou e está atualizando o ticket · *Aberto* = sem decisão.

| # | Onde | Conflito | Status e resolução |
|---|------|----------|--------------------|
| C1 | ADR-004 (adendo) × PRD §7/US21 × S6-01 | PRD e S6-01 pedem benchmark "reproduzível por terceiros", mas com o FitToday-cms privado terceiros não reproduzem os números principais. | **Aceito, ticket em revisão (S6-01).** Terceiros reproduzem com `make bench-public` ou com o próprio corpus; o README publica as duas tabelas e declara que os números do FitToday-cms se verificam só por hash e harness (§8, §11) |
| C2 | ADR-004 × S5-02 | "`required_evidence` sem colar código" é incompatível com verificação automática, que precisa do padrão literal. | **Resolvido (CTO).** Padrões literais só no corpus privado local; o repo guarda `description` + HMAC-SHA256 com chave local fora do repo (§3.2, §3.3) |
| C3 | S5-01 | O texto pede "fonte (commit/issue/prompt)", arquivo de tarefas "versionado no repo" e "≥15 de 20 com comando pass/fail". | **Aceito, ticket em revisão (S5-01).** Critério da camada A = `required_evidence` 10/10; fonte só no privado; ≥15 pass/fail vira apêndice da camada B |
| C4 | S5-01 (1,5h) × ADR-004 | Capturar 10 tarefas reais, definir evidências e exportar o SQLite para `jsonl.gz` não cabe em 1,5h, e o exportador não estava em nenhum ticket. | **Aceito, ticket em revisão (S5-01 / exportador SQLite).** Replanejamento de horas do S5 (5,0h sem folga) segue com o Product |
| C5 | S5-03 | No replay o baseline é 10/10 por construção, então "sucesso ≥ baseline" é o mesmo que 10/10. | **Aceito, ticket em revisão (S5-03).** Baseline vira checagem de validade (M5); meta = 10/10 (§7) |
| C6 | ADR-005 × PRD | A ADR-005 trocou `tiktoken-go` por `tiktoken-go/tokenizer` v0.10.0 e estava pendente de aprovação. | **Resolvido (CTO).** ADR-005 aceita: `tiktoken-go/tokenizer` v0.10.0 com `cl100k_base` desde a primeira rodada (§2.3, §5) |
| C7 | PRD §7 × ADR-005 §5 | PRD fala em p95 <20 ms geral; a ADR-005 dimensiona o orçamento para 100 KB, e respostas maiores podem estourar. | **Resolvido (CTO).** Gate = p95 <20 ms nas respostas ≤100 KB, com recorte por faixa. >100 KB é só reportado, e poucas amostras nessa faixa é dado, não exceção (M3, §6) |
| C8 | S5-03 | Ajustar config nas mesmas 10 tarefas da avaliação é sobreajuste. | **Aceito, ticket em revisão (S5-03).** Config congelada antes da rodada final; `config_hash` no resultado; tentativas registradas (§2.3, §7) |
| C9 | ADR-004 "Riscos" | A seção ainda diz "Pendente: confirmar se o FitToday-cms é público", resolvido no adendo. | **Aberto.** CTO limpa o texto da ADR |
| C10 | ADR-004 | O corpus sintético público estava como opcional. | **Resolvido (CTO)**: obrigatório, em `bench/public/`, `make bench-public`, conforme o adendo da ADR-004. **Aceito, ticket em revisão (Product)** |
| C11 | ADR-004 × ADR-002 | O replay assume que o agente teria feito as mesmas chamadas com o contexto cortado. | **Aberto.** Limitação a declarar no README; só a camada B cobre. Sem decisão registrada |

---

## 11. O que vai no README

Seção "Benchmark" (S6-01), com os números idênticos ao `report.md` da rodada (checagem linha a linha):

1. Tabela: tokens baseline, ctxgate, % por categoria e total; sucesso (proxy) baseline/ctxgate; p50/p95 de overhead; nº de amostras.
2. A frase: "Sucesso = retenção das evidências obrigatórias em replay. Não equivale a tarefa concluída por um agente."
3. "Tokens aproximados (cl100k_base); o valor cobrado pelo seu agente pode diferir."
4. Fração de chamadas sem consulta (não cortadas) e por faixa de tamanho.
5. Hash do corpus, commit, **`config_hash` (config congelada)**, data e máquina.
6. **Duas tabelas:** a privada (FitToday-cms: só agregados e hash do corpus) e a pública (`bench/public/`, reproduzível com `make bench-public`). Aviso: "Os 10 casos privados vêm de um projeto fechado; o corpus não é público e o código dele não foi anonimizado nem publicado."
7. Como reproduzir (comandos da §8) e como capturar o próprio corpus. O gate de overhead vale para respostas ≤100 KB; >100 KB aparece como informativo.
8. Limitações: replay não vê mudança de comportamento do agente; 10 tarefas de um só projeto; sem consulta ⇒ sem corte; Windows sem smoke test.
9. Camada B, se existir: com variância, rotulada como amostra manual.
