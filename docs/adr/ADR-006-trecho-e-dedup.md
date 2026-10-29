# ADR-006 — Unidade de trecho, conteúdo não textual e dedup

- **Status:** aceita (Vinicius, 2026-10-29) · **Resolve:** questões 4 e 5

## Contexto
Dedup, ranking e orçamento precisam da mesma unidade ("trecho"). Muito pequena (frase) fragmenta o contexto; muito grande (bloco inteiro) não permite cortar. O PRD diz "trechos repetidos" sem escopo.

## Decisão
**Trecho = parágrafo** de um bloco `content` de tipo `text`:
1. Quebra por linha em branco (`\n\n`); **blocos de código cercados (```) nunca são partidos**. Trecho acima de 512 tokens é subdividido por linha (`\n`), nunca no meio de uma linha. Uma linha única gigante (ex.: JSON minificado) vira **um** trecho e passa intacta.
2. Separadores originais são preservados na remontagem; a **ordem original dos trechos mantidos é preservada** (o ranking decide *quais* ficam, não a ordem).
3. **Não textual passa intacto:** blocos `image`, `audio`, `resource_link`, `resource` e qualquer tipo desconhecido não entram no pipeline e ficam na mesma posição. Resposta com `isError:true`, `structuredContent`, `resultType:"input_required"` ou sem blocos `text` ⇒ **passthrough** com `skip_reason`.
4. Bloco de texto que ficar vazio é removido da lista; se tudo for removido, **política "nunca vazio":** mantém o trecho de maior score inteiro (pode exceder o orçamento) e registra `Reason`.
5. **Marcador de truncamento: não implementar no MVP** (questão 13): sem uma ferramenta de "expandir" o agente não consegue agir sobre ele, e o marcador quebra idempotência e `tokens_after ≤ tokens_before`. Reavaliar após o S5 junto com um `ctxgate_expand` (fase 2). O corte aparece em log/métricas.

**Dedup:** dois critérios, mantém sempre a **primeira** ocorrência.
- *Exato:* hash (`sha256` truncado) do trecho normalizado (minúsculas, espaços colapsados, pontuação de borda removida).
- *Similar:* trechos ≥20 tokens; pré-filtro simhash de 64 bits (Hamming ≤6) e confirmação por Jaccard de shingles de 3 palavras **≥0,90** (`dedup.similarity`, `1.0` desliga). Padrão conservador; qualquer falso positivo no conjunto negativo do S2-03 derruba o limiar para cima.
- **Escopo: dentro da mesma resposta.** Entre chamadas da sessão e persistido ficam fora do MVP: remover algo "já enviado" pode deixar o agente sem o trecho se o contexto dele foi compactado, e persistir entre sessões não faz sentido (o agente não guarda contexto). Primeira alavanca se o S5 ficar abaixo de 30%: escopo de sessão (LRU em memória) como nova ADR, aprovada pelo PO.

## Alternativas
Janela fixa de N tokens (corta no meio de ideias); bloco `content` inteiro (granularidade ruim); frase (fragmenta); Jaccard O(n²) puro (lento com centenas de trechos); dedup entre chamadas já no MVP (risco ao sucesso da tarefa e estado extra).

## Consequências
(+) Uma unidade para três estágios, testável por tabela, 100% preservando bytes dos trechos mantidos (saída ⊆ entrada). (−) Economia do dedup restrita a repetição intra-resposta; mudar a unidade depois altera todos os estágios.

## Riscos
Parágrafos longos de memória (um "fato" em um único parágrafo de 2 KB) cortam grosso demais; mitigação: subdivisão a 512 tokens e padrão de orçamento alto. Tipos de bloco novos do protocolo passam intactos por construção.
