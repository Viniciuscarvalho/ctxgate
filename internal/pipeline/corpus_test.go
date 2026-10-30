package pipeline_test

import (
	"fmt"
	"math/rand/v2"
	"strings"
)

var sentences = []string{
	"O serviço respondeu com status 200 e o corpo foi gravado no cache local.",
	"Falha ao abrir o arquivo de configuração: permissão negada no diretório do usuário.",
	"A migração 0042 adicionou o índice parcial em pedidos por data de criação.",
	"Resumo da sessão: três ferramentas foram chamadas e duas devolveram resultados vazios.",
	"Não há ação sem atenção, e a atenção aos detalhes evita regressões no relay.",
	"The scheduler retries failed jobs with exponential backoff up to five attempts.",
	"Connection reset by peer while reading the response header from upstream.",
	"Atualize a dependência e rode os testes com a flag de corrida antes de abrir o PR.",
}

var codeLines = []string{
	"func (s *Server) handle(ctx context.Context, req Request) (Response, error) {",
	"\tif err := s.validate(req); err != nil {",
	"\t\treturn Response{}, fmt.Errorf(\"validate: %w\", err)",
	"\t}",
	"\tdefer s.metrics.Observe(time.Now())",
	"\treturn s.store.Lookup(ctx, req.Key)",
	"}",
}

// generated devolve blocos pequenos e variados, com parágrafos repetidos de propósito.
func generated(seed int) []string {
	rng := rand.New(rand.NewPCG(uint64(seed), 7))
	blocks := make([]string, rng.IntN(4))
	for i := range blocks {
		var parts []string
		for range rng.IntN(8) {
			parts = append(parts, sentences[rng.IntN(3)]) // vocabulário curto => repete
		}
		blocks[i] = strings.Join(parts, "\n\n")
	}
	return blocks
}

// mixedResponse monta ~size bytes de texto pt-BR/en/código; ~35% dos parágrafos repetem.
func mixedResponse(size int) string {
	rng := rand.New(rand.NewPCG(42, 1))
	var distinct []string
	var b strings.Builder
	for n := 0; b.Len() < size; n++ {
		if len(distinct) > 3 && rng.IntN(100) < 35 {
			b.WriteString(distinct[rng.IntN(len(distinct))])
			b.WriteString("\n\n")
			continue
		}
		var p strings.Builder
		if rng.IntN(3) == 0 {
			p.WriteString("```go\n")
			for range 3 + rng.IntN(5) {
				p.WriteString(codeLines[rng.IntN(len(codeLines))])
				p.WriteString("\n")
			}
			fmt.Fprintf(&p, "// bloco %d\n```", n)
		} else {
			for range 1 + rng.IntN(4) {
				p.WriteString(sentences[rng.IntN(len(sentences))])
				p.WriteString(" ")
			}
			fmt.Fprintf(&p, "(item %d)", n)
		}
		distinct = append(distinct, p.String())
		b.WriteString(p.String())
		b.WriteString("\n\n")
	}
	return b.String()
}
