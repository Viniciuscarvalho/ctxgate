package pipeline

import "strings"

// paragraph é a unidade do ADR-006: linhas seguidas das linhas em branco que as separam
// do próximo parágrafo. Juntar os parágrafos devolve o texto original.
type paragraph struct {
	lines  []string // cada linha mantém seu "\n"
	fenced bool     // contém bloco de código cercado: nunca é subdividido
}

// paragraphs quebra text por linha em branco. Linhas em branco dentro de um bloco
// cercado (```) não quebram o parágrafo.
func paragraphs(text string) []paragraph {
	var out []paragraph
	var cur paragraph
	inFence, hasContent, prevBlank := false, false, false
	for rest := text; rest != ""; {
		line := rest
		if i := strings.IndexByte(rest, '\n'); i >= 0 {
			line, rest = rest[:i+1], rest[i+1:]
		} else {
			rest = ""
		}
		blank := !inFence && strings.TrimSpace(line) == ""
		if !blank && prevBlank && hasContent {
			out = append(out, cur)
			cur, hasContent = paragraph{}, false
		}
		if !blank {
			hasContent = true
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				inFence = !inFence
				cur.fenced = true
			}
		}
		prevBlank = blank
		cur.lines = append(cur.lines, line)
	}
	if len(cur.lines) > 0 {
		out = append(out, cur)
	}
	return out
}
