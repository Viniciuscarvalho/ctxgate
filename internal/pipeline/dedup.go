package pipeline

import (
	"context"
	"crypto/sha256"
	"strings"
	"unicode"
)

// ExactDedup remove trechos repetidos na mesma resposta e mantém a primeira ocorrência
// (ADR-006). Dois trechos são iguais se o texto normalizado (minúsculas, espaços
// colapsados, pontuação de borda removida) tem o mesmo sha256, truncado a 16 bytes.
type ExactDedup struct{}

func (ExactDedup) Name() string { return "dedup" }

func (ExactDedup) Apply(ctx context.Context, d *Doc) error {
	seen := make(map[[16]byte]struct{}, len(d.Chunks))
	for i := range d.Chunks {
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		c := &d.Chunks[i]
		key := normalize(c.Text)
		if !c.Keep || key == "" {
			continue
		}
		sum := sha256.Sum256([]byte(key))
		h := [16]byte(sum[:16])
		if _, dup := seen[h]; dup {
			c.Keep, c.Reason = false, "dup:exact"
			continue
		}
		seen[h] = struct{}{}
	}
	return nil
}

func normalize(s string) string {
	s = strings.TrimFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsPunct(r) })
	var b strings.Builder
	b.Grow(len(s))
	space := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			space = true
			continue
		}
		if space {
			b.WriteByte(' ')
			space = false
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}
