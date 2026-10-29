// Package tokens conta tokens de forma aproximada (ADR-005).
package tokens

// Counter conta tokens de um texto. Implementações devem ser seguras para uso concorrente.
type Counter interface {
	Count(text string) int
	Encoding() string // ex.: "cl100k_base"; gravado em cada Record
}
