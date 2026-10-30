// Package tokens conta tokens de forma aproximada (ADR-005): o encoding é
// cl100k_base e outros tokenizers (Claude, Cursor) contam de outro jeito.
// O pacote não importa pipeline, metrics, mcp nem store.
package tokens

import (
	"context"
	"crypto/sha256"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/tiktoken-go/tokenizer"
)

// Counter conta tokens de um texto e é seguro para uso concorrente.
type Counter interface {
	Count(text string) (int, error)
	Encoding() string // gravado em cada Record
}

type cl100k struct{ codec tokenizer.Codec }

// New devolve o contador cl100k_base, com vocabulário embutido (offline).
func New() (Counter, error) {
	codec, err := tokenizer.Get(tokenizer.Cl100kBase)
	if err != nil {
		return nil, err
	}
	return cl100k{codec}, nil
}

func (c cl100k) Count(text string) (int, error) { return c.codec.Count(text) }
func (cl100k) Encoding() string                 { return string(tokenizer.Cl100kBase) }

// Measurement é o que a chamada grava no Report/Record (tokens_before,
// tokens_after, encoding, bytes não textuais e tokens_exact).
type Measurement struct {
	Encoding     string
	TokensBefore int
	TokensAfter  int
	NonTextBytes int  // bytes de blocos não textuais: nunca viram tokens
	Exact        bool // false: a contagem falhou e os tokens não valem; o stats ignora a linha
}

const maxWorkers = 4

// CountAll conta cada texto uma única vez, com até min(GOMAXPROCS,4) workers.
// Textos idênticos na mesma chamada são contados uma só vez (cache por hash).
func CountAll(ctx context.Context, c Counter, texts []string) ([]int, error) {
	index := make(map[[sha256.Size]byte]int, len(texts))
	var unique []string
	slot := make([]int, len(texts))
	for i, t := range texts {
		h := sha256.Sum256([]byte(t))
		j, ok := index[h]
		if !ok {
			j = len(unique)
			index[h] = j
			unique = append(unique, t)
		}
		slot[i] = j
	}

	counts := make([]int, len(unique))
	jobs := make(chan int)
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	fail := func(err error) { errOnce.Do(func() { firstErr = err }) }

	workers := min(runtime.GOMAXPROCS(0), maxWorkers, len(unique))
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				n, err := countSafely(c, unique[j])
				if err != nil {
					fail(err)
					continue
				}
				counts[j] = n
			}
		}()
	}
feed:
	for j := range unique {
		select {
		case jobs <- j:
		case <-ctx.Done():
			fail(ctx.Err())
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	out := make([]int, len(texts))
	for i, j := range slot {
		out[i] = counts[j]
	}
	return out, nil
}

// countSafely transforma panic do contador em erro: a contagem nunca derruba o proxy.
func countSafely(c Counter, text string) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("tokens: counter panic: %v", r)
		}
	}()
	return c.Count(text)
}

// asyncTimeout é o prazo próprio da contagem em segundo plano (decisão do CTO).
var asyncTimeout = 5 * time.Second

// Unchanged mede uma resposta que sai intacta (passthrough ou atalho do ADR-005 §4):
// tokens_before == tokens_after, com o valor exato. Se a contagem falhar, devolve
// Exact=false e tokens zerados.
func Unchanged(ctx context.Context, c Counter, texts []string, nonTextBytes int) Measurement {
	return measure(ctx, c, texts, nil, nonTextBytes)
}

// measure soma os tokens de todos os textos (before) e dos que têm keep[i] (after);
// keep nil mantém todos.
func measure(ctx context.Context, c Counter, texts []string, keep []bool, nonTextBytes int) Measurement {
	m := Measurement{Encoding: c.Encoding(), NonTextBytes: nonTextBytes}
	counts, err := CountAll(ctx, c, texts)
	if err != nil {
		return m
	}
	for i, n := range counts {
		m.TokensBefore += n
		if keep == nil || keep[i] {
			m.TokensAfter += n
		}
	}
	m.Exact = true
	return m
}

// UnchangedAsync retorna na hora e conta em outra goroutine, para o relay enviar a
// resposta ao agente antes e anexar a medição ao Record depois.
func UnchangedAsync(ctx context.Context, c Counter, texts []string, nonTextBytes int) <-chan Measurement {
	return MeasureAsync(ctx, c, texts, nil, nonTextBytes)
}

// MeasureAsync é a versão em segundo plano de measure. Depois do envio o agente pode
// cancelar a requisição, então a contagem ignora o cancelamento de ctx (WithoutCancel)
// e tem o próprio prazo de 5s.
func MeasureAsync(ctx context.Context, c Counter, texts []string, keep []bool, nonTextBytes int) <-chan Measurement {
	ch := make(chan Measurement, 1)
	ctx = context.WithoutCancel(ctx)
	go func() {
		ctx, cancel := context.WithTimeout(ctx, asyncTimeout)
		defer cancel()
		ch <- measure(ctx, c, texts, keep, nonTextBytes)
	}()
	return ch
}
