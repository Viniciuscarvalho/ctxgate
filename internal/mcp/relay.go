// Package mcp relays newline-delimited JSON-RPC between stdio streams.
package mcp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/Viniciuscarvalho/ctxgate/internal/pipeline"
)

// Relay is deliberately unaware of JSON-RPC structure while the pipeline is
// disabled. Each complete line is forwarded exactly as it arrived.
type Relay struct {
	UpstreamIn  io.WriteCloser
	UpstreamOut io.Reader

	// Reducer, se não for nil, recebe o result de cada tools/call elegível (ver reduce.go).
	// Nil mantém o passthrough byte a byte.
	Reducer Reducer
	// OnReport, se não for nil, recebe o Report final de cada tools/call depois que a
	// resposta foi enviada e a contagem exata terminou. Não atrasa o envio ao agente nem
	// segura a vaga da resposta; com MaxInFlight Reports pendentes, o próximo sai na hora
	// com Exact=false.
	OnReport func(pipeline.Report)
	// MaxInFlight limita quantas respostas de tools/call são reduzidas ao mesmo tempo e
	// quantos Reports esperam a contagem exata (ADR-001 §3). Zero ou negativo usa 16.
	MaxInFlight int

	deliveries chan struct{} // vagas dos Reports pendentes; criado em Serve
}

const defaultMaxInFlight = 16

var resultKey = []byte(`"result"`)

func (r Relay) maxInFlight() int {
	if r.MaxInFlight <= 0 {
		return defaultMaxInFlight
	}
	return r.MaxInFlight
}

// Serve forwards agent messages and upstream messages until the agent closes
// its input, the upstream closes its output, or ctx is cancelled.
func (r Relay) Serve(ctx context.Context, agentIn io.Reader, agentOut io.Writer) error {
	if r.UpstreamIn == nil || r.UpstreamOut == nil {
		return errors.New("mcp relay: upstream streams are required")
	}

	r.deliveries = make(chan struct{}, r.maxInFlight())
	var agentOutMu sync.Mutex
	var calls toolCalls
	write := func(line []byte) error {
		agentOutMu.Lock()
		defer agentOutMu.Unlock()
		_, err := agentOut.Write(line)
		return err
	}
	upstreamDone := make(chan error, 1)
	go func() {
		var inFlight sync.WaitGroup
		writeErr := make(chan error, 1) // primeiro erro de escrita de uma resposta reduzida
		slots := make(chan struct{}, r.maxInFlight())
		err := copyLines(r.UpstreamOut, func(line []byte) error {
			select {
			case err := <-writeErr:
				return err
			default:
			}
			// Só um result pode ser a resposta de um tools/call pendente. O resto segue em
			// ordem, na hora; as respostas reduzidas saem em qualquer ordem (o id correlaciona).
			if r.Reducer == nil || calls.idle() || !bytes.Contains(line, resultKey) {
				return write(line)
			}
			select {
			case slots <- struct{}{}: // com todas as vagas ocupadas, o relay espera aqui
			case <-ctx.Done():
				return ctx.Err()
			}
			inFlight.Add(1)
			go func() {
				defer func() { <-slots }()
				out, after := r.reduce(ctx, &calls, line)
				if err := write(out); err != nil {
					select {
					case writeErr <- err:
					default:
					}
				}
				inFlight.Done()
				if after != nil {
					after() // só passa de max_in_flight Reports pendentes; a vaga não espera a contagem
				}
			}()
			return nil
		})
		inFlight.Wait()
		select {
		case err = <-writeErr: // a escrita que falhou vale mais que o EOF do upstream
		default:
		}
		upstreamDone <- err
	}()

	agentDone := make(chan error, 1)
	go func() {
		agentDone <- copyLines(agentIn, func(line []byte) error {
			if r.Reducer != nil {
				calls.note(line)
			}
			_, err := r.UpstreamIn.Write(line)
			return err
		})
	}()

	select {
	case <-ctx.Done():
		_ = r.UpstreamIn.Close()
		return nil
	case err := <-agentDone:
		_ = r.UpstreamIn.Close()
		return normalizeCopyError(err)
	case err := <-upstreamDone:
		return normalizeCopyError(err)
	}
}

func copyLines(reader io.Reader, write func([]byte) error) error {
	buffered := bufio.NewReader(reader)
	for {
		line, err := buffered.ReadBytes('\n')
		if len(line) != 0 {
			if writeErr := write(line); writeErr != nil {
				return writeErr
			}
		}
		if err != nil {
			return err
		}
	}
}

func normalizeCopyError(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
