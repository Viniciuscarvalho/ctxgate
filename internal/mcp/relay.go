// Package mcp relays newline-delimited JSON-RPC between stdio streams.
package mcp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

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
	// com Exact=false. No encerramento, o Serve espera os pendentes até shutdownGrace (2 s);
	// o que não terminar é descartado e logado.
	OnReport func(pipeline.Report)
	// MaxInFlight limita quantas respostas de tools/call são reduzidas ao mesmo tempo e
	// quantos Reports esperam a contagem exata (ADR-001 §3). Zero ou negativo usa 16.
	MaxInFlight int
	// Logger recebe a linha de descarte do encerramento. Nil usa slog.Default().
	Logger *slog.Logger
	// Exiting, se não for nil, é chamado uma vez quando o Serve começa a esperar os
	// OnReport pendentes. mcp.Run usa o gancho para encerrar o upstream em paralelo com
	// essa espera, os dois sob o mesmo prazo de shutdownGrace.
	Exiting func()

	deliveries chan struct{}   // vagas dos Reports pendentes; criado em Serve
	pending    *pendingReports // OnReport ainda não terminados; criado em Serve
}

// pendingReports conta os OnReport que o Serve ainda precisa esperar no encerramento.
type pendingReports struct {
	mu   sync.Mutex
	n    int
	idle chan struct{} // fecha quando n volta a zero, se alguém espera
}

func (p *pendingReports) add() {
	p.mu.Lock()
	p.n++
	p.mu.Unlock()
}

func (p *pendingReports) done() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.n--; p.n == 0 && p.idle != nil {
		close(p.idle)
		p.idle = nil
	}
}

// wait espera os pendentes por até grace e devolve quantos não terminaram.
func (p *pendingReports) wait(grace time.Duration) int {
	p.mu.Lock()
	if p.n == 0 {
		p.mu.Unlock()
		return 0
	}
	idle := make(chan struct{})
	p.idle = idle
	p.mu.Unlock()
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-idle:
		return 0
	case <-timer.C:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.n
	}
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
	r.pending = &pendingReports{}
	defer r.drainReports() // todo retorno do Serve espera os OnReport, sem passar de shutdownGrace
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
				if after != nil {
					r.pending.add() // antes da escrita: quando o agente vê a resposta, o Report já é pendente
				}
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

// drainReports espera os OnReport pendentes por até shutdownGrace. O encerramento do
// upstream começa no mesmo instante (Exiting) e tem o mesmo prazo, então o processo sai
// em até shutdownGrace, não em 2 prazos seguidos. O que não terminar é descartado (a
// goroutine segue por conta própria) e uma linha de log informa quantos foram.
//
// No EOF do upstream, respostas que ainda estão sendo reduzidas (sem Report registrado)
// não contam como pendentes: só os OnReport já registrados são esperados. Aceito no MVP,
// porque nesse ponto o upstream já morreu e não há mais o que esperar dele.
func (r Relay) drainReports() {
	if r.Exiting != nil {
		r.Exiting()
	}
	if n := r.pending.wait(shutdownGrace); n > 0 {
		logger := r.Logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn("relay: pending reports discarded at shutdown", "discarded", n, "grace", shutdownGrace)
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
