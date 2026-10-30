// Package mcp relays newline-delimited JSON-RPC between stdio streams.
package mcp

import (
	"bufio"
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
	// resposta foi enviada e a contagem exata terminou. Não bloqueia o relay.
	OnReport func(pipeline.Report)
}

// Serve forwards agent messages and upstream messages until the agent closes
// its input, the upstream closes its output, or ctx is cancelled.
func (r Relay) Serve(ctx context.Context, agentIn io.Reader, agentOut io.Writer) error {
	if r.UpstreamIn == nil || r.UpstreamOut == nil {
		return errors.New("mcp relay: upstream streams are required")
	}

	var agentOutMu sync.Mutex
	var calls toolCalls
	upstreamDone := make(chan error, 1)
	go func() {
		upstreamDone <- copyLines(r.UpstreamOut, func(line []byte) error {
			after := func() {}
			if r.Reducer != nil {
				var next func()
				if line, next = r.reduce(ctx, &calls, line); next != nil {
					after = next
				}
			}
			agentOutMu.Lock()
			_, err := agentOut.Write(line)
			agentOutMu.Unlock()
			after()
			return err
		})
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
