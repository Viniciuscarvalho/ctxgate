// Package mcp relays newline-delimited JSON-RPC between stdio streams.
package mcp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"sync"
)

// Relay is deliberately unaware of JSON-RPC structure while the pipeline is
// disabled. Each complete line is forwarded exactly as it arrived.
type Relay struct {
	UpstreamIn  io.WriteCloser
	UpstreamOut io.Reader
}

// Serve forwards agent messages and upstream messages until the agent closes
// its input, the upstream closes its output, or ctx is cancelled.
func (r Relay) Serve(ctx context.Context, agentIn io.Reader, agentOut io.Writer) error {
	if r.UpstreamIn == nil || r.UpstreamOut == nil {
		return errors.New("mcp relay: upstream streams are required")
	}

	var agentOutMu sync.Mutex
	upstreamDone := make(chan error, 1)
	go func() {
		upstreamDone <- copyLines(r.UpstreamOut, func(line []byte) error {
			agentOutMu.Lock()
			defer agentOutMu.Unlock()
			_, err := agentOut.Write(line)
			return err
		})
	}()

	agentDone := make(chan error, 1)
	go func() {
		agentDone <- copyLines(agentIn, func(line []byte) error {
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
