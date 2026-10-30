// Package mcp relays newline-delimited JSON-RPC between stdio streams.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
)

const (
	codeInternalError     = -32603
	upstreamExitedMessage = "upstream exited before responding"
)

var errUpstreamGone = errors.New(upstreamExitedMessage)

// Relay forwards each complete line exactly as it arrived. It only reads the
// JSON-RPC id and method of each line, to know which requests are still
// waiting for an answer if the upstream goes away.
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

	var (
		agentOutMu sync.Mutex // serializes writes to the agent
		finished   bool       // set once pending requests were answered with errors
		pendingMu  sync.Mutex
		pending    = map[string]json.RawMessage{} // requests the upstream has not answered
	)

	// answerPending replies to every unanswered request with a JSON-RPC error
	// and stops any later upstream output from reaching the agent.
	answerPending := func(message string) int {
		pendingMu.Lock()
		ids := pending
		pending = map[string]json.RawMessage{}
		pendingMu.Unlock()

		agentOutMu.Lock()
		defer agentOutMu.Unlock()
		finished = true
		for _, key := range slices.Sorted(maps.Keys(ids)) {
			line, _ := json.Marshal(errorResponse{JSONRPC: "2.0", ID: ids[key], Error: rpcError{Code: codeInternalError, Message: message}})
			_, _ = agentOut.Write(append(line, '\n'))
		}
		return len(ids)
	}

	upstreamDone := make(chan error, 1)
	go func() {
		upstreamDone <- copyLines(r.UpstreamOut, func(line []byte) error {
			if id, isRequest, ok := idOf(line); ok && !isRequest {
				pendingMu.Lock()
				delete(pending, string(id))
				pendingMu.Unlock()
			}
			agentOutMu.Lock()
			defer agentOutMu.Unlock()
			if finished {
				return io.EOF
			}
			_, err := agentOut.Write(line)
			return err
		})
	}()

	agentDone := make(chan error, 1)
	go func() {
		agentDone <- copyLines(agentIn, func(line []byte) error {
			if id, isRequest, ok := idOf(line); ok && isRequest {
				pendingMu.Lock()
				pending[string(id)] = id
				pendingMu.Unlock()
			}
			if _, err := r.UpstreamIn.Write(line); err != nil {
				return fmt.Errorf("%w: %v", errUpstreamGone, err)
			}
			return nil
		})
	}()

	select {
	case <-ctx.Done():
		_ = r.UpstreamIn.Close()
		if cause := context.Cause(ctx); !errors.Is(cause, context.Canceled) {
			answerPending(cause.Error())
			return cause
		}
		return nil
	case err := <-agentDone:
		_ = r.UpstreamIn.Close()
		if errors.Is(err, errUpstreamGone) {
			answerPending(upstreamExitedMessage)
		}
		return normalizeCopyError(err)
	case err := <-upstreamDone:
		err = normalizeCopyError(err)
		if answerPending(upstreamExitedMessage) > 0 && err == nil {
			err = errUpstreamGone
		}
		return err
	}
}

type errorResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Error   rpcError        `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// idOf reads the id and kind of one JSON-RPC line without touching it. ok is
// false for notifications and for lines that are not a single JSON object.
func idOf(line []byte) (id json.RawMessage, isRequest, ok bool) {
	var m struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if json.Unmarshal(line, &m) != nil || len(m.ID) == 0 || string(m.ID) == "null" {
		return nil, false, false
	}
	return m.ID, m.Method != "", true
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
