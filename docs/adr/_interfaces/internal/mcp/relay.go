// Package mcp: relay JSON-RPC 2.0 sobre stdio, agnóstico de versão do protocolo (ADR-001).
package mcp

import (
	"context"
	"encoding/json"
	"io"

	"github.com/Viniciuscarvalho/ctxgate/internal/pipeline"
)

// Reducer é a única dependência do relay para cortar contexto.
// *pipeline.Pipeline satisfaz esta interface (interface definida no consumidor).
type Reducer interface {
	Run(ctx context.Context, in pipeline.Input) pipeline.Outcome
}

// Upstream é o subprocesso MCP. Spawn/Close ficam atrás desta interface
// para testar com pipes em memória (dois adapters: processo real e io.Pipe).
type Upstream interface {
	Name() string
	Streams() (w io.WriteCloser, r io.ReadCloser, err error)
	Close(ctx context.Context) error // SIGTERM, espera, SIGKILL; sem zumbi
}

// Relay copia mensagens agente<->upstream sem interpretar, exceto a resposta de tools/call.
type Relay struct {
	// Up, Reducer (pode ser nil => passthrough puro), Observer, MaxInFlight, Logger (stderr)
}

// Serve bloqueia até EOF do agente, erro fatal do upstream ou cancelamento de ctx.
// stdout só recebe protocolo; logs vão para stderr.
func (r *Relay) Serve(ctx context.Context, agentIn io.Reader, agentOut io.Writer) error { return nil }

// ToolResultView é a visão mínima de um result de tools/call que o relay sabe editar.
// O resto do objeto (isError, _meta, resultType, campos desconhecidos) é preservado como bytes.
type ToolResultView struct {
	Raw     json.RawMessage   // objeto result original
	Content []json.RawMessage // blocos do campo "content"
}
