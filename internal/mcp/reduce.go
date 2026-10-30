package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"

	"github.com/Viniciuscarvalho/ctxgate/internal/pipeline"
)

// Reducer é a única dependência do relay para cortar contexto; *pipeline.Pipeline a
// satisfaz. Ela fica na fronteira: o relay não conhece estágios nem tokens.
type Reducer interface {
	Run(ctx context.Context, in pipeline.Input) pipeline.Outcome
}

// toolCalls guarda os tools/call enviados ao upstream até a resposta voltar.
type toolCalls struct {
	mu      sync.Mutex
	pending map[string]pipeline.Call
}

// note registra o pedido se ele for um tools/call; qualquer outra linha é ignorada.
func (t *toolCalls) note(line []byte) {
	if !bytes.Contains(line, []byte(`"tools/call"`)) {
		return
	}
	var req struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		} `json:"params"`
	}
	if json.Unmarshal(line, &req) != nil || req.Method != "tools/call" || len(req.ID) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pending == nil {
		t.pending = map[string]pipeline.Call{}
	}
	t.pending[string(req.ID)] = pipeline.Call{Tool: req.Params.Name, Args: req.Params.Arguments}
}

func (t *toolCalls) take(id json.RawMessage) (pipeline.Call, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	call, ok := t.pending[string(id)]
	delete(t.pending, string(id))
	return call, ok
}

func (t *toolCalls) idle() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending) == 0
}

// reduce é o ponto único em que o pipeline entra no relay. Só a resposta de um
// tools/call passa por aqui; qualquer falha devolve a linha original (fail-open).
// after, se não for nil, roda depois que a linha foi enviada ao agente e entrega o Report
// (ver deliver); não espera a contagem exata.
func (r Relay) reduce(ctx context.Context, calls *toolCalls, line []byte) (out []byte, after func()) {
	out = line
	defer func() {
		if recover() != nil {
			out, after = line, nil
		}
	}()
	var env map[string]json.RawMessage
	if json.Unmarshal(line, &env) != nil || len(env["result"]) == 0 || len(env["id"]) == 0 {
		return line, nil
	}
	call, ok := calls.take(env["id"])
	if !ok {
		return line, nil
	}

	view := viewOf(env["result"])
	outcome := r.Reducer.Run(ctx, pipeline.Input{Call: call, Texts: view.texts, NonTextBytes: view.nonTextBytes, SkipReason: view.skip})
	if r.OnReport != nil {
		after = func() { r.deliver(outcome) }
	}
	if outcome.Report.Mode != pipeline.ModeApplied || len(outcome.Texts) != len(view.texts) {
		return line, after
	}
	if rewritten, err := view.rewrite(env, outcome.Texts); err == nil {
		out = rewritten
	}
	return out, after
}

// deliver entrega o Report ao observador sem esperar a contagem exata: o que espera é uma
// goroutine, e só max_in_flight delas existem ao mesmo tempo. Sem vaga, o Report sai na
// hora e, se a contagem ainda não terminou, com Exact=false (fora do M1).
func (r Relay) deliver(o pipeline.Outcome) {
	select {
	case r.deliveries <- struct{}{}:
		go func() {
			defer func() { <-r.deliveries }()
			report := o.Report
			if o.Done != nil {
				report = <-o.Done
			}
			r.OnReport(report)
		}()
	default:
		report := o.Report
		report.Exact = false
		if o.Done != nil {
			select {
			case report = <-o.Done:
			default:
			}
		}
		r.OnReport(report)
	}
}

// resultView é a visão mínima de um result de tools/call que o relay sabe editar:
// o resto do objeto (isError, _meta, resultType, campos desconhecidos) fica como bytes.
type resultView struct {
	raw          map[string]json.RawMessage
	blocks       []map[string]json.RawMessage
	isText       []bool // por bloco: é bloco de texto
	texts        []string
	nonTextBytes int
	skip         string // por que o result não é elegível; vazio se for
}

func viewOf(result json.RawMessage) resultView {
	var v resultView
	if json.Unmarshal(result, &v.raw) != nil {
		v.skip = "no_text"
		return v
	}
	if json.Unmarshal(v.raw["content"], &v.blocks) == nil {
		for _, b := range v.blocks {
			var typ, text string
			_ = json.Unmarshal(b["type"], &typ)
			isText := typ == "text" && json.Unmarshal(b["text"], &text) == nil
			v.isText = append(v.isText, isText)
			if isText {
				v.texts = append(v.texts, text)
			} else if raw, err := json.Marshal(b); err == nil {
				v.nonTextBytes += len(raw)
			}
		}
	}

	var isError bool
	var resultType string
	_ = json.Unmarshal(v.raw["isError"], &isError)
	_ = json.Unmarshal(v.raw["resultType"], &resultType)
	switch {
	case isError:
		v.skip = "is_error"
	case v.raw["structuredContent"] != nil:
		v.skip = "structured_content"
	case resultType == "input_required":
		v.skip = "input_required"
	case resultType != "" && resultType != "complete":
		v.skip = "result_type"
	case v.nonTextBytes > 0:
		v.skip = "non_text_block"
	case len(v.texts) == 0:
		v.skip = "no_text"
	}
	return v
}

// rewrite troca o texto dos blocos de texto e tira os que ficaram vazios; o resto da
// resposta mantém os bytes originais de cada valor.
func (v resultView) rewrite(env map[string]json.RawMessage, texts []string) ([]byte, error) {
	var content []map[string]json.RawMessage
	next := 0
	for i, b := range v.blocks {
		if !v.isText[i] {
			content = append(content, b)
			continue
		}
		text := texts[next]
		next++
		if text == "" {
			continue
		}
		encoded, err := marshalNoEscape(text)
		if err != nil {
			return nil, err
		}
		b["text"] = encoded
		content = append(content, b)
	}
	var err error
	if v.raw["content"], err = json.Marshal(content); err != nil {
		return nil, err
	}
	if env["result"], err = json.Marshal(v.raw); err != nil {
		return nil, err
	}
	out, err := json.Marshal(env)
	return append(out, '\n'), err
}

func marshalNoEscape(s string) (json.RawMessage, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
