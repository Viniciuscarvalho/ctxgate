package mcp

import (
	"bufio"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Viniciuscarvalho/ctxgate/internal/pipeline"
)

// Um upstream que responde a um tools/call e depois ignora SIGTERM e o fechamento do stdin.
const stubbornUpstream = `trap "" TERM
read line
printf '{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"x"}]}}\n'
while :; do sleep 1; done`

// O encerramento tem um prazo único de 2 s: a espera dos OnReport e o encerramento do
// upstream (fechar stdin, SIGTERM, SIGKILL) rodam em paralelo, não 2 s e depois mais 2 s.
func TestShutdownIsOneSharedDeadlineForReportsAndUpstream(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	logs := &syncBuffer{}
	agentInR, agentInW := io.Pipe()
	agentOutR, agentOutW := io.Pipe()
	ran := make(chan error, 1)
	go func() {
		ran <- Run(context.Background(), agentInR, agentOutW, logs, "sh", []string{"-c", stubbornUpstream}, nil, 10*time.Second,
			WithReducer(&fakeReducer{outcome: applied("x")}),
			WithOnReport(func(pipeline.Report) { <-release })) // Report lento: nunca termina
		_ = agentOutW.Close()
	}()
	if _, err := io.WriteString(agentInW, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"t","arguments":{}}}`+"\n"); err != nil {
		t.Fatal(err)
	}
	if line, err := bufio.NewReader(agentOutR).ReadString('\n'); err != nil || !strings.Contains(line, `"id":1`) {
		t.Fatalf("response = %q, err = %v", line, err)
	}
	start := time.Now()
	_ = agentInW.Close()
	select {
	case <-ran:
	case <-time.After(8 * time.Second):
		t.Fatal("Run did not return")
	}
	elapsed := time.Since(start)
	if elapsed < 1800*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("Run returned after %v, want about %v in total (reports and upstream under the same deadline)", elapsed, shutdownGrace)
	}
	if !strings.Contains(logs.String(), "discarded=1") {
		t.Fatalf("log = %q, want the discarded count", logs.String())
	}
}

// Até o Serve que devolve erro de argumentos passa pelo gancho, uma vez: o Run nunca fica
// esperando um encerramento que ninguém começou.
func TestServeCallsTheExitingHookOnceEvenOnAnArgumentError(t *testing.T) {
	calls := 0
	err := Relay{exiting: func() { calls++ }}.Serve(context.Background(), strings.NewReader(""), io.Discard)
	if err == nil || calls != 1 {
		t.Fatalf("err = %v, hook calls = %d, want an error and exactly 1 call", err, calls)
	}
}
