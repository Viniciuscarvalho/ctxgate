package mcp

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/Viniciuscarvalho/ctxgate/internal/pipeline"
)

const shutdownGrace = 2 * time.Second

// Option customizes the Relay that Run builds.
type Option func(*Relay)

// WithReducer plugs a Reducer into the relay (see Relay.Reducer).
func WithReducer(r Reducer) Option { return func(relay *Relay) { relay.Reducer = r } }

// WithOnReport sets Relay.OnReport.
func WithOnReport(f func(pipeline.Report)) Option { return func(relay *Relay) { relay.OnReport = f } }

// WithMaxInFlight sets Relay.MaxInFlight (zero uses the default of 16).
func WithMaxInFlight(n int) Option { return func(relay *Relay) { relay.MaxInFlight = n } }

// Run starts one stdio upstream and relays the agent session to it.
func Run(ctx context.Context, agentIn io.Reader, agentOut io.Writer, stderr io.Writer, command string, args []string, env map[string]string, startupTimeout time.Duration, opts ...Option) error {
	cmd := exec.Command(command, args...)
	cmd.Stderr = stderr
	cmd.Env = mergeEnv(env)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	upstreamIn, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("upstream stdin: %w", err)
	}
	upstreamOut, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("upstream stdout: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start upstream %q: %w", command, err)
	}

	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()

	relayCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	started := make(chan struct{})
	responded := make(chan struct{})
	timedOut := make(chan struct{})
	go watchStartup(relayCtx, cancel, started, responded, timedOut, startupTimeout)

	relay := Relay{UpstreamIn: upstreamIn, UpstreamOut: upstreamOut}
	if stderr != nil {
		relay.Logger = slog.New(slog.NewTextHandler(stderr, nil))
	}
	for _, opt := range opts {
		opt(&relay)
	}
	// O encerramento do upstream começa quando o Serve passa a esperar os OnReport
	// (Relay.Exiting) e roda em paralelo com essa espera, sob o mesmo prazo de shutdownGrace.
	shutdownDone := make(chan struct{})
	startShutdown := sync.OnceFunc(func() {
		go func() {
			shutdown(cmd.Process.Pid, upstreamIn, waited)
			close(shutdownDone)
		}()
	})
	relay.Exiting = startShutdown
	serveErr := relay.Serve(relayCtx, &firstReadReader{Reader: cancelOnEOFReader{Reader: agentIn, cancel: cancel}, signal: started}, &firstWriteWriter{Writer: agentOut, signal: responded})
	startShutdown() // Serve pode voltar antes de chamar o gancho (erro de argumentos)
	<-shutdownDone
	select {
	case <-timedOut:
		return fmt.Errorf("upstream %q did not respond within startup_timeout %s", command, startupTimeout)
	default:
	}
	return serveErr
}

func watchStartup(ctx context.Context, cancel context.CancelFunc, started, responded <-chan struct{}, timedOut chan<- struct{}, timeout time.Duration) {
	select {
	case <-ctx.Done():
		return
	case <-started:
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-responded:
	case <-timer.C:
		close(timedOut)
		cancel()
	}
}

type firstReadReader struct {
	io.Reader
	once   sync.Once
	signal chan<- struct{}
}

type cancelOnEOFReader struct {
	io.Reader
	cancel context.CancelFunc
}

func (r cancelOnEOFReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		r.cancel()
	}
	return n, err
}

func (r *firstReadReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.once.Do(func() { close(r.signal) })
	}
	return n, err
}

type firstWriteWriter struct {
	io.Writer
	once   sync.Once
	signal chan<- struct{}
}

func (w *firstWriteWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if n > 0 {
		w.once.Do(func() { close(w.signal) })
	}
	return n, err
}

func mergeEnv(overrides map[string]string) []string {
	env := append([]string(nil), os.Environ()...)
	for key, value := range overrides {
		env = append(env, key+"="+value)
	}
	return env
}

func shutdown(pid int, stdin io.Closer, waited <-chan error) {
	_ = stdin.Close()
	select {
	case <-waited:
		return
	default:
	}
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	select {
	case <-waited:
		return
	case <-time.After(shutdownGrace):
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		<-waited
	}
}
