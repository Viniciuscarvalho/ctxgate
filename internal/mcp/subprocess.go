package mcp

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

const (
	shutdownGrace         = 2 * time.Second // fixed on purpose: no YAML field (decisoes-cto, 2026-10-29)
	defaultStartupTimeout = 30 * time.Second
)

// Run starts one stdio upstream and relays the agent session to it.
func Run(ctx context.Context, agentIn io.Reader, agentOut io.Writer, stderr io.Writer, command string, args []string, env map[string]string, startupTimeout time.Duration) error {
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

	relayCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	started := make(chan struct{})
	responded := make(chan struct{})
	if startupTimeout <= 0 {
		startupTimeout = defaultStartupTimeout
	}
	go watchStartup(relayCtx, cancel, started, responded, fmt.Errorf("upstream %q did not respond within startup_timeout %s", command, startupTimeout), startupTimeout)

	relay := Relay{UpstreamIn: upstreamIn, UpstreamOut: upstreamOut}
	serveErr := relay.Serve(relayCtx, &firstReadReader{Reader: cancelOnEOFReader{Reader: agentIn, cancel: func() { cancel(nil) }}, signal: started}, &firstWriteWriter{Writer: agentOut, signal: responded})
	shutdown(cmd.Process.Pid, upstreamIn, waited)
	return serveErr
}

func watchStartup(ctx context.Context, cancel context.CancelCauseFunc, started, responded <-chan struct{}, timeoutErr error, timeout time.Duration) {
	select {
	case <-ctx.Done():
		return
	case <-started:
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-responded:
	case <-timer.C:
		cancel(timeoutErr)
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

// shutdown ends the upstream's whole process group: stdin closes, SIGTERM goes
// to the group, and whatever is still alive after shutdownGrace gets SIGKILL.
func shutdown(pgid int, stdin io.Closer, waited <-chan error) {
	_ = stdin.Close()
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	deadline := time.Now().Add(shutdownGrace)
	for groupAlive(pgid) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if groupAlive(pgid) {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}
	<-waited
}

func groupAlive(pgid int) bool {
	return syscall.Kill(-pgid, 0) == nil
}
