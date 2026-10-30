package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/Viniciuscarvalho/ctxgate/internal/config"
	"github.com/Viniciuscarvalho/ctxgate/internal/mcp"
	"github.com/Viniciuscarvalho/ctxgate/internal/pipeline"
	"github.com/Viniciuscarvalho/ctxgate/internal/tokens"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 1 && args[0] == "--version" {
		_, err := fmt.Fprintf(stdout, "ctxgate %s\n", version())
		return err
	}
	if len(args) == 0 || args[0] != "serve" {
		return errors.New("usage: ctxgate serve --config <file> | ctxgate --version")
	}

	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to YAML configuration")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *configPath == "" {
		return errors.New("serve: --config is required")
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("serve: unexpected argument %q", flags.Arg(0))
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	upstream := cfg.Upstreams[0]
	if _, err := fmt.Fprintf(stderr, "ctxgate: configured upstream %q (%s)\n", upstream.Name, upstream.Command); err != nil {
		return err
	}

	opts, err := pipelineOptions(cfg.Pipeline, stderr)
	if err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return mcp.Run(ctx, stdin, stdout, stderr, upstream.Command, upstream.Args, upstream.Env, time.Duration(upstream.StartupTimeout), opts...)
}

// pipelineOptions liga o pipeline ao relay: o reducer e o limite pipeline.max_in_flight.
// Com pipeline.enabled: false não há opções e o relay fica em passthrough.
func pipelineOptions(cfg config.Pipeline, stderr io.Writer) ([]mcp.Option, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	reducer, err := newPipeline(cfg, stderr)
	if err != nil {
		return nil, err
	}
	return []mcp.Option{mcp.WithReducer(reducer), mcp.WithMaxInFlight(cfg.MaxInFlight)}, nil
}

// newPipeline monta o pipeline do relay a partir da config. Por enquanto só o dedup exato
// existe; ele roda quando pipeline.dedup.enabled é true (a config ainda não tem defaults).
func newPipeline(cfg config.Pipeline, stderr io.Writer) (*pipeline.Pipeline, error) {
	counter, err := tokens.New()
	if err != nil {
		return nil, fmt.Errorf("tokenizer: %w", err)
	}
	var stages []pipeline.Stage
	if cfg.Dedup.Enabled {
		stages = append(stages, pipeline.ExactDedup{})
	}
	return pipeline.New(pipeline.Options{
		Counter:     counter,
		Stages:      stages,
		Timeout:     time.Duration(cfg.Timeout),
		MaxInFlight: cfg.MaxInFlight,
		Logger:      slog.New(slog.NewTextHandler(stderr, nil)),
	}), nil
}

func version() string {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return "devel"
}
