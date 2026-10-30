package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/Viniciuscarvalho/ctxgate/internal/config"
	"github.com/Viniciuscarvalho/ctxgate/internal/mcp"
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

	return mcp.Run(ctx, stdin, stdout, stderr, upstream.Command, upstream.Args, upstream.Env, time.Duration(upstream.StartupTimeout))
}

func version() string {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return "devel"
}
