package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

const defaultStartupTimeout = 30 * time.Second

type Config struct {
	Version   int             `yaml:"version"`
	Upstreams []Upstream      `yaml:"upstreams"`
	Pipeline  Pipeline        `yaml:"pipeline"`
	Tools     map[string]Tool `yaml:"tools"`
	Store     Store           `yaml:"store"`
	Metrics   Metrics         `yaml:"metrics"`
}

type Upstream struct {
	Name           string            `yaml:"name"`
	Command        string            `yaml:"command"`
	Args           []string          `yaml:"args"`
	Env            map[string]string `yaml:"env"`
	StartupTimeout Duration          `yaml:"startup_timeout"`
}

type Pipeline struct {
	Enabled      bool     `yaml:"enabled"`
	Timeout      Duration `yaml:"timeout"`
	MaxInFlight  int      `yaml:"max_in_flight"`
	BudgetTokens int      `yaml:"budget_tokens"`
	Dedup        Dedup    `yaml:"dedup"`
	Rank         Rank     `yaml:"rank"`
}

type Dedup struct {
	Enabled    bool    `yaml:"enabled"`
	Similarity float64 `yaml:"similarity"`
}

type Rank struct {
	Enabled bool    `yaml:"enabled"`
	K1      float64 `yaml:"k1"`
	B       float64 `yaml:"b"`
}

type Tool struct {
	QueryArgs []string `yaml:"query_args"`
}

type Store struct {
	Path           string `yaml:"path"`
	RetentionDays  int    `yaml:"retention_days"`
	CaptureContent bool   `yaml:"capture_content"`
}

type Metrics struct {
	Listen string `yaml:"listen"`
}

type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
		return fmt.Errorf("must be a duration string")
	}
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("startup_timeout: %w", err)
	}
	*d = Duration(parsed)
	return nil
}

func Load(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: open %q: %w", path, err)
	}
	defer file.Close()

	var cfg Config
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return Config{}, errors.New("config: file is empty")
		}
		if strings.Contains(err.Error(), "must be a duration string") {
			return Config{}, fmt.Errorf("config.upstreams[].startup_timeout: %w", err)
		}
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if len(c.Upstreams) != 1 {
		return fmt.Errorf("config.upstreams: exactly one upstream is required, got %d", len(c.Upstreams))
	}
	upstream := &c.Upstreams[0]
	if upstream.Name == "" {
		return fmt.Errorf("config.upstreams[0].name: required")
	}
	if upstream.Command == "" {
		return fmt.Errorf("config.upstreams[0].command: required")
	}
	if upstream.StartupTimeout == 0 {
		upstream.StartupTimeout = Duration(defaultStartupTimeout)
	}
	if upstream.StartupTimeout < 0 {
		return fmt.Errorf("config.upstreams[0].startup_timeout: must be positive")
	}
	return nil
}
