// Package config holds the agent's startup configuration. The agent is
// configured by its install script through a small env file — flags are
// for a person debugging, env is the real path.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/graphene-ci/pipeline/pkg/id"
)

// Environment variable names the install script writes.
const (
	EnvServer   = "GRAPHENE_AGENT_SERVER"   // gRPC address of the graphene server
	EnvToken    = "GRAPHENE_AGENT_TOKEN"    //nolint:gosec // the env var NAME, not a credential
	EnvAgentId  = "GRAPHENE_AGENT_ID"       // machine record this agent embodies
	EnvInsecure = "GRAPHENE_AGENT_INSECURE" // "1" disables TLS (dev only)
	EnvCAFile   = "GRAPHENE_AGENT_CA_FILE"  // extra CA bundle for the server cert
	EnvDataDir  = "GRAPHENE_AGENT_DATA_DIR" // container bundles and images live here
	EnvRuntime  = "GRAPHENE_AGENT_RUNTIME"  // "runc" (default) or "exec" (dev)
	EnvRegistry = "GRAPHENE_AGENT_REGISTRY" // host[:port] of the server's registry proxy
)

// Config is everything the agent needs to run.
type Config struct {
	Server   string
	Token    string
	AgentId  id.AgentId
	Insecure bool
	CAFile   string
	DataDir  string
	Runtime  string
	Registry string
	// Reconnect tunes the outbound-connection retry loop.
	ReconnectMin time.Duration
	ReconnectMax time.Duration
}

// FromEnv reads the configuration; a missing required variable is an
// error, not a default.
func FromEnv() (Config, error) {
	cfg := Config{
		Server:       os.Getenv(EnvServer),
		Token:        os.Getenv(EnvToken),
		CAFile:       os.Getenv(EnvCAFile),
		DataDir:      os.Getenv(EnvDataDir),
		Runtime:      os.Getenv(EnvRuntime),
		Registry:     os.Getenv(EnvRegistry),
		ReconnectMin: time.Second,
		ReconnectMax: time.Minute,
	}
	if cfg.Server == "" {
		return cfg, errors.New(EnvServer + " is required")
	}
	if cfg.Token == "" {
		return cfg, errors.New(EnvToken + " is required")
	}
	mid, err := id.ParseAgentId(os.Getenv(EnvAgentId))
	if err != nil {
		return cfg, fmt.Errorf("%s: %w", EnvAgentId, err)
	}
	cfg.AgentId = mid
	if v := os.Getenv(EnvInsecure); v != "" {
		insecure, err := strconv.ParseBool(v)
		if err != nil {
			return cfg, fmt.Errorf("%s: %w", EnvInsecure, err)
		}
		cfg.Insecure = insecure
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "/var/lib/graphene-agent"
	}
	if cfg.Runtime == "" {
		cfg.Runtime = "runc"
	}
	return cfg, nil
}
