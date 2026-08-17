package config

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	koanfenv "github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"
	"github.com/spf13/pflag"
)

const envPrefix = "GRAPHENE_AGENT_"

type Config struct {
	Server   Server
	Auth     Auth
	State    State
	Runtime  Runtime
	Facts    Facts
	Output   Output
	Artifact Artifact
}

type Server struct {
	Address    string
	Insecure   bool
	CAFile     string
	ServerName string
}

type Auth struct {
	TokenFile string
}

type State struct {
	Path string
}

type Runtime struct {
	Shell            string
	WorkingDirectory string
	DefaultTimeout   time.Duration
	Heartbeat        time.Duration
	ReconnectMin     time.Duration
	ReconnectMax     time.Duration
	ShutdownTimeout  time.Duration
	ProbeTimeout     time.Duration
	MaxConcurrent    int
}

type Facts struct {
	AllowSensitive bool
	MaxItems       int
}

type Output struct {
	GlobalPendingBytes uint64
	DrainTimeout       time.Duration
}

type Artifact struct {
	ChunkBytes  int
	DefaultMode uint32
}

type ParseResult struct {
	Config      Config
	ShowVersion bool
}

type rawConfig struct {
	Server struct {
		Address    string `koanf:"address"`
		Insecure   bool   `koanf:"insecure"`
		CAFile     string `koanf:"ca_file"`
		ServerName string `koanf:"server_name"`
	} `koanf:"server"`
	Auth struct {
		TokenFile string `koanf:"token_file"`
	} `koanf:"auth"`
	State struct {
		Path string `koanf:"path"`
	} `koanf:"state"`
	Runtime struct {
		Shell            string `koanf:"shell"`
		WorkingDirectory string `koanf:"working_directory"`
		DefaultTimeout   string `koanf:"default_timeout"`
		Heartbeat        string `koanf:"heartbeat"`
		ReconnectMin     string `koanf:"reconnect_min"`
		ReconnectMax     string `koanf:"reconnect_max"`
		ShutdownTimeout  string `koanf:"shutdown_timeout"`
		ProbeTimeout     string `koanf:"probe_timeout"`
		MaxConcurrent    int    `koanf:"max_concurrent"`
	} `koanf:"runtime"`
	Facts struct {
		AllowSensitive bool `koanf:"allow_sensitive"`
		MaxItems       int  `koanf:"max_items"`
	} `koanf:"facts"`
	Output struct {
		GlobalPendingBytes uint64 `koanf:"global_pending_bytes"`
		DrainTimeout       string `koanf:"drain_timeout"`
	} `koanf:"output"`
	Artifact struct {
		ChunkBytes  int    `koanf:"chunk_bytes"`
		DefaultMode string `koanf:"default_mode"`
	} `koanf:"artifact"`
}

var defaults = map[string]any{
	"server.address":              "127.0.0.1:7443",
	"server.insecure":             false,
	"server.ca_file":              "",
	"server.server_name":          "",
	"auth.token_file":             "",
	"state.path":                  "/var/lib/graphene-agent/state.db",
	"runtime.shell":               "/bin/sh",
	"runtime.working_directory":   "/",
	"runtime.default_timeout":     "30m",
	"runtime.heartbeat":           "10s",
	"runtime.reconnect_min":       "500ms",
	"runtime.reconnect_max":       "30s",
	"runtime.shutdown_timeout":    "10s",
	"runtime.probe_timeout":       "5s",
	"runtime.max_concurrent":      16,
	"facts.allow_sensitive":       false,
	"facts.max_items":             1024,
	"output.global_pending_bytes": uint64(64 << 20),
	"output.drain_timeout":        "5s",
	"artifact.chunk_bytes":        1 << 20,
	"artifact.default_mode":       "0644",
}

var environmentKeys = map[string]string{
	"SERVER_ADDRESS":              "server.address",
	"SERVER_INSECURE":             "server.insecure",
	"SERVER_CA_FILE":              "server.ca_file",
	"SERVER_NAME":                 "server.server_name",
	"AUTH_TOKEN_FILE":             "auth.token_file",
	"STATE_PATH":                  "state.path",
	"SHELL":                       "runtime.shell",
	"WORKING_DIRECTORY":           "runtime.working_directory",
	"DEFAULT_TIMEOUT":             "runtime.default_timeout",
	"HEARTBEAT":                   "runtime.heartbeat",
	"RECONNECT_MIN":               "runtime.reconnect_min",
	"RECONNECT_MAX":               "runtime.reconnect_max",
	"SHUTDOWN_TIMEOUT":            "runtime.shutdown_timeout",
	"PROBE_TIMEOUT":               "runtime.probe_timeout",
	"MAX_CONCURRENT":              "runtime.max_concurrent",
	"FACTS_ALLOW_SENSITIVE":       "facts.allow_sensitive",
	"FACTS_MAX_ITEMS":             "facts.max_items",
	"OUTPUT_GLOBAL_PENDING_BYTES": "output.global_pending_bytes",
	"OUTPUT_DRAIN_TIMEOUT":        "output.drain_timeout",
	"ARTIFACT_CHUNK_BYTES":        "artifact.chunk_bytes",
	"ARTIFACT_DEFAULT_MODE":       "artifact.default_mode",
}

var flagKeys = map[string]string{
	"server":                      "server.address",
	"insecure":                    "server.insecure",
	"ca-file":                     "server.ca_file",
	"tls-server-name":             "server.server_name",
	"token-file":                  "auth.token_file",
	"state-path":                  "state.path",
	"shell":                       "runtime.shell",
	"working-directory":           "runtime.working_directory",
	"default-timeout":             "runtime.default_timeout",
	"heartbeat":                   "runtime.heartbeat",
	"reconnect-min":               "runtime.reconnect_min",
	"reconnect-max":               "runtime.reconnect_max",
	"shutdown-timeout":            "runtime.shutdown_timeout",
	"probe-timeout":               "runtime.probe_timeout",
	"max-concurrent":              "runtime.max_concurrent",
	"facts-allow-sensitive":       "facts.allow_sensitive",
	"facts-max-items":             "facts.max_items",
	"output-global-pending-bytes": "output.global_pending_bytes",
	"output-drain-timeout":        "output.drain_timeout",
	"artifact-chunk-bytes":        "artifact.chunk_bytes",
	"artifact-default-mode":       "artifact.default_mode",
}

func Parse(args []string, lookupEnv func(string) (string, bool), environ func() []string, output io.Writer) (ParseResult, error) {
	flags := newFlagSet(output)
	if err := flags.Parse(args); err != nil {
		return ParseResult{}, err
	}

	k := koanf.New(".")
	if err := k.Load(confmap.Provider(defaults, "."), nil); err != nil {
		return ParseResult{}, fmt.Errorf("load defaults: %w", err)
	}

	configPath, err := flags.GetString("config")
	if err != nil {
		return ParseResult{}, fmt.Errorf("read config flag: %w", err)
	}
	if configPath == "" {
		configPath, _ = lookupEnv(envPrefix + "CONFIG")
	}
	if configPath != "" {
		if err := k.Load(file.Provider(configPath), yaml.Parser()); err != nil {
			return ParseResult{}, fmt.Errorf("load config %q: %w", configPath, err)
		}
	}

	if err := k.Load(koanfenv.Provider(".", koanfenv.Opt{
		Prefix:      envPrefix,
		EnvironFunc: environ,
		TransformFunc: func(name, value string) (string, any) {
			key, ok := environmentKeys[strings.TrimPrefix(name, envPrefix)]
			if !ok {
				return "", nil
			}
			return key, value
		},
	}), nil); err != nil {
		return ParseResult{}, fmt.Errorf("load environment: %w", err)
	}

	if err := k.Load(posflag.ProviderWithFlag(flags, ".", k, func(flag *pflag.Flag) (string, any) {
		key, ok := flagKeys[flag.Name]
		if !ok {
			return "", nil
		}
		return key, posflag.FlagVal(flags, flag)
	}), nil); err != nil {
		return ParseResult{}, fmt.Errorf("load flags: %w", err)
	}

	var raw rawConfig
	if err := k.UnmarshalWithConf("", &raw, koanf.UnmarshalConf{
		Tag: "koanf",
		DecoderConfig: &mapstructure.DecoderConfig{
			ErrorUnused:      true,
			WeaklyTypedInput: true,
		},
	}); err != nil {
		return ParseResult{}, fmt.Errorf("decode config: %w", err)
	}
	cfg, err := normalize(raw)
	if err != nil {
		return ParseResult{}, err
	}

	showVersion, err := flags.GetBool("version")
	if err != nil {
		return ParseResult{}, fmt.Errorf("read version flag: %w", err)
	}

	return ParseResult{Config: cfg, ShowVersion: showVersion}, nil
}

func newFlagSet(output io.Writer) *pflag.FlagSet {
	flags := pflag.NewFlagSet("graphene-agent", pflag.ContinueOnError)
	flags.SetOutput(output)
	flags.String("config", "", "path to YAML configuration")
	flags.Bool("version", false, "print version and exit")
	flags.String("server", defaults["server.address"].(string), "Graphene gRPC address")
	flags.Bool("insecure", false, "use plaintext gRPC (development only)")
	flags.String("ca-file", "", "additional PEM CA bundle")
	flags.String("tls-server-name", "", "TLS server name override")
	flags.String("token-file", "", "path to scoped agent token")
	flags.String("state-path", defaults["state.path"].(string), "durable state database path")
	flags.String("shell", defaults["runtime.shell"].(string), "shell executable")
	flags.String("working-directory", defaults["runtime.working_directory"].(string), "default command directory")
	flags.String("default-timeout", defaults["runtime.default_timeout"].(string), "default command timeout")
	flags.String("heartbeat", defaults["runtime.heartbeat"].(string), "heartbeat interval")
	flags.String("reconnect-min", defaults["runtime.reconnect_min"].(string), "minimum reconnect delay")
	flags.String("reconnect-max", defaults["runtime.reconnect_max"].(string), "maximum reconnect delay")
	flags.String("shutdown-timeout", defaults["runtime.shutdown_timeout"].(string), "graceful shutdown timeout")
	flags.String("probe-timeout", defaults["runtime.probe_timeout"].(string), "per-fact probe timeout")
	flags.Int("max-concurrent", defaults["runtime.max_concurrent"].(int), "maximum active instructions")
	flags.Bool("facts-allow-sensitive", defaults["facts.allow_sensitive"].(bool), "allow explicitly requested sensitive machine facts")
	flags.Int("facts-max-items", defaults["facts.max_items"].(int), "maximum inventory entries per fact group")
	flags.Uint64("output-global-pending-bytes", defaults["output.global_pending_bytes"].(uint64), "global pending command-output limit")
	flags.String("output-drain-timeout", defaults["output.drain_timeout"].(string), "maximum final output drain time")
	flags.Int("artifact-chunk-bytes", defaults["artifact.chunk_bytes"].(int), "artifact transfer chunk size")
	flags.String("artifact-default-mode", defaults["artifact.default_mode"].(string), "default destination mode")
	return flags
}

func normalize(raw rawConfig) (Config, error) {
	parseDuration := func(name, value string) (time.Duration, error) {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return 0, fmt.Errorf("%s must be a positive duration: %q", name, value)
		}
		return duration, nil
	}

	defaultTimeout, err := parseDuration("runtime.default_timeout", raw.Runtime.DefaultTimeout)
	if err != nil {
		return Config{}, err
	}
	heartbeat, err := parseDuration("runtime.heartbeat", raw.Runtime.Heartbeat)
	if err != nil {
		return Config{}, err
	}
	reconnectMin, err := parseDuration("runtime.reconnect_min", raw.Runtime.ReconnectMin)
	if err != nil {
		return Config{}, err
	}
	reconnectMax, err := parseDuration("runtime.reconnect_max", raw.Runtime.ReconnectMax)
	if err != nil {
		return Config{}, err
	}
	shutdownTimeout, err := parseDuration("runtime.shutdown_timeout", raw.Runtime.ShutdownTimeout)
	if err != nil {
		return Config{}, err
	}
	probeTimeout, err := parseDuration("runtime.probe_timeout", raw.Runtime.ProbeTimeout)
	if err != nil {
		return Config{}, err
	}
	drainTimeout, err := parseDuration("output.drain_timeout", raw.Output.DrainTimeout)
	if err != nil {
		return Config{}, err
	}

	mode, err := strconv.ParseUint(raw.Artifact.DefaultMode, 0, 32)
	if err != nil || mode > 0o7777 {
		return Config{}, fmt.Errorf("artifact.default_mode must contain Unix permission bits: %q", raw.Artifact.DefaultMode)
	}

	cfg := Config{
		Server:   Server{Address: strings.TrimSpace(raw.Server.Address), Insecure: raw.Server.Insecure, CAFile: raw.Server.CAFile, ServerName: raw.Server.ServerName},
		Auth:     Auth{TokenFile: raw.Auth.TokenFile},
		State:    State{Path: raw.State.Path},
		Runtime:  Runtime{Shell: raw.Runtime.Shell, WorkingDirectory: raw.Runtime.WorkingDirectory, DefaultTimeout: defaultTimeout, Heartbeat: heartbeat, ReconnectMin: reconnectMin, ReconnectMax: reconnectMax, ShutdownTimeout: shutdownTimeout, ProbeTimeout: probeTimeout, MaxConcurrent: raw.Runtime.MaxConcurrent},
		Facts:    Facts{AllowSensitive: raw.Facts.AllowSensitive, MaxItems: raw.Facts.MaxItems},
		Output:   Output{GlobalPendingBytes: raw.Output.GlobalPendingBytes, DrainTimeout: drainTimeout},
		Artifact: Artifact{ChunkBytes: raw.Artifact.ChunkBytes, DefaultMode: uint32(mode)},
	}

	switch {
	case cfg.Server.Address == "":
		return Config{}, errors.New("server.address is required")
	case cfg.State.Path == "":
		return Config{}, errors.New("state.path is required")
	case !filepath.IsAbs(cfg.State.Path):
		return Config{}, errors.New("state.path must be absolute")
	case cfg.Runtime.Shell == "":
		return Config{}, errors.New("runtime.shell is required")
	case !filepath.IsAbs(cfg.Runtime.Shell):
		return Config{}, errors.New("runtime.shell must be absolute")
	case cfg.Runtime.WorkingDirectory == "":
		return Config{}, errors.New("runtime.working_directory is required")
	case !filepath.IsAbs(cfg.Runtime.WorkingDirectory):
		return Config{}, errors.New("runtime.working_directory must be absolute")
	case cfg.Runtime.ReconnectMin > cfg.Runtime.ReconnectMax:
		return Config{}, errors.New("runtime.reconnect_min must not exceed runtime.reconnect_max")
	case cfg.Runtime.MaxConcurrent <= 0:
		return Config{}, errors.New("runtime.max_concurrent must be positive")
	case cfg.Facts.MaxItems <= 0 || cfg.Facts.MaxItems > 65536:
		return Config{}, errors.New("facts.max_items must be between 1 and 65536")
	case cfg.Output.GlobalPendingBytes == 0:
		return Config{}, errors.New("output.global_pending_bytes must be positive")
	case cfg.Artifact.ChunkBytes <= 0 || cfg.Artifact.ChunkBytes > 16<<20:
		return Config{}, errors.New("artifact.chunk_bytes must be between 1 and 16777216")
	case cfg.Server.Insecure && (cfg.Server.CAFile != "" || cfg.Server.ServerName != ""):
		return Config{}, errors.New("server.ca_file and server.server_name cannot be used with server.insecure")
	}

	return cfg, nil
}
