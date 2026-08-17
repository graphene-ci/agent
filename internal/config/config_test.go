package config

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParsePrecedence(t *testing.T) {
	t.Parallel()
	configPath := filepath.Join(t.TempDir(), "agent.yaml")
	content := []byte("server:\n  address: file:443\nstate:\n  path: /from/file.db\nruntime:\n  max_concurrent: 3\nartifact:\n  default_mode: \"0600\"\n")
	if err := os.WriteFile(configPath, content, 0o600); err != nil {
		t.Fatal(err)
	}
	environment := map[string]string{
		"GRAPHENE_AGENT_CONFIG":                configPath,
		"GRAPHENE_AGENT_SERVER_ADDRESS":        "env:443",
		"GRAPHENE_AGENT_HEARTBEAT":             "2s",
		"GRAPHENE_AGENT_FACTS_ALLOW_SENSITIVE": "true",
	}
	lookup := func(name string) (string, bool) { value, ok := environment[name]; return value, ok }
	environ := func() []string {
		values := make([]string, 0, len(environment))
		for name, value := range environment {
			values = append(values, name+"="+value)
		}
		return values
	}
	result, err := Parse([]string{"--server=flag:443", "--max-concurrent=7", "--facts-max-items=23"}, lookup, environ, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Config.Server.Address != "flag:443" {
		t.Fatalf("server address = %q", result.Config.Server.Address)
	}
	if result.Config.State.Path != "/from/file.db" {
		t.Fatalf("state path = %q", result.Config.State.Path)
	}
	if result.Config.Runtime.MaxConcurrent != 7 {
		t.Fatalf("max concurrent = %d", result.Config.Runtime.MaxConcurrent)
	}
	if result.Config.Runtime.Heartbeat != 2*time.Second {
		t.Fatalf("heartbeat = %s", result.Config.Runtime.Heartbeat)
	}
	if result.Config.Artifact.DefaultMode != 0o600 {
		t.Fatalf("mode = %#o", result.Config.Artifact.DefaultMode)
	}
	if !result.Config.Facts.AllowSensitive || result.Config.Facts.MaxItems != 23 {
		t.Fatalf("facts config = %#v", result.Config.Facts)
	}
}

func TestParseRejectsAmbiguousTLS(t *testing.T) {
	t.Parallel()
	_, err := Parse([]string{"--insecure", "--ca-file=/tmp/ca.pem"}, func(string) (string, bool) { return "", false }, func() []string { return nil }, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestParseRejectsUnknownYAMLKey(t *testing.T) {
	t.Parallel()
	configPath := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(configPath, []byte("runtime:\n  heartbet: 2s\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Parse([]string{"--config", configPath}, func(string) (string, bool) { return "", false }, func() []string { return nil }, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected an unknown-key error")
	}
}

func TestParseRejectsUnboundedFacts(t *testing.T) {
	t.Parallel()
	_, err := Parse([]string{"--facts-max-items=0"}, func(string) (string, bool) { return "", false }, func() []string { return nil }, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected an invalid facts limit error")
	}
}
