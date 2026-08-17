package session

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/graphene-ci/agent/internal/config"
)

func TestDialBuildsInsecureAndTLSClients(t *testing.T) {
	t.Parallel()
	for _, server := range []config.Server{
		{Address: "passthrough:///insecure", Insecure: true},
		{Address: "passthrough:///tls", ServerName: "localhost"},
	} {
		connection, err := Dial(server, "token")
		if err != nil {
			t.Fatal(err)
		}
		if err := connection.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDialRejectsInvalidCAFile(t *testing.T) {
	t.Parallel()
	for _, path := range []string{filepath.Join(t.TempDir(), "missing"), writeCAFile(t, "not a certificate")} {
		if connection, err := Dial(config.Server{Address: "passthrough:///tls", CAFile: path}, "token"); err == nil {
			_ = connection.Close()
			t.Fatalf("Dial() accepted CA file %q", path)
		}
	}
}

func TestBearerCredentials(t *testing.T) {
	t.Parallel()
	credentials := bearerCredentials{token: "secret", allowInsecure: true}
	metadata, err := credentials.GetRequestMetadata(t.Context())
	if err != nil || metadata["authorization"] != "Bearer secret" || credentials.RequireTransportSecurity() {
		t.Fatalf("metadata = %#v, security = %t, error = %v", metadata, credentials.RequireTransportSecurity(), err)
	}
	credentials.allowInsecure = false
	if !credentials.RequireTransportSecurity() {
		t.Fatal("TLS credentials do not require transport security")
	}
}

func writeCAFile(t *testing.T, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(value)), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
