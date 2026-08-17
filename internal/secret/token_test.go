package secret

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	token, err := Load(path, func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if token != "secret" {
		t.Fatalf("token = %q", token)
	}
}

func TestLoadRejectsTwoSources(t *testing.T) {
	t.Parallel()
	_, err := Load("/token", func(name string) (string, bool) {
		if name == tokenEnvironment {
			return "secret", true
		}
		return "", false
	})
	if err == nil {
		t.Fatal("expected an error")
	}
}
