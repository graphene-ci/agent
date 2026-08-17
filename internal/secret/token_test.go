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

func TestLoadFromEnvironment(t *testing.T) {
	t.Parallel()
	token, err := Load("", func(name string) (string, bool) {
		return "  secret  ", name == tokenEnvironment
	})
	if err != nil || token != "secret" {
		t.Fatalf("Load() = %q, %v", token, err)
	}
}

func TestLoadRejectsMissingAndInvalidTokens(t *testing.T) {
	t.Parallel()
	missing := filepath.Join(t.TempDir(), "missing")
	empty := filepath.Join(t.TempDir(), "empty")
	large := filepath.Join(t.TempDir(), "large")
	if err := os.WriteFile(empty, []byte(" \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(large, make([]byte, (64<<10)+1), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		file   string
		direct string
		set    bool
	}{
		{},
		{direct: " \n", set: true},
		{file: missing},
		{file: empty},
		{file: large},
	} {
		if _, err := Load(test.file, func(string) (string, bool) { return test.direct, test.set }); err == nil {
			t.Fatalf("Load(%q, %q) succeeded", test.file, test.direct)
		}
	}
}
