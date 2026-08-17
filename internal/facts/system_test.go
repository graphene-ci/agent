package facts

import (
	"os"
	"path/filepath"
	"testing"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
)

func TestReadFactBooleanPreservesUnavailableState(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	enabled := filepath.Join(directory, "enabled")
	disabled := filepath.Join(directory, "disabled")
	if err := os.WriteFile(enabled, []byte("1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(disabled, []byte("0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := readFactBoolean(enabled); got != agentpb.FactBoolean_FACT_BOOLEAN_TRUE {
		t.Fatalf("enabled = %s", got)
	}
	if got := readFactBoolean(disabled); got != agentpb.FactBoolean_FACT_BOOLEAN_FALSE {
		t.Fatalf("disabled = %s", got)
	}
	if got := readFactBoolean(filepath.Join(directory, "missing")); got != agentpb.FactBoolean_FACT_BOOLEAN_UNSPECIFIED {
		t.Fatalf("missing = %s", got)
	}
}
