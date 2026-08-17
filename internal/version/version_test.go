package version

import "testing"

func TestValueUsesInjectedVersion(t *testing.T) {
	previous := value
	value = "v1.2.3"
	t.Cleanup(func() { value = previous })
	if got := Value(); got != "v1.2.3" {
		t.Fatalf("Value() = %q", got)
	}
}
