package state

import (
	"path/filepath"
	"testing"
)

func TestInstructionBarrierSurvivesRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	installationID, err := store.InstallationID()
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := store.Reserve("instruction-1", "command")
	if err != nil || !reserved {
		t.Fatalf("reserve = %v, %v", reserved, err)
	}
	if err := store.Activate("instruction-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	secondID, err := reopened.InstallationID()
	if err != nil {
		t.Fatal(err)
	}
	if secondID != installationID {
		t.Fatalf("installation id changed: %q != %q", secondID, installationID)
	}
	reserved, err = reopened.Reserve("instruction-1", "command")
	if err != nil {
		t.Fatal(err)
	}
	if reserved {
		t.Fatal("duplicate instruction was reserved")
	}
	record, found, err := reopened.Record("instruction-1")
	if err != nil || !found {
		t.Fatalf("record = %#v, %v, %v", record, found, err)
	}
	if record.Status != StatusUnknown {
		t.Fatalf("status = %q", record.Status)
	}
}
