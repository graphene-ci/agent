package session

import (
	"testing"

	"github.com/graphene-ci/agent/pkg/host"
	"github.com/graphene-ci/pipeline/pkg/id"
)

func TestStoreRoundtrip(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := host.RunContainer{
		MachineId: id.MachineId("vm-1"),
		RunId:     id.RunId("run-1"),
		Image:     host.ImageRef("repo/app:1"),
		Env:       map[string]string{"A": "b"},
	}
	if err := s.Put(c); err != nil {
		t.Fatal(err)
	}

	// A fresh store over the same dir sees the record: restart survival.
	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := s2.Get(c.MachineId, c.RunId)
	if !ok || got.Image != c.Image || got.Env["A"] != "b" {
		t.Fatalf("reloaded record mismatch: %+v ok=%v", got, ok)
	}
	if n := len(s2.List()); n != 1 {
		t.Fatalf("List = %d, want 1", n)
	}

	if err := s2.Delete(c); err != nil {
		t.Fatal(err)
	}
	if _, ok := s2.Get(c.MachineId, c.RunId); ok {
		t.Fatal("record survived Delete")
	}
	// Deleting again is a no-op, not an error.
	if err := s2.Delete(c); err != nil {
		t.Fatal(err)
	}
}
