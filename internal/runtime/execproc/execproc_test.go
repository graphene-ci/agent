package execproc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/graphene-ci/agent/pkg/host"
	"github.com/graphene-ci/pipeline/pkg/id"
)

func TestLifecycle(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "worker.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 60\n"), 0o700); err != nil { //nolint:gosec // the script must be executable
		t.Fatal(err)
	}
	rt := New(filepath.Join(dir, "data"))
	c := host.RunContainer{
		AgentId: id.AgentId("vm-1"),
		RunId:   id.RunId("run-1"),
		Image:   host.ImageRef(script),
	}
	ctx := context.Background()

	if err := rt.Pull(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := rt.Start(ctx, c); err != nil {
		t.Fatal(err)
	}
	// Idempotent: starting a running container is a no-op.
	if err := rt.Start(ctx, c); err != nil {
		t.Fatal(err)
	}
	status, err := rt.Status(ctx, c)
	if err != nil || status != host.StatusRunning {
		t.Fatalf("Status = %v, %v; want running", status, err)
	}

	if err := rt.Stop(ctx, c); err != nil {
		t.Fatal(err)
	}
	status, err = rt.Status(ctx, c)
	if err != nil || status != host.StatusStopped {
		t.Fatalf("Status after stop = %v, %v; want stopped", status, err)
	}
	// Idempotent: stopping an absent container is a no-op.
	if err := rt.Stop(ctx, c); err != nil {
		t.Fatal(err)
	}
}

func TestStopKillsProcessTree(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "alive")
	script := filepath.Join(dir, "worker.sh")
	// The child keeps touching the marker; after Stop the touching ends.
	content := "#!/bin/sh\n(while true; do date > " + marker + "; sleep 0.1; done) &\nwait\n"
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil { //nolint:gosec // the script must be executable
		t.Fatal(err)
	}
	rt := New(filepath.Join(dir, "data"))
	c := host.RunContainer{
		AgentId: id.AgentId("vm-1"),
		RunId:   id.RunId("run-tree"),
		Image:   host.ImageRef(script),
	}
	ctx := context.Background()
	if err := rt.Start(ctx, c); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child never wrote the marker")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := rt.Stop(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("process tree still alive after Stop")
	}
}
