package runcrt

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func tarball(t *testing.T, entries func(*tar.Writer)) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	entries(tw)
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf
}

func TestUntar(t *testing.T) {
	buf := tarball(t, func(tw *tar.Writer) {
		_ = tw.WriteHeader(&tar.Header{Name: "bin/", Typeflag: tar.TypeDir, Mode: 0o755})
		_ = tw.WriteHeader(&tar.Header{Name: "bin/app", Typeflag: tar.TypeReg, Mode: 0o755, Size: 5})
		_, _ = tw.Write([]byte("hello"))
		_ = tw.WriteHeader(&tar.Header{Name: "bin/link", Typeflag: tar.TypeSymlink, Linkname: "app"})
		_ = tw.WriteHeader(&tar.Header{Name: "bin/hard", Typeflag: tar.TypeLink, Linkname: "bin/app"})
	})
	dst := t.TempDir()
	if err := untar(buf, dst); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dst, "bin", "app")) //nolint:gosec // test reads its own tempdir
	if err != nil || string(raw) != "hello" {
		t.Fatalf("file: %q, %v", raw, err)
	}
	if target, err := os.Readlink(filepath.Join(dst, "bin", "link")); err != nil || target != "app" {
		t.Fatalf("symlink: %q, %v", target, err)
	}
	if raw, err := os.ReadFile(filepath.Join(dst, "bin", "hard")); err != nil || string(raw) != "hello" { //nolint:gosec // test reads its own tempdir
		t.Fatalf("hardlink: %q, %v", raw, err)
	}
}

func TestUntarRejectsEscape(t *testing.T) {
	for _, name := range []string{"../evil", "a/../../evil"} {
		buf := tarball(t, func(tw *tar.Writer) {
			_ = tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: 0})
		})
		if err := untar(buf, t.TempDir()); err == nil {
			t.Fatalf("entry %q accepted", name)
		}
	}
}

func TestUntarRejectsEscapingHardlink(t *testing.T) {
	buf := tarball(t, func(tw *tar.Writer) {
		_ = tw.WriteHeader(&tar.Header{Name: "inner", Typeflag: tar.TypeLink, Linkname: "../outside"})
	})
	if err := untar(buf, t.TempDir()); err == nil {
		t.Fatal("escaping hardlink accepted")
	}
}
