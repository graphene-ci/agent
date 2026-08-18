package runcrt

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// untar extracts a flattened image tar into dst. Entry names are
// confined to dst — an absolute or escaping path is an error, not a
// write outside the rootfs.
func untar(r io.Reader, dst string) error {
	tr := tar.NewReader(r)
	// Directory modes are applied after their contents: a read-only
	// directory written first would reject its own children.
	type dirMode struct {
		path string
		mode os.FileMode
	}
	var dirs []dirMode
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		target, err := confine(dst, hdr.Name)
		if err != nil {
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			// 0o755 everywhere below: the rootfs must stay world-traversable
			// for in-container non-root processes.
			if err := os.MkdirAll(target, 0o755); err != nil { //nolint:gosec // see comment above
				return err
			}
			dirs = append(dirs, dirMode{target, hdr.FileInfo().Mode().Perm()})
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil { //nolint:gosec // world-traversable rootfs
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, hdr.FileInfo().Mode().Perm()) //nolint:gosec // target is confined above
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil { //nolint:gosec // image size is bounded by the pull, not per file
				_ = f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		case tar.TypeSymlink:
			// The link target is kept verbatim (it resolves inside the
			// container); only the link's own location is confined.
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil { //nolint:gosec // world-traversable rootfs
				return err
			}
			if err := os.RemoveAll(target); err != nil {
				return err
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		case tar.TypeLink:
			source, err := confine(dst, hdr.Linkname)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil { //nolint:gosec // world-traversable rootfs
				return err
			}
			if err := os.RemoveAll(target); err != nil {
				return err
			}
			if err := os.Link(source, target); err != nil {
				return err
			}
		default:
			// Devices, FIFOs and the rest are skipped: the runtime mounts
			// /dev itself, images have no business shipping device nodes.
			continue
		}
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := os.Chmod(dirs[i].path, dirs[i].mode); err != nil {
			return err
		}
	}
	return nil
}

// confine joins name to root, rejecting escapes.
func confine(root, tarPath string) (string, error) {
	rel := filepath.Clean(strings.TrimPrefix(tarPath, "/"))
	if rel == "." {
		return root, nil
	}
	if !filepath.IsLocal(rel) {
		return "", fmt.Errorf("tar entry escapes rootfs: %q", tarPath)
	}
	return filepath.Join(root, rel), nil
}
