package runcrt

import (
	"fmt"
	"syscall"
)

// mountOverlay mounts lower (the pulled image rootfs, shared and
// read-only) under merged with a per-container upper/work pair.
func mountOverlay(lower, upper, work, merged string) error {
	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lower, upper, work)
	if err := syscall.Mount("overlay", merged, "overlay", 0, opts); err != nil {
		return fmt.Errorf("mount overlay on %s: %w", merged, err)
	}
	return nil
}

// unmountOverlay detaches merged; not-mounted is not an error.
func unmountOverlay(merged string) error {
	err := syscall.Unmount(merged, 0)
	if err == nil || err == syscall.EINVAL || err == syscall.ENOENT {
		// EINVAL: not a mount point (already unmounted).
		return nil
	}
	return err
}
