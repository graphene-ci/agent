//go:build !linux

package runcrt

import "errors"

// The runc runtime is Linux-only; other platforms build for development
// and use the exec runtime instead.

func mountOverlay(_, _, _, _ string) error {
	return errors.New("runc runtime requires linux")
}

func unmountOverlay(_ string) error {
	return errors.New("runc runtime requires linux")
}
