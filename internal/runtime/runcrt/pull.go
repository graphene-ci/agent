package runcrt

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/graphene-ci/agent/pkg/host"
)

// imageConfig is what Start needs from the image after Pull flattened it.
type imageConfig struct {
	Entrypoint []string `json:"entrypoint,omitempty"`
	Cmd        []string `json:"cmd,omitempty"`
	Env        []string `json:"env,omitempty"`
	WorkingDir string   `json:"workingDir,omitempty"`
}

// Pull fetches the image through the server's registry proxy — the only
// registry the agent knows — and unpacks a flattened rootfs under the
// data dir, keyed by image ref. Present images are not re-fetched.
func (r *Runtime) Pull(ctx context.Context, image host.ImageRef) error {
	if err := image.Validate(); err != nil {
		return err
	}
	dir := r.imageDir(image)
	if _, err := os.Stat(filepath.Join(dir, "config.json")); err == nil {
		return nil
	}

	nameOpts := []name.Option{name.WithDefaultRegistry(r.registry)}
	if r.insecure {
		// TODO(tls): drop once the door serves TLS.
		nameOpts = append(nameOpts, name.Insecure)
	}
	ref, err := name.ParseReference(string(image), nameOpts...)
	if err != nil {
		return fmt.Errorf("image ref: %w", err)
	}
	opts := []remote.Option{remote.WithContext(ctx)}
	if r.token != "" {
		opts = append(opts, remote.WithAuth(&authn.Bearer{Token: r.token}))
	}
	img, err := remote.Image(ref, opts...)
	if err != nil {
		return fmt.Errorf("fetch image: %w", err)
	}
	return r.unpack(img, dir)
}

func (r *Runtime) unpack(img v1.Image, dir string) error {
	cfgFile, err := img.ConfigFile()
	if err != nil {
		return fmt.Errorf("image config: %w", err)
	}
	tmp := dir + ".tmp"
	if err := os.RemoveAll(tmp); err != nil {
		return err
	}
	rootfs := filepath.Join(tmp, "rootfs")
	// The rootfs must stay world-traversable: in-container non-root
	// processes walk it.
	if err := os.MkdirAll(rootfs, 0o755); err != nil { //nolint:gosec // see comment above
		return err
	}
	flat := mutate.Extract(img)
	defer func() { _ = flat.Close() }()
	if err := untar(flat, rootfs); err != nil {
		return fmt.Errorf("unpack rootfs: %w", err)
	}
	cfg := imageConfig{
		Entrypoint: cfgFile.Config.Entrypoint,
		Cmd:        cfgFile.Config.Cmd,
		Env:        cfgFile.Config.Env,
		WorkingDir: cfgFile.Config.WorkingDir,
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(tmp, "config.json"), raw, 0o600); err != nil {
		return err
	}
	// The final rename makes Pull atomic: a crash mid-unpack leaves only
	// a .tmp dir that the next Pull sweeps away.
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return os.Rename(tmp, dir)
}

func (r *Runtime) readImageConfig(image host.ImageRef) (imageConfig, error) {
	var cfg imageConfig
	raw, err := os.ReadFile(filepath.Join(r.imageDir(image), "config.json"))
	if err != nil {
		return cfg, fmt.Errorf("image not pulled: %w", err)
	}
	err = json.Unmarshal(raw, &cfg)
	return cfg, err
}

func (r *Runtime) imageDir(image host.ImageRef) string {
	return filepath.Join(r.dataDir, "images", sanitizeName(string(image)))
}

// sanitizeName flattens an arbitrary identifier into one path- and
// runtime-safe segment.
func sanitizeName(s string) string {
	return strings.Map(func(c rune) rune {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '.':
			return c
		default:
			return '_'
		}
	}, s)
}
