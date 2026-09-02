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

// Pull fetches the container's image through the server's registry proxy —
// the only registry the agent knows — and unpacks a flattened rootfs under
// the data dir, keyed by image ref. Present images are not re-fetched.
// Pull progress streams to obs (stream "pull") so `graphenectl logs
// run/<id>` shows the download as a person would see `docker pull` — no
// more guessing whether a slow run is pulling or hung.
func (r *Runtime) Pull(ctx context.Context, c host.RunContainer) error {
	image := c.Image
	if err := image.Validate(); err != nil {
		return err
	}
	dir := r.imageDir(image)
	if _, err := os.Stat(filepath.Join(dir, "config.json")); err == nil {
		r.op(c, "pull", "image "+string(image)+" already present, skipping pull")
		return nil
	}
	r.op(c, "pull", "pulling "+string(image))

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
	// Progress: go-containerregistry reports bytes complete/total on a
	// channel; render it as periodic "downloading N% (done/total)" lines,
	// the pull's equivalent of docker's per-layer bars.
	updates := make(chan v1.Update, 8)
	opts = append(opts, remote.WithProgress(updates))
	done := make(chan struct{})
	go func() {
		defer close(done)
		var lastPct int64 = -1
		for u := range updates {
			if u.Error != nil {
				r.op(c, "pull", "download error: "+u.Error.Error())
				continue
			}
			if u.Total <= 0 {
				continue
			}
			pct := u.Complete * 100 / u.Total
			// Throttle to whole-percent steps: a byte-level channel is far
			// too chatty for a log.
			if pct != lastPct {
				lastPct = pct
				r.op(c, "pull", fmt.Sprintf("downloading %3d%% (%s / %s)", pct, humanBytes(u.Complete), humanBytes(u.Total)))
			}
		}
	}()
	img, err := remote.Image(ref, opts...)
	if err != nil {
		<-done
		return fmt.Errorf("fetch image: %w", err)
	}
	<-done
	r.op(c, "pull", "download complete, unpacking rootfs")
	if err := r.unpack(img, dir); err != nil {
		return err
	}
	r.op(c, "pull", "pulled "+string(image))
	return nil
}

// humanBytes renders a byte count like docker does.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGT"[exp])
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
