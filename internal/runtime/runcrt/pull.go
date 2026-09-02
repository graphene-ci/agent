package runcrt

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

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
	img, err := remote.Image(ref, opts...)
	if err != nil {
		return fmt.Errorf("fetch image: %w", err)
	}
	// Progress from the manifest: layer count and total compressed size,
	// the header docker prints before the bars.
	if m, merr := img.Manifest(); merr == nil {
		var total int64
		for _, l := range m.Layers {
			total += l.Size
		}
		r.op(c, "pull", fmt.Sprintf("%d layers, %s to download", len(m.Layers), humanBytes(total)))
	}
	if err := r.unpack(img, dir, func(n int64) { r.op(c, "pull", "downloaded "+humanBytes(n)) }); err != nil {
		return err
	}
	r.op(c, "pull", "pulled "+string(image))
	return nil
}

// countingReader tallies bytes read and reports the running total no more
// often than `every` — steady pull progress without a line per read.
type countingReader struct {
	r      io.Reader
	n      int64
	last   time.Time
	every  time.Duration
	report func(int64)
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	if time.Since(c.last) >= c.every {
		c.last = time.Now()
		c.report(c.n)
	}
	return n, err
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

func (r *Runtime) unpack(img v1.Image, dir string, progress func(n int64)) error {
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
	// mutate.Extract streams the decompressed layers; wrap it in a
	// time-throttled byte counter so the pull reports steady progress
	// (docker-style) instead of a silent block that reads as "stuck".
	flat := mutate.Extract(img)
	defer func() { _ = flat.Close() }()
	var src io.Reader = flat
	if progress != nil {
		src = &countingReader{r: flat, every: time.Second, report: progress}
	}
	if err := untar(src, rootfs); err != nil {
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
