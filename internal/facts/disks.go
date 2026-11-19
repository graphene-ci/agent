package facts

import (
	"os"
	"path"
	"slices"
	"strconv"
	"strings"

	agentpb "github.com/graphene-ci/agent/pkg/proto/agent/v1"
)

const (
	// maxDisks bounds the report: facts ride every Hello.
	maxDisks = 64
	// maxDiskIDs bounds the by-id names kept per disk.
	maxDiskIDs = 8
	// sectorBytes is the unit /sys/block/<dev>/size counts in, whatever
	// the device's own sector size.
	sectorBytes = 512
)

// collectDisks reports the physical block devices of the filesystem at
// root ("/" on the machine). A device without a "device" link — loop,
// ram, zram, device-mapper, md — is virtual and not a fact about the box.
func collectDisks(root string) []*agentpb.Facts_Disk {
	r, err := os.OpenRoot(root)
	if err != nil {
		return nil
	}
	defer func() { _ = r.Close() }()
	names := readDir(r, "sys/block")
	byID := diskIDs(r)
	var out []*agentpb.Facts_Disk
	for _, name := range names {
		dev := path.Join("sys/block", name)
		if _, err := r.Stat(path.Join(dev, "device")); err != nil {
			continue
		}
		out = append(out, &agentpb.Facts_Disk{
			Name:       name,
			SizeBytes:  sectors(r, path.Join(dev, "size")) * sectorBytes,
			Rotational: readTrim(r, path.Join(dev, "queue/rotational")) == "1",
			Transport:  transport(r, name, dev),
			Model:      readTrim(r, path.Join(dev, "device/model")),
			Serial:     firstNonEmpty(readTrim(r, path.Join(dev, "device/serial")), readTrim(r, path.Join(dev, "serial"))),
			ById:       byID[name],
		})
		if len(out) == maxDisks {
			break
		}
	}
	return out
}

// readDir lists a directory's entry names, sorted; nil when unreadable.
func readDir(r *os.Root, dir string) []string {
	f, err := r.Open(dir)
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()
	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil
	}
	slices.Sort(names)
	return names
}

// diskIDs maps a kernel disk name to the /dev/disk/by-id names pointing
// at the whole disk (partitions are skipped).
func diskIDs(r *os.Root) map[string][]string {
	out := map[string][]string{}
	for _, id := range readDir(r, "dev/disk/by-id") {
		if strings.Contains(id, "-part") {
			continue
		}
		target, err := r.Readlink(path.Join("dev/disk/by-id", id))
		if err != nil {
			continue
		}
		name := path.Base(target)
		if len(out[name]) < maxDiskIDs {
			out[name] = append(out[name], id)
		}
	}
	return out
}

// transport names the bus: by the kernel name where it is certain, else by
// the subsystem the device sits on.
func transport(r *os.Root, name, dev string) string {
	switch {
	case strings.HasPrefix(name, "nvme"):
		return "nvme"
	case strings.HasPrefix(name, "vd"):
		return "virtio"
	}
	target, err := r.Readlink(path.Join(dev, "device/subsystem"))
	if err != nil {
		return ""
	}
	return path.Base(target)
}

func sectors(r *os.Root, name string) uint64 {
	n, err := strconv.ParseUint(readTrim(r, name), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func readTrim(r *os.Root, name string) string {
	b, err := r.ReadFile(name)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
