package facts

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// machine builds a fake sysfs and /dev/disk/by-id under a temporary root.
func machine(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	write := func(p, content string) {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	link := func(target, p string) {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, full); err != nil {
			t.Fatal(err)
		}
	}
	// An NVMe disk: model and serial on the device.
	write("sys/block/nvme0n1/size", "3750748848\n")
	write("sys/block/nvme0n1/queue/rotational", "0\n")
	write("sys/block/nvme0n1/device/model", "SAMSUNG MZQL23T8HCLS-00A07  \n")
	write("sys/block/nvme0n1/device/serial", "S64HNE0R123456\n")
	// A spinning SATA disk on the scsi subsystem.
	write("sys/block/sda/size", "1953525168\n")
	write("sys/block/sda/queue/rotational", "1\n")
	write("sys/block/sda/device/model", "ST1000NM0055\n")
	link("../../../bus/scsi", "sys/block/sda/device/subsystem")
	write("sys/bus/scsi/.keep", "")
	// A virtio disk with its serial on the block device.
	write("sys/block/vdb/size", "209715200\n")
	write("sys/block/vdb/queue/rotational", "1\n")
	write("sys/block/vdb/serial", "epd1234\n")
	write("sys/block/vdb/device/.keep", "")
	// Virtual devices: no device link.
	write("sys/block/loop0/size", "0\n")
	write("sys/block/dm-0/size", "100\n")
	// by-id names, partitions included to be skipped.
	link("../../nvme0n1", "dev/disk/by-id/nvme-SAMSUNG_MZQL23T8HCLS-00A07_S64HNE0R123456")
	link("../../nvme0n1", "dev/disk/by-id/nvme-eui.0025388b11223344")
	link("../../nvme0n1p1", "dev/disk/by-id/nvme-SAMSUNG_MZQL23T8HCLS-00A07_S64HNE0R123456-part1")
	link("../../vdb", "dev/disk/by-id/virtio-epd1234")
	return root
}

func TestCollectDisks(t *testing.T) {
	t.Parallel()
	disks := collectDisks(machine(t))
	type want struct {
		size       uint64
		rotational bool
		transport  string
		model      string
		serial     string
		ids        int
	}
	wants := map[string]want{
		"nvme0n1": {size: 3750748848 * 512, transport: "nvme", model: "SAMSUNG MZQL23T8HCLS-00A07", serial: "S64HNE0R123456", ids: 2},
		"sda":     {size: 1953525168 * 512, rotational: true, transport: "scsi", model: "ST1000NM0055"},
		"vdb":     {size: 209715200 * 512, rotational: true, transport: "virtio", serial: "epd1234", ids: 1},
	}
	if len(disks) != len(wants) {
		t.Fatalf("disks %v, want only the physical ones", disks)
	}
	for _, d := range disks {
		w, ok := wants[d.GetName()]
		if !ok {
			t.Fatalf("unexpected disk %s", d.GetName())
		}
		if d.GetSizeBytes() != w.size || d.GetRotational() != w.rotational || d.GetTransport() != w.transport ||
			d.GetModel() != w.model || d.GetSerial() != w.serial || len(d.GetById()) != w.ids {
			t.Fatalf("%s: got %+v, want %+v", d.GetName(), d, w)
		}
	}
}

// No sysfs (a container, another OS) means no disks, never an error.
func TestCollectDisksWithoutSysfs(t *testing.T) {
	t.Parallel()
	if got := collectDisks(t.TempDir()); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
	if got := collectDisks(filepath.Join(t.TempDir(), "missing")); got != nil {
		t.Fatalf("got %v", got)
	}
}

func TestCollect(t *testing.T) {
	t.Parallel()
	f := Collect()
	if f.GetOs() != runtime.GOOS || f.GetArch() != runtime.GOARCH || f.GetCpus() == 0 {
		t.Fatalf("facts %+v", f)
	}
	for _, d := range f.GetDisks() {
		t.Logf("disk on this machine: %s %d bytes %s rotational=%v model=%q ids=%v", d.GetName(), d.GetSizeBytes(), d.GetTransport(), d.GetRotational(), d.GetModel(), d.GetById())
	}
}

func BenchmarkCollectDisks(b *testing.B) {
	root := machine(b)
	for b.Loop() {
		collectDisks(root)
	}
}
