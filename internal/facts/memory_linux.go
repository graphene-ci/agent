package facts

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// totalMemoryBytes reads MemTotal from /proc/meminfo; 0 when unreadable.
func totalMemoryBytes() uint64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kb, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0
			}
			return kb * 1024
		}
	}
	if err := scanner.Err(); err != nil {
		return 0
	}
	return 0
}
