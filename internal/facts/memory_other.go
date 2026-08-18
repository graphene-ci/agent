//go:build !linux

package facts

// totalMemoryBytes is unavailable off Linux; agents run on Linux
// machines, other platforms exist only for development builds.
func totalMemoryBytes() uint64 { return 0 }
