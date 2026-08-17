package version

import (
	"runtime/debug"
)

var value = "dev"

func Value() string {
	if value != "dev" {
		return value
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return value
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" && setting.Value != "" {
			return setting.Value
		}
	}
	return value
}
