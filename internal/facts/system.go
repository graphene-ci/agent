package facts

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/elastic/go-sysinfo"
	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"github.com/shirou/gopsutil/v4/host"
	"golang.org/x/sys/unix"
)

func collectOperatingSystem(_ context.Context, sensitive bool, _ int) *agentpb.FactGroupResult {
	payload := &agentpb.OperatingSystemFacts{}
	var collectionErrors []error

	system, err := sysinfo.Host()
	if err != nil {
		collectionErrors = append(collectionErrors, fmt.Errorf("host info: %w", err))
	} else {
		info := system.Info()
		if info.OS != nil {
			payload.Id = info.OS.Platform
			payload.Name = info.OS.Name
			payload.Version = info.OS.Version
			payload.BuildId = info.OS.Build
		}
		payload.KernelRelease = info.KernelVersion
	}

	if release, err := osRelease(); err == nil {
		payload.Id = firstNonEmpty(release["ID"], payload.GetId())
		payload.Name = firstNonEmpty(release["NAME"], payload.GetName())
		payload.PrettyName = release["PRETTY_NAME"]
		payload.VersionId = release["VERSION_ID"]
		payload.Version = firstNonEmpty(release["VERSION"], payload.GetVersion())
		payload.BuildId = firstNonEmpty(release["BUILD_ID"], payload.GetBuildId())
	} else {
		collectionErrors = append(collectionErrors, fmt.Errorf("os release: %w", err))
	}

	var name unix.Utsname
	if err := unix.Uname(&name); err != nil {
		collectionErrors = append(collectionErrors, fmt.Errorf("kernel: %w", err))
	} else {
		payload.KernelRelease = unix.ByteSliceToString(name.Release[:])
		payload.KernelVersion = unix.ByteSliceToString(name.Version[:])
	}

	omitted := !sensitive
	if sensitive {
		bootID, err := readTrimmed("/proc/sys/kernel/random/boot_id")
		if err != nil {
			collectionErrors = append(collectionErrors, fmt.Errorf("boot id: %w", err))
		} else {
			payload.BootId = bootID
		}
	}

	status, message := collectionStatus(collectionErrors)
	return &agentpb.FactGroupResult{
		Status: status, Message: message, SensitiveFieldsOmitted: omitted,
		Facts: &agentpb.FactGroupResult_OperatingSystem{OperatingSystem: payload},
	}
}

func collectNetwork(_ context.Context, sensitive bool, limit int) *agentpb.FactGroupResult {
	interfaces, err := net.Interfaces()
	if err != nil {
		return failureResult(err)
	}
	payload := &agentpb.NetworkFacts{}
	truncated := len(interfaces) > limit
	if truncated {
		interfaces = interfaces[:limit]
	}
	var collectionErrors []error
	for _, networkInterface := range interfaces {
		item := &agentpb.NetworkInterface{
			Index: uint32(networkInterface.Index),
			Name:  networkInterface.Name,
			Mtu:   uint32(max(networkInterface.MTU, 0)),
		}
		if flags := networkInterface.Flags.String(); flags != "" {
			item.Flags = strings.Split(flags, "|")
		}
		if sensitive {
			item.HardwareAddress = networkInterface.HardwareAddr.String()
			addresses, addressErr := networkInterface.Addrs()
			if addressErr != nil {
				collectionErrors = append(collectionErrors, fmt.Errorf("interface %s addresses: %w", networkInterface.Name, addressErr))
			} else {
				for _, address := range addresses {
					item.Addresses = append(item.Addresses, address.String())
				}
			}
		}
		payload.Interfaces = append(payload.Interfaces, item)
	}
	status, message := collectionStatus(collectionErrors)
	if truncated && status == agentpb.FactStatus_FACT_STATUS_OK {
		status = agentpb.FactStatus_FACT_STATUS_PARTIAL
	}
	return &agentpb.FactGroupResult{
		Status: status, Message: message, SensitiveFieldsOmitted: !sensitive,
		Truncated: truncated, Facts: &agentpb.FactGroupResult_Network{Network: payload},
	}
}

func collectSecurity(_ context.Context, _ bool, _ int) *agentpb.FactGroupResult {
	payload := &agentpb.SecurityFacts{}
	var collectionErrors []error

	if value, err := readTrimmed("/sys/fs/selinux/enforce"); err == nil {
		if value == "1" {
			payload.Selinux = "enforcing"
		} else {
			payload.Selinux = "permissive"
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		collectionErrors = append(collectionErrors, fmt.Errorf("SELinux: %w", err))
	} else {
		payload.Selinux = "disabled"
	}

	if value, err := readTrimmed("/sys/module/apparmor/parameters/enabled"); err == nil {
		if strings.EqualFold(value, "Y") {
			payload.Apparmor = "enabled"
		} else {
			payload.Apparmor = "disabled"
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		collectionErrors = append(collectionErrors, fmt.Errorf("AppArmor: %w", err))
	} else {
		payload.Apparmor = "disabled"
	}

	if value, err := readTrimmed("/sys/kernel/security/lockdown"); err == nil {
		payload.KernelLockdown = selectedBracketValue(value)
	} else if !errors.Is(err, os.ErrNotExist) {
		collectionErrors = append(collectionErrors, fmt.Errorf("kernel lockdown: %w", err))
	}
	payload.FipsEnabledState = readFactBoolean("/proc/sys/crypto/fips_enabled")
	payload.UnprivilegedUserNamespacesState = readFactBoolean("/proc/sys/kernel/unprivileged_userns_clone")

	status, message := collectionStatus(collectionErrors)
	return &agentpb.FactGroupResult{
		Status: status, Message: message,
		Facts: &agentpb.FactGroupResult_Security{Security: payload},
	}
}

func collectExecutionEnvironment(ctx context.Context, _ bool, _ int) *agentpb.FactGroupResult {
	payload := &agentpb.ExecutionEnvironmentFacts{Scope: "host"}
	var collectionErrors []error
	virtualizationSystem, virtualizationRole, err := host.VirtualizationWithContext(ctx)
	if err != nil {
		collectionErrors = append(collectionErrors, fmt.Errorf("virtualization: %w", err))
	} else {
		payload.VirtualizationSystem = virtualizationSystem
		payload.VirtualizationRole = virtualizationRole
	}

	containerized := false
	if system, hostErr := sysinfo.Host(); hostErr == nil {
		if value := system.Info().Containerized; value != nil {
			containerized = *value
		}
	} else {
		collectionErrors = append(collectionErrors, fmt.Errorf("container state: %w", hostErr))
	}
	payload.Namespaced = containerized || namespaceDiffers("mnt") || namespaceDiffers("pid")
	if containerized || isContainerVirtualization(virtualizationSystem) {
		payload.Scope = "container"
	} else if virtualizationRole == "guest" {
		payload.Scope = "virtual-machine"
	}

	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err == nil {
		payload.CgroupVersion = "2"
	} else if _, err := os.Stat("/proc/cgroups"); err == nil {
		payload.CgroupVersion = "1"
	}

	status, message := collectionStatus(collectionErrors)
	return &agentpb.FactGroupResult{
		Status: status, Message: message,
		Facts: &agentpb.FactGroupResult_ExecutionEnvironment{ExecutionEnvironment: payload},
	}
}

func osRelease() (map[string]string, error) {
	file, err := os.Open("/etc/os-release")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), "=")
		if !found {
			continue
		}
		unquoted, unquoteErr := strconv.Unquote(value)
		if unquoteErr == nil {
			value = unquoted
		}
		values[key] = value
	}
	return values, scanner.Err()
}

func readTrimmed(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func readFactBoolean(path string) agentpb.FactBoolean {
	value, err := readTrimmed(path)
	if err != nil {
		return agentpb.FactBoolean_FACT_BOOLEAN_UNSPECIFIED
	}
	if value == "1" || strings.EqualFold(value, "Y") || strings.EqualFold(value, "true") {
		return agentpb.FactBoolean_FACT_BOOLEAN_TRUE
	}
	return agentpb.FactBoolean_FACT_BOOLEAN_FALSE
}

func namespaceDiffers(namespace string) bool {
	self, selfErr := os.Readlink("/proc/self/ns/" + namespace)
	init, initErr := os.Readlink("/proc/1/ns/" + namespace)
	return selfErr == nil && initErr == nil && self != init
}

func selectedBracketValue(value string) string {
	for _, field := range strings.Fields(value) {
		if strings.HasPrefix(field, "[") && strings.HasSuffix(field, "]") {
			return strings.Trim(field, "[]")
		}
	}
	return value
}

func isContainerVirtualization(system string) bool {
	switch system {
	case "docker", "containerd", "lxc", "lxc-libvirt", "openvz", "podman", "rkt", "systemd-nspawn", "wsl":
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
