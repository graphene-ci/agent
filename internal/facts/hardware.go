package facts

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"github.com/jaypipes/ghw"
)

func collectCompute(_ context.Context, _ bool, limit int) *agentpb.FactGroupResult {
	cpuInfo, err := ghw.CPU(ghw.WithDisableTools(), ghw.WithDisableWarnings())
	if err != nil {
		return failureResult(err)
	}
	payload := &agentpb.ComputeFacts{
		PhysicalPackages:  uint32(len(cpuInfo.Processors)),
		PhysicalCores:     cpuInfo.TotalCores,
		LogicalProcessors: cpuInfo.TotalHardwareThreads,
	}
	truncated := len(cpuInfo.Processors) > limit
	processors := cpuInfo.Processors
	if truncated {
		processors = processors[:limit]
	}
	for _, processor := range processors {
		if processor == nil {
			continue
		}
		capabilities := append([]string(nil), processor.Capabilities...)
		sort.Strings(capabilities)
		payload.Processors = append(payload.Processors, &agentpb.Processor{
			Id: strconv.Itoa(processor.ID), Vendor: processor.Vendor, Model: processor.Model,
			PhysicalCores: processor.TotalCores, LogicalProcessors: processor.TotalHardwareThreads,
			Capabilities: capabilities,
		})
	}

	var collectionErrors []error
	topologyInfo, topologyErr := ghw.Topology(ghw.WithDisableTools(), ghw.WithDisableWarnings())
	if topologyErr != nil {
		collectionErrors = append(collectionErrors, fmt.Errorf("topology: %w", topologyErr))
	} else {
		remaining := max(limit-len(payload.Processors), 0)
		if len(topologyInfo.Nodes) > remaining {
			truncated = true
		}
		for index, node := range topologyInfo.Nodes {
			if index >= remaining {
				break
			}
			if node == nil {
				continue
			}
			item := &agentpb.NumaNode{Id: int32(node.ID)}
			if node.Memory != nil {
				item.TotalPhysicalBytes = nonNegative(node.Memory.TotalPhysicalBytes)
				item.TotalUsableBytes = nonNegative(node.Memory.TotalUsableBytes)
			}
			logical := make(map[uint32]struct{})
			for _, core := range node.Cores {
				if core == nil {
					continue
				}
				for _, processorID := range core.LogicalProcessors {
					if processorID >= 0 {
						logical[uint32(processorID)] = struct{}{}
					}
				}
			}
			for processorID := range logical {
				item.LogicalProcessors = append(item.LogicalProcessors, processorID)
			}
			sort.Slice(item.LogicalProcessors, func(i, j int) bool { return item.LogicalProcessors[i] < item.LogicalProcessors[j] })
			for _, cache := range node.Caches {
				if cache == nil {
					continue
				}
				logicalProcessors := append([]uint32(nil), cache.LogicalProcessors...)
				sort.Slice(logicalProcessors, func(i, j int) bool { return logicalProcessors[i] < logicalProcessors[j] })
				item.Caches = append(item.Caches, &agentpb.ProcessorCache{
					Level: uint32(cache.Level), Kind: fmt.Sprint(cache.Type), SizeBytes: cache.SizeBytes,
					LogicalProcessors: logicalProcessors,
				})
			}
			payload.NumaNodes = append(payload.NumaNodes, item)
		}
	}
	status, message := collectionStatus(collectionErrors)
	if truncated && status == agentpb.FactStatus_FACT_STATUS_OK {
		status = agentpb.FactStatus_FACT_STATUS_PARTIAL
	}
	return &agentpb.FactGroupResult{
		Status: status, Message: message, Truncated: truncated,
		Facts: &agentpb.FactGroupResult_Compute{Compute: payload},
	}
}

func collectMemory(_ context.Context, _ bool, _ int) *agentpb.FactGroupResult {
	memoryInfo, err := ghw.Memory(ghw.WithDisableTools(), ghw.WithDisableWarnings())
	if err != nil {
		return failureResult(err)
	}
	pageSizes := append([]uint64(nil), memoryInfo.SupportedPageSizes...)
	sort.Slice(pageSizes, func(i, j int) bool { return pageSizes[i] < pageSizes[j] })
	payload := &agentpb.MemoryFacts{
		TotalPhysicalBytes:      nonNegative(memoryInfo.TotalPhysicalBytes),
		TotalUsableBytes:        nonNegative(memoryInfo.TotalUsableBytes),
		SupportedPageSizesBytes: pageSizes,
	}
	return &agentpb.FactGroupResult{
		Status: agentpb.FactStatus_FACT_STATUS_OK,
		Facts:  &agentpb.FactGroupResult_Memory{Memory: payload},
	}
}

func collectHardware(_ context.Context, sensitive bool, limit int) *agentpb.FactGroupResult {
	payload := &agentpb.HardwareFacts{}
	var collectionErrors []error
	if info, err := ghw.Product(ghw.WithDisableTools(), ghw.WithDisableWarnings()); err != nil {
		collectionErrors = append(collectionErrors, fmt.Errorf("product: %w", err))
	} else {
		payload.Product = &agentpb.ProductInfo{Vendor: info.Vendor, Name: info.Name, Version: info.Version}
		if sensitive {
			payload.Product.SerialNumber = info.SerialNumber
			payload.Product.Uuid = info.UUID
		}
	}
	if info, err := ghw.Chassis(ghw.WithDisableTools(), ghw.WithDisableWarnings()); err != nil {
		collectionErrors = append(collectionErrors, fmt.Errorf("chassis: %w", err))
	} else {
		payload.Chassis = &agentpb.ChassisInfo{
			Type: firstNonEmpty(info.TypeDescription, info.Type), Vendor: info.Vendor, Version: info.Version,
		}
		if sensitive {
			payload.Chassis.SerialNumber = info.SerialNumber
		}
	}
	if info, err := ghw.BIOS(ghw.WithDisableTools(), ghw.WithDisableWarnings()); err != nil {
		collectionErrors = append(collectionErrors, fmt.Errorf("BIOS: %w", err))
	} else {
		payload.Bios = &agentpb.BiosInfo{Vendor: info.Vendor, Version: info.Version, Date: info.Date}
	}
	if info, err := ghw.Baseboard(ghw.WithDisableTools(), ghw.WithDisableWarnings()); err != nil {
		collectionErrors = append(collectionErrors, fmt.Errorf("baseboard: %w", err))
	} else {
		payload.Baseboard = &agentpb.BaseboardInfo{Vendor: info.Vendor, Name: info.Product, Version: info.Version}
		if sensitive {
			payload.Baseboard.SerialNumber = info.SerialNumber
		}
	}

	truncated := false
	if info, err := ghw.PCI(ghw.WithDisableTools(), ghw.WithDisableWarnings()); err != nil {
		collectionErrors = append(collectionErrors, fmt.Errorf("PCI: %w", err))
	} else {
		devices := info.Devices
		if len(devices) > limit {
			devices = devices[:limit]
			truncated = true
		}
		for _, device := range devices {
			if device == nil {
				continue
			}
			item := &agentpb.HardwareDevice{Kind: "pci", Address: device.Address, Driver: device.Driver}
			if device.Class != nil {
				item.Class = device.Class.Name
			}
			if device.Vendor != nil {
				item.VendorId = device.Vendor.ID
				item.VendorName = device.Vendor.Name
			}
			if device.Product != nil {
				item.ProductId = device.Product.ID
				item.ProductName = device.Product.Name
			}
			payload.Devices = append(payload.Devices, item)
		}
	}
	status, message := collectionStatus(collectionErrors)
	if truncated && status == agentpb.FactStatus_FACT_STATUS_OK {
		status = agentpb.FactStatus_FACT_STATUS_PARTIAL
	}
	return &agentpb.FactGroupResult{
		Status: status, Message: message, SensitiveFieldsOmitted: !sensitive,
		Truncated: truncated, Facts: &agentpb.FactGroupResult_Hardware{Hardware: payload},
	}
}

func collectStorage(_ context.Context, sensitive bool, limit int) *agentpb.FactGroupResult {
	blockInfo, err := ghw.Block(ghw.WithDisableTools(), ghw.WithDisableWarnings())
	if err != nil {
		return failureResult(err)
	}
	payload := &agentpb.StorageFacts{TotalSizeBytes: blockInfo.TotalSizeBytes}
	truncated := false
	remaining := limit
	for _, disk := range blockInfo.Disks {
		if disk == nil {
			continue
		}
		if remaining == 0 {
			truncated = true
			break
		}
		remaining--
		item := &agentpb.BlockDevice{
			Name: disk.Name, DriveType: fmt.Sprint(disk.DriveType), Controller: fmt.Sprint(disk.StorageController),
			SizeBytes: disk.SizeBytes, PhysicalBlockSizeBytes: disk.PhysicalBlockSizeBytes,
			Removable: disk.IsRemovable, Vendor: disk.Vendor, Model: disk.Model, NumaNodeId: int32(disk.NUMANodeID),
		}
		if sensitive {
			item.SerialNumber = disk.SerialNumber
			item.Wwn = disk.WWN
		}
		for _, partition := range disk.Partitions {
			if partition == nil {
				continue
			}
			if remaining == 0 {
				truncated = true
				break
			}
			remaining--
			part := &agentpb.Partition{
				Name: partition.Name, Label: partition.Label, FilesystemLabel: partition.FilesystemLabel,
				FilesystemType: partition.Type, MountPoint: partition.MountPoint,
				SizeBytes: partition.SizeBytes, ReadOnly: partition.IsReadOnly,
			}
			if sensitive {
				part.Uuid = partition.UUID
			}
			item.Partitions = append(item.Partitions, part)
		}
		payload.Devices = append(payload.Devices, item)
	}
	status := agentpb.FactStatus_FACT_STATUS_OK
	if truncated {
		status = agentpb.FactStatus_FACT_STATUS_PARTIAL
	}
	return &agentpb.FactGroupResult{
		Status: status, SensitiveFieldsOmitted: !sensitive, Truncated: truncated,
		Facts: &agentpb.FactGroupResult_Storage{Storage: payload},
	}
}

func nonNegative(value int64) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}
