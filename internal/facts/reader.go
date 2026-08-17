package facts

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
)

const (
	// SchemaVersion is the facts schema advertised in Hello.
	SchemaVersion = "1"
	defaultLimit  = 1024
	messageLimit  = 512
)

// Config controls bounded fact collection and sensitive-field policy.
type Config struct {
	Timeout        time.Duration
	AllowSensitive bool
	MaxItems       int
}

type collector func(context.Context, bool, int) *agentpb.FactGroupResult

// Reader collects typed inventory groups without executing external programs.
type Reader struct {
	config     Config
	collectors map[agentpb.FactGroup]collector
}

// New creates a fact reader backed by operating-system APIs and ghw.
func New(config Config) *Reader {
	if config.MaxItems <= 0 {
		config.MaxItems = defaultLimit
	}
	return &Reader{
		config: config,
		collectors: map[agentpb.FactGroup]collector{
			agentpb.FactGroup_FACT_GROUP_OPERATING_SYSTEM:      collectOperatingSystem,
			agentpb.FactGroup_FACT_GROUP_COMPUTE:               collectCompute,
			agentpb.FactGroup_FACT_GROUP_MEMORY:                collectMemory,
			agentpb.FactGroup_FACT_GROUP_HARDWARE:              collectHardware,
			agentpb.FactGroup_FACT_GROUP_STORAGE:               collectStorage,
			agentpb.FactGroup_FACT_GROUP_NETWORK:               collectNetwork,
			agentpb.FactGroup_FACT_GROUP_SECURITY:              collectSecurity,
			agentpb.FactGroup_FACT_GROUP_EXECUTION_ENVIRONMENT: collectExecutionEnvironment,
		},
	}
}

// SupportedGroups returns the stable sorted group list advertised in Hello.
func (r *Reader) SupportedGroups() []agentpb.FactGroup {
	groups := make([]agentpb.FactGroup, 0, len(r.collectors))
	for group := range r.collectors {
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i] < groups[j] })
	return groups
}

// SensitiveAllowed reports whether local policy permits sensitive fields.
func (r *Reader) SensitiveAllowed() bool {
	return r.config.AllowSensitive
}

// Read collects every distinct selected group in stable numeric order.
func (r *Reader) Read(ctx context.Context, request *agentpb.ReadFacts) []*agentpb.FactGroupResult {
	groups := request.GetGroups()
	if len(groups) == 0 {
		groups = r.SupportedGroups()
	}
	groups = distinctSorted(groups)
	results := make([]*agentpb.FactGroupResult, 0, len(groups))
	includeSensitive := request.GetIncludeSensitive() && r.config.AllowSensitive
	for _, group := range groups {
		probe, ok := r.collectors[group]
		if !ok {
			results = append(results, result(group, agentpb.FactStatus_FACT_STATUS_UNSUPPORTED, "fact group is not supported"))
			continue
		}
		results = append(results, r.run(ctx, group, probe, includeSensitive))
	}
	return results
}

func (r *Reader) run(ctx context.Context, group agentpb.FactGroup, probe collector, includeSensitive bool) *agentpb.FactGroupResult {
	probeCtx := ctx
	cancel := func() {}
	if r.config.Timeout > 0 {
		probeCtx, cancel = context.WithTimeout(ctx, r.config.Timeout)
	}
	defer cancel()

	completed := make(chan *agentpb.FactGroupResult, 1)
	go func() { completed <- probe(probeCtx, includeSensitive, r.config.MaxItems) }()
	select {
	case collected := <-completed:
		if collected == nil {
			return result(group, agentpb.FactStatus_FACT_STATUS_ERROR, "fact collector returned no result")
		}
		collected.Group = group
		collected.Message = boundedMessage(collected.GetMessage())
		return collected
	case <-probeCtx.Done():
		status := agentpb.FactStatus_FACT_STATUS_ERROR
		if errors.Is(probeCtx.Err(), context.DeadlineExceeded) {
			status = agentpb.FactStatus_FACT_STATUS_TIMEOUT
		}
		return result(group, status, probeCtx.Err().Error())
	}
}

func distinctSorted(groups []agentpb.FactGroup) []agentpb.FactGroup {
	seen := make(map[agentpb.FactGroup]struct{}, len(groups))
	result := make([]agentpb.FactGroup, 0, len(groups))
	for _, group := range groups {
		if _, duplicate := seen[group]; duplicate {
			continue
		}
		seen[group] = struct{}{}
		result = append(result, group)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func result(group agentpb.FactGroup, status agentpb.FactStatus, message string) *agentpb.FactGroupResult {
	return &agentpb.FactGroupResult{Group: group, Status: status, Message: boundedMessage(message)}
}

func boundedMessage(message string) string {
	message = strings.TrimSpace(message)
	if len(message) <= messageLimit {
		return message
	}
	return message[:messageLimit]
}

func collectionStatus(collectionErrors []error) (agentpb.FactStatus, string) {
	if len(collectionErrors) == 0 {
		return agentpb.FactStatus_FACT_STATUS_OK, ""
	}
	messages := make([]string, 0, len(collectionErrors))
	for _, err := range collectionErrors {
		messages = append(messages, err.Error())
	}
	return agentpb.FactStatus_FACT_STATUS_PARTIAL, boundedMessage(strings.Join(messages, "; "))
}

func failureResult(err error) *agentpb.FactGroupResult {
	status := agentpb.FactStatus_FACT_STATUS_ERROR
	if errors.Is(err, context.DeadlineExceeded) {
		status = agentpb.FactStatus_FACT_STATUS_TIMEOUT
	} else if errors.Is(err, os.ErrPermission) {
		status = agentpb.FactStatus_FACT_STATUS_PERMISSION_DENIED
	}
	return result(agentpb.FactGroup_FACT_GROUP_UNSPECIFIED, status, fmt.Sprintf("collect facts: %v", err))
}
