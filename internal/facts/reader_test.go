package facts

import (
	"context"
	"testing"
	"time"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
)

func TestReadSelectsDistinctGroupsInStableOrder(t *testing.T) {
	t.Parallel()
	reader := &Reader{config: Config{Timeout: time.Second, MaxItems: 4}, collectors: map[agentpb.FactGroup]collector{
		agentpb.FactGroup_FACT_GROUP_MEMORY: func(context.Context, bool, int) *agentpb.FactGroupResult {
			return result(agentpb.FactGroup_FACT_GROUP_MEMORY, agentpb.FactStatus_FACT_STATUS_OK, "")
		},
		agentpb.FactGroup_FACT_GROUP_OPERATING_SYSTEM: func(context.Context, bool, int) *agentpb.FactGroupResult {
			return result(agentpb.FactGroup_FACT_GROUP_OPERATING_SYSTEM, agentpb.FactStatus_FACT_STATUS_OK, "")
		},
	}}
	results := reader.Read(context.Background(), &agentpb.ReadFacts{Groups: []agentpb.FactGroup{
		agentpb.FactGroup_FACT_GROUP_MEMORY,
		agentpb.FactGroup_FACT_GROUP_UNSPECIFIED,
		agentpb.FactGroup_FACT_GROUP_OPERATING_SYSTEM,
		agentpb.FactGroup_FACT_GROUP_MEMORY,
	}})
	if len(results) != 3 {
		t.Fatalf("result count = %d, want 3", len(results))
	}
	if results[0].GetGroup() != agentpb.FactGroup_FACT_GROUP_UNSPECIFIED ||
		results[0].GetStatus() != agentpb.FactStatus_FACT_STATUS_UNSUPPORTED ||
		results[1].GetGroup() != agentpb.FactGroup_FACT_GROUP_OPERATING_SYSTEM ||
		results[2].GetGroup() != agentpb.FactGroup_FACT_GROUP_MEMORY {
		t.Fatalf("results = %#v", results)
	}
}

func TestReadDefaultsToEverySupportedGroup(t *testing.T) {
	t.Parallel()
	reader := New(Config{Timeout: 20 * time.Second})
	results := reader.Read(context.Background(), &agentpb.ReadFacts{})
	groups := reader.SupportedGroups()
	if len(results) != len(groups) {
		t.Fatalf("result count = %d, want %d", len(results), len(groups))
	}
	for index, group := range groups {
		if results[index].GetGroup() != group {
			t.Fatalf("result %d group = %s, want %s", index, results[index].GetGroup(), group)
		}
		if results[index].GetStatus() == agentpb.FactStatus_FACT_STATUS_UNSPECIFIED {
			t.Fatalf("result %d has unspecified status", index)
		}
	}
}

func TestReadEnforcesSensitivePolicyAndItemLimit(t *testing.T) {
	t.Parallel()
	var gotSensitive bool
	var gotLimit int
	reader := &Reader{config: Config{Timeout: time.Second, AllowSensitive: false, MaxItems: 7}, collectors: map[agentpb.FactGroup]collector{
		agentpb.FactGroup_FACT_GROUP_NETWORK: func(_ context.Context, sensitive bool, limit int) *agentpb.FactGroupResult {
			gotSensitive = sensitive
			gotLimit = limit
			return result(agentpb.FactGroup_FACT_GROUP_NETWORK, agentpb.FactStatus_FACT_STATUS_OK, "")
		},
	}}
	reader.Read(context.Background(), &agentpb.ReadFacts{
		Groups: []agentpb.FactGroup{agentpb.FactGroup_FACT_GROUP_NETWORK}, IncludeSensitive: true,
	})
	if gotSensitive {
		t.Fatal("sensitive collection bypassed local policy")
	}
	if gotLimit != 7 {
		t.Fatalf("limit = %d, want 7", gotLimit)
	}
}

func TestReadReportsPerGroupTimeout(t *testing.T) {
	t.Parallel()
	reader := &Reader{config: Config{Timeout: 10 * time.Millisecond, MaxItems: 1}, collectors: map[agentpb.FactGroup]collector{
		agentpb.FactGroup_FACT_GROUP_MEMORY: func(ctx context.Context, _ bool, _ int) *agentpb.FactGroupResult {
			<-ctx.Done()
			return failureResult(ctx.Err())
		},
	}}
	results := reader.Read(context.Background(), &agentpb.ReadFacts{Groups: []agentpb.FactGroup{
		agentpb.FactGroup_FACT_GROUP_MEMORY,
	}})
	if len(results) != 1 || results[0].GetStatus() != agentpb.FactStatus_FACT_STATUS_TIMEOUT {
		t.Fatalf("results = %#v", results)
	}
}
