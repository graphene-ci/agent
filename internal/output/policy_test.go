package output

import (
	"testing"
	"time"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestNormalizeDefaultsAndOverrides(t *testing.T) {
	t.Parallel()
	defaults, err := Normalize(nil, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.MaxFrameBytes != defaultFrameBytes || defaults.FlushInterval != 100*time.Millisecond || defaults.MaxPendingBytes != defaultPendingBytes || defaults.MaxInlineBytes != defaultInlineBytes || defaults.Overflow != agentpb.OutputOverflowPolicy_OUTPUT_OVERFLOW_POLICY_DROP_OLDEST {
		t.Fatalf("defaults = %#v", defaults)
	}

	policy, err := Normalize(&agentpb.OutputPolicy{
		MaxFrameBytes: 200, FlushInterval: durationpb.New(2 * time.Second), MaxPendingBytes: 100,
		MaxInlineBytes: 300, Overflow: agentpb.OutputOverflowPolicy_OUTPUT_OVERFLOW_POLICY_DROP_NEWEST,
	}, 80)
	if err != nil {
		t.Fatal(err)
	}
	if policy.MaxFrameBytes != 80 || policy.FlushInterval != 2*time.Second || policy.MaxPendingBytes != 80 || policy.MaxInlineBytes != 300 || policy.Overflow != agentpb.OutputOverflowPolicy_OUTPUT_OVERFLOW_POLICY_DROP_NEWEST {
		t.Fatalf("policy = %#v", policy)
	}
}

func TestNormalizeRejectsInvalidPolicy(t *testing.T) {
	t.Parallel()
	invalidDuration := durationpb.New(11 * time.Second)
	invalidDuration.Nanos = 1_000_000_000
	for _, test := range []struct {
		name   string
		value  *agentpb.OutputPolicy
		global uint64
	}{
		{name: "zero global"},
		{name: "frame too large", value: &agentpb.OutputPolicy{MaxFrameBytes: maximumFrameBytes + 1}, global: 1 << 20},
		{name: "invalid duration", value: &agentpb.OutputPolicy{FlushInterval: invalidDuration}, global: 1 << 20},
		{name: "negative duration", value: &agentpb.OutputPolicy{FlushInterval: durationpb.New(-time.Second)}, global: 1 << 20},
		{name: "long duration", value: &agentpb.OutputPolicy{FlushInterval: durationpb.New(11 * time.Second)}, global: 1 << 20},
		{name: "unknown overflow", value: &agentpb.OutputPolicy{Overflow: agentpb.OutputOverflowPolicy(99)}, global: 1 << 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Normalize(test.value, test.global)
			if err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
