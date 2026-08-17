package output

import (
	"errors"
	"fmt"
	"time"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
)

const (
	defaultFrameBytes   = 64 << 10
	defaultPendingBytes = 1 << 20
	defaultInlineBytes  = 4 << 20
	maximumFrameBytes   = 4 << 20
)

type Policy struct {
	MaxFrameBytes   int
	FlushInterval   time.Duration
	MaxPendingBytes uint64
	MaxInlineBytes  uint64
	Overflow        agentpb.OutputOverflowPolicy
}

func Normalize(value *agentpb.OutputPolicy, globalLimit uint64) (Policy, error) {
	policy := Policy{
		MaxFrameBytes:   defaultFrameBytes,
		FlushInterval:   100 * time.Millisecond,
		MaxPendingBytes: defaultPendingBytes,
		MaxInlineBytes:  defaultInlineBytes,
		Overflow:        agentpb.OutputOverflowPolicy_OUTPUT_OVERFLOW_POLICY_DROP_OLDEST,
	}
	if value != nil {
		if value.GetMaxFrameBytes() != 0 {
			if value.GetMaxFrameBytes() > maximumFrameBytes {
				return Policy{}, fmt.Errorf("max_frame_bytes exceeds %d", maximumFrameBytes)
			}
			policy.MaxFrameBytes = int(value.GetMaxFrameBytes())
		}
		if value.GetFlushInterval() != nil {
			if err := value.GetFlushInterval().CheckValid(); err != nil {
				return Policy{}, fmt.Errorf("invalid flush_interval: %w", err)
			}
			if duration := value.GetFlushInterval().AsDuration(); duration != 0 {
				if duration < 0 || duration > 10*time.Second {
					return Policy{}, errors.New("flush_interval must be between zero and 10s")
				}
				policy.FlushInterval = duration
			}
		}
		if value.GetMaxPendingBytes() != 0 {
			policy.MaxPendingBytes = value.GetMaxPendingBytes()
		}
		if value.GetMaxInlineBytes() != 0 {
			policy.MaxInlineBytes = value.GetMaxInlineBytes()
		}
		switch value.GetOverflow() {
		case agentpb.OutputOverflowPolicy_OUTPUT_OVERFLOW_POLICY_UNSPECIFIED,
			agentpb.OutputOverflowPolicy_OUTPUT_OVERFLOW_POLICY_DROP_OLDEST:
			policy.Overflow = agentpb.OutputOverflowPolicy_OUTPUT_OVERFLOW_POLICY_DROP_OLDEST
		case agentpb.OutputOverflowPolicy_OUTPUT_OVERFLOW_POLICY_DROP_NEWEST:
			policy.Overflow = agentpb.OutputOverflowPolicy_OUTPUT_OVERFLOW_POLICY_DROP_NEWEST
		default:
			return Policy{}, fmt.Errorf("unsupported output overflow policy %d", value.GetOverflow())
		}
	}
	if globalLimit == 0 {
		return Policy{}, errors.New("global output limit is zero")
	}
	if policy.MaxPendingBytes > globalLimit {
		policy.MaxPendingBytes = globalLimit
	}
	if policy.MaxPendingBytes == 0 || policy.MaxInlineBytes == 0 {
		return Policy{}, errors.New("output limits must be positive")
	}
	if uint64(policy.MaxFrameBytes) > policy.MaxPendingBytes {
		policy.MaxFrameBytes = int(policy.MaxPendingBytes)
	}
	return policy, nil
}
