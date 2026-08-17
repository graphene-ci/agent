package protocol

import (
	"context"
	"errors"
	"io/fs"
	"testing"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorCode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want agentpb.ErrorCode
	}{
		{name: "canceled", err: context.Canceled, want: agentpb.ErrorCode_ERROR_CODE_CANCELED},
		{name: "local not found", err: fs.ErrNotExist, want: agentpb.ErrorCode_ERROR_CODE_NOT_FOUND},
		{name: "grpc not found", err: status.Error(codes.NotFound, "missing"), want: agentpb.ErrorCode_ERROR_CODE_NOT_FOUND},
		{name: "already exists", err: status.Error(codes.AlreadyExists, "exists"), want: agentpb.ErrorCode_ERROR_CODE_ALREADY_EXISTS},
		{name: "permission", err: status.Error(codes.PermissionDenied, "denied"), want: agentpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED},
		{name: "invalid", err: status.Error(codes.InvalidArgument, "invalid"), want: agentpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{name: "data loss", err: status.Error(codes.DataLoss, "digest"), want: agentpb.ErrorCode_ERROR_CODE_CHECKSUM_MISMATCH},
		{name: "io", err: errors.New("broken"), want: agentpb.ErrorCode_ERROR_CODE_IO},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := ErrorCode(test.err); got != test.want {
				t.Fatalf("ErrorCode() = %s, want %s", got, test.want)
			}
		})
	}
}
