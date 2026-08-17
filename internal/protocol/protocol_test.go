package protocol

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInstructionID(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		id   *agentpb.InstructionId
		want string
	}{
		{name: "valid", id: &agentpb.InstructionId{Value: " instruction "}, want: " instruction "},
		{name: "absent"},
		{name: "blank", id: &agentpb.InstructionId{Value: " \t"}},
		{name: "too long", id: &agentpb.InstructionId{Value: strings.Repeat("x", 4097)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := InstructionID(test.id)
			if test.want == "" && err == nil {
				t.Fatal("expected an error")
			}
			if test.want != "" && (err != nil || got != test.want) {
				t.Fatalf("InstructionID() = %q, %v", got, err)
			}
		})
	}
}

func TestFailureSanitizesMessage(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		message string
		want    string
	}{
		{name: "trim", message: "  failed  ", want: "failed"},
		{name: "empty", message: " \n", want: "operation failed"},
		{name: "bounded", message: strings.Repeat("x", 5000), want: strings.Repeat("x", 4096)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := Failure("instruction", agentpb.ErrorCode_ERROR_CODE_IO, test.message)
			if request.GetInstructionId().GetValue() != "instruction" || request.GetOperationFailed().GetCode() != agentpb.ErrorCode_ERROR_CODE_IO || request.GetOperationFailed().GetMessage() != test.want {
				t.Fatalf("Failure() = %#v", request)
			}
		})
	}
}

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
