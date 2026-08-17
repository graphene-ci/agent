package protocol

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"strings"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func InstructionID(id *agentpb.InstructionId) (string, error) {
	if id == nil || strings.TrimSpace(id.GetValue()) == "" {
		return "", errors.New("instruction id is required")
	}
	if len(id.GetValue()) > 4096 {
		return "", errors.New("instruction id exceeds 4096 bytes")
	}
	return id.GetValue(), nil
}

func Failure(id string, code agentpb.ErrorCode, message string) *agentpb.ConnectRequest {
	return &agentpb.ConnectRequest{
		InstructionId: &agentpb.InstructionId{Value: id},
		Event: &agentpb.ConnectRequest_OperationFailed{OperationFailed: &agentpb.OperationFailed{
			Code:    code,
			Message: sanitize(message),
		}},
	}
}

func ErrorCode(err error) agentpb.ErrorCode {
	switch {
	case errors.Is(err, context.Canceled), status.Code(err) == codes.Canceled:
		return agentpb.ErrorCode_ERROR_CODE_CANCELED
	case errors.Is(err, fs.ErrNotExist), status.Code(err) == codes.NotFound:
		return agentpb.ErrorCode_ERROR_CODE_NOT_FOUND
	case errors.Is(err, fs.ErrExist), status.Code(err) == codes.AlreadyExists:
		return agentpb.ErrorCode_ERROR_CODE_ALREADY_EXISTS
	case errors.Is(err, fs.ErrPermission), errors.Is(err, os.ErrPermission),
		status.Code(err) == codes.PermissionDenied, status.Code(err) == codes.Unauthenticated:
		return agentpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED
	case status.Code(err) == codes.InvalidArgument, status.Code(err) == codes.OutOfRange:
		return agentpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT
	case status.Code(err) == codes.DataLoss:
		return agentpb.ErrorCode_ERROR_CODE_CHECKSUM_MISMATCH
	default:
		return agentpb.ErrorCode_ERROR_CODE_IO
	}
}

func sanitize(message string) string {
	message = strings.TrimSpace(message)
	if len(message) > 4096 {
		return message[:4096]
	}
	if message == "" {
		return "operation failed"
	}
	return message
}
