package testserver

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// UnaryAuthInterceptor authenticates unary RPCs with the agent Bearer token.
func UnaryAuthInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := authenticate(ctx, token); err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
}

// StreamAuthInterceptor authenticates streaming RPCs with the agent Bearer token.
func StreamAuthInterceptor(token string) grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authenticate(stream.Context(), token); err != nil {
			return err
		}
		return handler(server, stream)
	}
}

func authenticate(ctx context.Context, token string) error {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return status.Error(codes.Unauthenticated, "exactly one Bearer token is required")
	}
	presented := strings.TrimPrefix(values[0], "Bearer ")
	if len(presented) != len(token) || subtle.ConstantTimeCompare([]byte(presented), []byte(token)) != 1 {
		return status.Error(codes.Unauthenticated, "invalid Bearer token")
	}
	return nil
}
