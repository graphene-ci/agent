package session

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/graphene-ci/agent/internal/config"
)

// bearerCredentials sends the scoped token with every RPC.
type bearerCredentials struct {
	token         string
	allowInsecure bool
}

func (b bearerCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b bearerCredentials) RequireTransportSecurity() bool {
	return !b.allowInsecure
}

// Dial creates the outbound gRPC connection to the server — the agent's
// only connection to the world.
func Dial(cfg config.Config) (*grpc.ClientConn, error) {
	var transport credentials.TransportCredentials
	if cfg.Insecure {
		transport = insecure.NewCredentials()
	} else {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.CAFile != "" {
			roots, err := x509.SystemCertPool()
			if err != nil {
				return nil, fmt.Errorf("load system certificate pool: %w", err)
			}
			pem, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, fmt.Errorf("read CA file: %w", err)
			}
			if !roots.AppendCertsFromPEM(pem) {
				return nil, errors.New("CA file contains no certificates")
			}
			tlsConfig.RootCAs = roots
		}
		transport = credentials.NewTLS(tlsConfig)
	}
	conn, err := grpc.NewClient(cfg.Server,
		grpc.WithTransportCredentials(transport),
		grpc.WithPerRPCCredentials(bearerCredentials{token: cfg.Token, allowInsecure: cfg.Insecure}),
		// Keepalive is the ONLY thing that detects a half-open connection:
		// when the server is recreated without a clean RST (a compose
		// redeploy), neither Recv nor a buffered heartbeat Send errors for
		// minutes, and the agent sits a zombie — connected in its own mind,
		// dropped by the server. HTTP/2 PINGs surface the dead peer within
		// Time+Timeout, so the reconnect loop actually runs.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                15 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create gRPC client: %w", err)
	}
	return conn, nil
}
