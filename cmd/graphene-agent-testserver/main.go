package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/graphene-ci/agent/internal/secret"
	"github.com/graphene-ci/agent/internal/testserver"
	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		slog.Error("testserver stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	flags := pflag.NewFlagSet("graphene-agent-testserver", pflag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	listenAddress := flags.String("listen", "127.0.0.1:7443", "gRPC listen address")
	dataDirectory := flags.String("data-dir", ".graphene-agent-testserver", "artifact and staging directory")
	tokenFile := flags.String("token-file", "", "path to the same scoped token used by the agent")
	chunkBytes := flags.Int("artifact-chunk-bytes", 1<<20, "artifact stream chunk size")
	allowRemote := flags.Bool("allow-remote", false, "allow listening on a non-loopback address")
	noConsole := flags.Bool("no-console", false, "serve until a signal without reading commands from stdin")
	if err := flags.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			return nil
		}
		return err
	}
	token, err := secret.Load(*tokenFile, os.LookupEnv)
	if err != nil {
		return err
	}
	absoluteDataDirectory, err := filepath.Abs(*dataDirectory)
	if err != nil {
		return fmt.Errorf("resolve data directory: %w", err)
	}
	service, err := testserver.New(absoluteDataDirectory, *chunkBytes)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer func() { _ = listener.Close() }()
	if err := requireLoopback(listener.Addr(), *allowRemote); err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(testserver.UnaryAuthInterceptor(token)),
		grpc.StreamInterceptor(testserver.StreamAuthInterceptor(token)),
	)
	agentpb.RegisterAgentServiceServer(grpcServer, service)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	serveResult := make(chan error, 1)
	go func() { serveResult <- grpcServer.Serve(listener) }()
	slog.Info("graphene agent testserver listening", "address", listener.Addr().String(), "data_dir", absoluteDataDirectory)

	consoleResult := make(chan error, 1)
	if !*noConsole {
		console := testserver.NewConsole(service, os.Stdout)
		go func() { consoleResult <- console.Run(ctx, os.Stdin) }()
	}
	var result error
	select {
	case <-ctx.Done():
	case err := <-serveResult:
		if !errors.Is(err, grpc.ErrServerStopped) {
			result = err
		}
	case err := <-consoleResult:
		result = err
	}
	cancel()
	stopServer(grpcServer)
	return result
}

func requireLoopback(address net.Addr, allowRemote bool) error {
	if allowRemote {
		return nil
	}
	tcpAddress, ok := address.(*net.TCPAddr)
	if !ok || !tcpAddress.IP.IsLoopback() {
		return errors.New("testserver only listens on loopback unless --allow-remote is set")
	}
	return nil
}

func stopServer(server *grpc.Server) {
	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		server.Stop()
	}
}
