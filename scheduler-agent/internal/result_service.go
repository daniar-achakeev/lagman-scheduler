package internal

import (
	"context"
	"fmt"
	"net"
	"strconv"

	jobpb "github.com/daniar-achakeev/scheduler/pkg-contracts/go"
	"google.golang.org/grpc"
)

type Logger interface {
	Logf(format string, args ...any)
}

type ResultReceiver struct {
	jobpb.UnimplementedResultsServer
	lgr Logger
}

func NewResultReceiver(lgr Logger) *ResultReceiver {
	return &ResultReceiver{
		lgr: lgr,
	}
}

func (r *ResultReceiver) Run(port int) error {
	// TODO just playing with grpc
	const addr = "127.0.0.1" // localhost
	listener, err := net.Listen("tcp", net.JoinHostPort(addr, strconv.Itoa(port)))
	if err != nil {
		return fmt.Errorf("unable to listen on port %d: %w", port, err)
	}
	grpcServer := grpc.NewServer()
	jobpb.RegisterResultsServer(grpcServer, r)
	r.lgr.Logf("starting server on %s:%d", addr, port)
	err = grpcServer.Serve(listener)
	if err != nil {
		return fmt.Errorf("unable to start gRPC server: %w", err)
	}
	return nil
}

func (r *ResultReceiver) SubmitResult(ctx context.Context, req *jobpb.SubmitResultRequest) (*jobpb.SubmitResultResponse, error) {
	jobId := req.Result.JobId
	taskId := req.Result.TaskId
	r.lgr.Logf("JobId %s, Taks %s", jobId, taskId)
	return &jobpb.SubmitResultResponse{Success: true}, nil
}
