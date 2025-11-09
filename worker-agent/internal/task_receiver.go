package internal

import (
	"context"
	"fmt"
	"net"
	"strconv"

	taskpb "github.com/daniar-achakeev/scheduler/pkg-contracts/go"
	"google.golang.org/grpc"
)

type TaskReceiverServer struct {
	taskpb.UnimplementedTasksServer
}

func (s *TaskReceiverServer) ReceiveTasks(address string, port int) error {
	listener, err := net.Listen("tcp", net.JoinHostPort(address, strconv.Itoa(port)))
	if err != nil {
		return fmt.Errorf("unable to listen on port %d: %w", port, err)
	}
	grpcServer := grpc.NewServer()
	taskpb.RegisterTasksServer(grpcServer, s)
	err = grpcServer.Serve(listener)
	if err != nil {
		return fmt.Errorf("unable to start gRPC server: %w", err)
	}
	return nil
}

func (s *TaskReceiverServer) SubmitTask(ctx context.Context, req *taskpb.SubmitTaskRequest) (*taskpb.SubmitTaskResponse, error) {
	// TODO
	// receives task
	// executes command in a goroutine
	//
	return &taskpb.SubmitTaskResponse{Success: true}, nil
}
