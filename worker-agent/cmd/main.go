package main

import (
	"log"

	"google.golang.org/grpc"
)

func main() {
	serverAddr := "localhost:28001"
	// very simple for testing tls would be next FIXME
	conn, err := grpc.NewClient(serverAddr)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
}
