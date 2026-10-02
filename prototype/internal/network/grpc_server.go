package network

import (
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
)

// GRPCServer is the gRPC server
type GRPCServer struct {
	server   *grpc.Server
	listener net.Listener
	port     int
}

// NewGRPCServer creates a gRPC server
func NewGRPCServer(port int) (*GRPCServer, error) {
	// Create the listener
	addr := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create listener: %v", err)
	}

	// Create the gRPC server
	server := grpc.NewServer()

	// Register services
	// TODO: register the NodeService service
	// pb.RegisterNodeServiceServer(server, &nodeServiceServer{})

	return &GRPCServer{
		server:   server,
		listener: listener,
		port:     port,
	}, nil
}

// Start starts the gRPC server
func (s *GRPCServer) Start() error {
	log.Printf("gRPC server started, listening on port: %d", s.port)
	return s.server.Serve(s.listener)
}

// Stop stops the gRPC server
func (s *GRPCServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
		log.Printf("gRPC server stopped")
	}
}
