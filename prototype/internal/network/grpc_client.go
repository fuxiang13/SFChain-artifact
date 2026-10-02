package network

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCClient is the gRPC client
type GRPCClient struct {
	conn *grpc.ClientConn
}

// NewGRPCClient creates a gRPC client
func NewGRPCClient(address string) (*GRPCClient, error) {
	// Create the gRPC connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server: %v", err)
	}

	log.Printf("gRPC client connected: %s", address)

	return &GRPCClient{
		conn: conn,
	}, nil
}

// Close closes the gRPC connection
func (c *GRPCClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// GetConnection returns the gRPC connection
func (c *GRPCClient) GetConnection() *grpc.ClientConn {
	return c.conn
}
