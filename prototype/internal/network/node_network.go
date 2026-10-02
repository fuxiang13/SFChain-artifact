package network

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sfchain/pkg/types"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	defaultTimeout = 30 * time.Second
	maxRetries     = -1 // -1 means retry indefinitely until success
	baseRetryDelay = 500 * time.Millisecond
)

// NodeNetwork handles node network communication
type NodeNetwork struct {
	nodeID         string
	baseURL        string
	client         *http.Client
	transport      *http.Transport
	connectionPool map[string][]*http.Client // connection pool: address -> clients
	messageQueue   *MessageQueue
	grpcClients    map[string]*GRPCClient // gRPC clients: address -> client
	grpcServer     *GRPCServer
}

// NewNodeNetwork creates a node network communication instance
func NewNodeNetwork(nodeID, baseURL string, natsURL string) *NodeNetwork {
	// Create a transport with a connection pool
	transport := &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	client := &http.Client{
		Timeout:   defaultTimeout,
		Transport: transport,
	}

	// Initialize the message queue
	messageQueue, _ := NewMessageQueue(nodeID, natsURL)

	return &NodeNetwork{
		nodeID:         nodeID,
		baseURL:        baseURL,
		client:         client,
		transport:      transport,
		connectionPool: make(map[string][]*http.Client),
		messageQueue:   messageQueue,
		grpcClients:    make(map[string]*GRPCClient),
		grpcServer:     nil,
	}
}

// SendBlock sends a block to the given node
func (nn *NodeNetwork) SendBlock(block *types.Block, isFull bool, targetNode *types.NodeInfo, managerSignatureHex string) error {
	return nn.SendBlockWithPreviousSig(block, isFull, targetNode, managerSignatureHex, 0, "")
}

// SendBlockWithPreviousSig sends a block to the given node (including the
// previous block's aggregated signature)
func (nn *NodeNetwork) SendBlockWithPreviousSig(block *types.Block, isFull bool, targetNode *types.NodeInfo, managerSignatureHex string, previousBlockHeight int64, previousAggregatedSig string) error {
	_ = block.Header.CalculateHash()

	var msg types.NetworkMessage
	if isFull {
		msg = types.NetworkMessage{
			Type:  "full_block",
			Chain: block.Header.ChainType,
			From:  nn.nodeID,
			To:    targetNode.NodeID,
			Data: types.BlockMessage{
				Block:                 block,
				IsFull:                true,
				ManagerSignature:      managerSignatureHex,
				PreviousBlockHeight:   previousBlockHeight,
				PreviousAggregatedSig: previousAggregatedSig,
			},
		}
	} else {
		msg = types.NetworkMessage{
			Type:  "block_header",
			Chain: block.Header.ChainType,
			From:  nn.nodeID,
			To:    targetNode.NodeID,
			Data: types.BlockHeaderMessage{
				Header:                block.Header,
				ManagerSignature:      managerSignatureHex,
				PreviousBlockHeight:   previousBlockHeight,
				PreviousAggregatedSig: previousAggregatedSig,
			},
		}
	}

	if err := nn.sendMessage(targetNode.Address, msg); err != nil {
		return fmt.Errorf("failed to send block to node %s: %v", targetNode.NodeID, err)
	}
	return nil
}

// BroadcastSignature broadcasts a signature to all registered nodes
func (nn *NodeNetwork) BroadcastSignature(blockHash, signature string, nodes map[string]*types.NodeInfo) error {
	msg := types.NetworkMessage{
		Type: "signature",
		From: nn.nodeID,
		Data: types.SignatureMessage{
			BlockHash: blockHash,
			Signature: signature,
		},
	}

	for _, node := range nodes {
		if node.NodeID == nn.nodeID {
			continue // skip self
		}
		if err := nn.sendMessage(node.Address, msg); err != nil {
			log.Printf("failed to send signature to node %s: %v", node.NodeID, err)
		}
	}
	return nil
}

// SendSignature sends a single signature to the given node (used by other
// nodes to send their signatures to the management node)
func (nn *NodeNetwork) SendSignature(blockHash, signature, publicKey string, targetNode *types.NodeInfo, chainType types.TransactionType) error {
	msg := types.NetworkMessage{
		Type:  "signature",
		Chain: int(chainType),
		From:  nn.nodeID,
		To:    targetNode.NodeID,
		Data: types.SignatureMessage{
			BlockHash: blockHash,
			Signature: signature,
			PublicKey: publicKey,
		},
	}

	return nn.sendMessage(targetNode.Address, msg)
}

// SendBlockConfirmation sends a block-processing confirmation to the management node
// DTO nodes send this confirmation after successfully persisting a block
func (nn *NodeNetwork) SendBlockConfirmation(blockHash string, height int64, targetNode *types.NodeInfo, chainType types.TransactionType) error {
	msg := types.NetworkMessage{
		Type:  "block_confirmation",
		Chain: int(chainType),
		From:  nn.nodeID,
		To:    targetNode.NodeID,
		Data: types.BlockConfirmationMessage{
			BlockHash: blockHash,
			Height:    height,
		},
	}

	return nn.sendMessage(targetNode.Address, msg)
}

// SendBlockHeader sends a block header to the given node
func (nn *NodeNetwork) SendBlockHeader(header *types.BlockHeader, targetNode *types.NodeInfo) error {
	msg := types.NetworkMessage{
		Type:  "block_header",
		Chain: header.ChainType,
		From:  nn.nodeID,
		To:    targetNode.NodeID,
		Data:  header,
	}

	return nn.sendMessage(targetNode.Address, msg)
}

// sendMessage sends a generic message
func (nn *NodeNetwork) sendMessage(address string, msg interface{}) error {
	// Serialize the message
	jsonData, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Try gRPC communication (only when a gRPC client already exists)
	if _, exists := nn.grpcClients[address]; exists {
		// TODO: send the message via the gRPC client
		// Only a skeleton exists because no concrete gRPC service code was generated
		// Skip gRPC for now and send directly over HTTP
	} else {
		// Create the gRPC client only on the first attempt to avoid reconnecting every time
		// Create it asynchronously to avoid blocking the HTTP send
		go func(addr string) {
			if _, err := nn.getGRPCClient(addr); err != nil {
				// Print the error only in debug mode
			}
		}(address)
	}

	// Try asynchronous communication via the message queue (skip NATS when
	// SFCHAIN_NET=http: under load spikes, slow-consumer disconnects drop messages)
	if os.Getenv("SFCHAIN_NET") != "http" && nn.messageQueue != nil && nn.messageQueue.IsConnected() {
		// Build the subject
		subject := fmt.Sprintf("sfchain.node.%s", address)

		// Publish the message to the message queue
		err := nn.messageQueue.Publish(subject, jsonData)
		if err == nil {
			return nil
		} else {
			// Print the error only in debug mode
		}
	} else {
		// Message queue not connected; silently fall back to HTTP
	}

	// Compress the message
	compressedData, err := nn.compressData(jsonData)
	if err != nil {
		return err
	}

	// Send the message (with retry)
	request, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api/network/receive", address), bytes.NewBuffer(compressedData))
	if err != nil {
		return err
	}

	// Set compression headers
	request.Header.Set("Content-Encoding", "zstd")
	request.Header.Set("Content-Type", "application/json")

	// Execute the request with retry
	resp, err := nn.doRequestWithRetry(request, maxRetries)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("receiver returned error status code: %d", resp.StatusCode)
	}

	return nil
}

// getGRPCClient returns or creates a gRPC client
func (nn *NodeNetwork) getGRPCClient(address string) (*GRPCClient, error) {
	// Check whether a client already exists
	if client, exists := nn.grpcClients[address]; exists {
		return client, nil
	}

	// Create a new gRPC client
	client, err := NewGRPCClient(address)
	if err != nil {
		return nil, err
	}

	// Cache the client
	nn.grpcClients[address] = client
	return client, nil
}

// compressData compresses the data
func (nn *NodeNetwork) compressData(data []byte) ([]byte, error) {
	// Use the zstd algorithm, which is more efficient than gzip
	var buf bytes.Buffer
	writer, err := zstd.NewWriter(&buf)
	if err != nil {
		// Fall back to gzip if zstd initialization fails
		return nn.compressDataWithGzip(data)
	}

	_, err = writer.Write(data)
	if err != nil {
		writer.Close()
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// compressDataWithGzip compresses the data with gzip (fallback)
func (nn *NodeNetwork) compressDataWithGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gzipWriter := gzip.NewWriter(&buf)
	_, err := gzipWriter.Write(data)
	if err != nil {
		return nil, err
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// doRequestWithRetry performs an HTTP request with retry
// maxRetries < 0 means retry indefinitely until success
func (nn *NodeNetwork) doRequestWithRetry(req *http.Request, maxRetries int) (*http.Response, error) {
	var lastErr error
	i := 0

	for {
		// maxRetries < 0 means unlimited retries
		if maxRetries >= 0 && i >= maxRetries {
			break
		}

		// Replay the request body before retrying: http.Request.Body can be
		// consumed once; reusing a consumed req sends an empty payload that the
		// peer silently drops (root cause of lost blocks)
		if i > 0 && req.GetBody != nil {
			if b, err := req.GetBody(); err == nil {
				req.Body = b
			}
		}

		resp, err := nn.client.Do(req)
		if err == nil {
			return resp, nil
		}

		lastErr = err
		retryDelay := nn.calculateRetryDelay(i)
		time.Sleep(retryDelay)
		i++
	}

	return nil, fmt.Errorf("max retries %d reached: %v", maxRetries, lastErr)
}

// calculateRetryDelay computes the retry delay (with exponential backoff and jitter)
func (nn *NodeNetwork) calculateRetryDelay(attempt int) time.Duration {
	// Exponential backoff: baseRetryDelay * (2^attempt)
	delay := baseRetryDelay * (1 << uint(attempt))

	// Add jitter: ±20%
	// Ensure delay/5 is at least 1 to avoid a rand.Intn panic
	jitterFactor := int(delay / 5)
	if jitterFactor < 1 {
		jitterFactor = 1
	}
	jitter := time.Duration(rand.Intn(jitterFactor) - jitterFactor/2)
	delay += jitter

	// Cap the delay at 5 seconds
	if delay > 5*time.Second {
		delay = 5 * time.Second
	}

	// Enforce a minimum delay
	if delay < time.Millisecond {
		delay = time.Millisecond
	}

	return delay
}
