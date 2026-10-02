package network

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sfchain/pkg/types"

	"github.com/gorilla/mux"
	"github.com/klauspost/compress/zstd"
)

// NodeAPI is the node API handler
type NodeAPI struct {
	consensus types.ConsensusNodeInterface
	router    *mux.Router
}

// NewNodeAPI creates a node API instance
func NewNodeAPI(consensus types.ConsensusNodeInterface) *NodeAPI {
	api := &NodeAPI{
		consensus: consensus,
		router:    mux.NewRouter(),
	}
	api.registerRoutes()
	return api
}

// RegisterHandlers registers the handlers on an external router
func (api *NodeAPI) RegisterHandlers(router *mux.Router) {
	router.HandleFunc("/api/network/receive", api.handleNetworkMessage).Methods("POST")
	router.HandleFunc("/api/tx/endorsed", api.handleEndorsedTransactions).Methods("POST")
}

// registerRoutes registers the routes
func (api *NodeAPI) registerRoutes() {
	api.router.HandleFunc("/api/network/receive", api.handleNetworkMessage).Methods("POST")
	api.router.HandleFunc("/api/tx/endorsed", api.handleEndorsedTransactions).Methods("POST")
}

// Start starts the API server
func (api *NodeAPI) Start(port int) error {
	log.Printf("Starting node API server on port %d", port)
	return http.ListenAndServe(fmt.Sprintf(":%d", port), api.router)
}

// handleNetworkMessage handles a received network message
func (api *NodeAPI) handleNetworkMessage(w http.ResponseWriter, r *http.Request) {

	// Read and possibly decompress the request body
	body, err := api.readAndDecompressBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var msg map[string]interface{}
	if err := json.Unmarshal(body, &msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	msgType, _ := msg["type"].(string)
	msgChain, _ := msg["chain"].(float64)

	switch msgType {
	case "block", "block_header", "full_block":
		if err := api.handleBlockMessageFromMap(msg, int(msgChain)); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	case "signature":
		api.handleSignatureMessageFromMap(msg)
	case "block_confirmation":
		api.handleBlockConfirmationMessageFromMap(msg)
	default:
		http.Error(w, "unknown message type", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleEndorsedTransactions receives endorsed transactions pushed directly by
// the endorsement service (in-memory endorsement mode). The body is a JSON
// array [*types.Transaction,...] (gzip-compressed bodies are supported); the
// whole batch enters the management node's in-memory packaging pool.
func (api *NodeAPI) handleEndorsedTransactions(w http.ResponseWriter, r *http.Request) {
	pool, ok := api.consensus.(interface {
		ReceiveEndorsedTransactionsBatch(txs []*types.Transaction)
	})
	if !ok {
		http.Error(w, "node does not accept direct endorsed transactions", http.StatusNotImplemented)
		return
	}

	body, err := api.readAndDecompressBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var txs []*types.Transaction
	if err := json.Unmarshal(body, &txs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	pool.ReceiveEndorsedTransactionsBatch(txs)
	log.Printf("[direct push] received %d endorsed transactions into the in-memory packaging pool", len(txs))
	w.WriteHeader(http.StatusOK)
}

// handleBlockMessageFromMap handles a block message from a map, returning an
// error so the caller can respond with an HTTP error
func (api *NodeAPI) handleBlockMessageFromMap(msg map[string]interface{}, chain int) error {
	msgType, _ := msg["type"].(string)
	log.Printf("Received block message for chain %d, type: %s", chain, msgType)
	log.Printf("Message data type: %T", msg["data"])

	dtoNode, ok := api.consensus.(interface {
		ProcessReceivedBlock(block *types.Block, isFull bool, managerSignature string) error
		ProcessReceivedBlockHeader(header *types.BlockHeader, chainType types.TransactionType, managerSignature string) error
	})

	if !ok {
		log.Printf("Type assertion failed: consensus is not a DTONode, consensus type: %T", api.consensus)
		return fmt.Errorf("type assertion failed: consensus is not a DTONode")
	}

	log.Printf("Type assertion succeeded, processing block message")

	dataBytes, err := json.Marshal(msg["data"])
	if err != nil {
		log.Printf("Failed to serialize data: %v", err)
		return fmt.Errorf("failed to serialize data: %v", err)
	}

	switch msgType {
	case "full_block":
		log.Printf("Processing full_block message")
		var blockMsg types.BlockMessage
		if err := json.Unmarshal(dataBytes, &blockMsg); err != nil {
			log.Printf("Failed to deserialize BlockMessage: %v", err)
			return fmt.Errorf("failed to deserialize BlockMessage: %v", err)
		}
		log.Printf("BlockMessage parsed, Block: %+v, IsFull: %v, ManagerSignature: %s", blockMsg.Block, blockMsg.IsFull, types.SafeSubstring(blockMsg.ManagerSignature, 32))
		if blockMsg.Block == nil {
			log.Printf("Block is nil")
			return fmt.Errorf("block is nil")
		}
		if blockMsg.Block.Header == nil {
			log.Printf("Block.Header is nil")
			return fmt.Errorf("block header is nil")
		}

		if err := dtoNode.ProcessReceivedBlock(blockMsg.Block, blockMsg.IsFull, blockMsg.ManagerSignature); err != nil {
			log.Printf("Failed to process full block: %v", err)
			return err
		}
	case "block_header":
		log.Printf("Processing block_header message")
		var headerMsg types.BlockHeaderMessage
		if err := json.Unmarshal(dataBytes, &headerMsg); err != nil {
			log.Printf("Failed to deserialize BlockHeaderMessage: %v", err)
			return fmt.Errorf("failed to deserialize BlockHeaderMessage: %v", err)
		}
		log.Printf("BlockHeaderMessage parsed, Height: %d, ManagerSignature: %s", headerMsg.Header.BlockHeight, types.SafeSubstring(headerMsg.ManagerSignature, 32))
		if headerMsg.Header == nil {
			log.Printf("Header is nil")
			return fmt.Errorf("header is nil")
		}

		// Then process the new block header (asynchronous)
		if err := dtoNode.ProcessReceivedBlockHeader(headerMsg.Header, types.TransactionType(chain), headerMsg.ManagerSignature); err != nil {
			log.Printf("Failed to process block header: %v", err)
			return err
		}
	}

	return nil
}

// handleSignatureMessageFromMap handles a signature message from a map
func (api *NodeAPI) handleSignatureMessageFromMap(msg map[string]interface{}) {

	data, ok := msg["data"].(map[string]interface{})
	if !ok {
		return
	}

	blockHash, _ := data["block_hash"].(string)
	signature, _ := data["signature"].(string)
	publicKey, _ := data["public_key"].(string)
	from, _ := msg["from"].(string)
	chain, _ := msg["chain"].(float64)

	// Process the signature asynchronously to avoid blocking the HTTP response
	go api.consensus.ReceiveSignature(
		types.TransactionType(int(chain)),
		blockHash,
		from,
		signature,
		publicKey,
	)
}

// handleBlockConfirmationMessageFromMap handles a block confirmation message from a map
func (api *NodeAPI) handleBlockConfirmationMessageFromMap(msg map[string]interface{}) {

	data, ok := msg["data"].(map[string]interface{})
	if !ok {
		return
	}

	blockHash, _ := data["block_hash"].(string)
	height, _ := data["height"].(float64)
	from, _ := msg["from"].(string)
	chain, _ := msg["chain"].(float64)

	// Process the confirmation asynchronously to avoid blocking the HTTP response
	go api.consensus.ReceiveBlockConfirmation(
		types.TransactionType(int(chain)),
		blockHash,
		from,
		int64(height),
	)
}

// readAndDecompressBody reads and decompresses the request body
func (api *NodeAPI) readAndDecompressBody(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	contentEncoding := r.Header.Get("Content-Encoding")

	// Check whether the body is gzip-compressed
	if contentEncoding == "gzip" {

		// Decompress the data
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %v", err)
		}
		defer reader.Close()

		decompressedBody, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress message: %v", err)
		}

		return decompressedBody, nil
		// Check whether the body is zstd-compressed
	} else if contentEncoding == "zstd" {

		// Decompress the data
		reader, err := zstd.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd reader: %v", err)
		}
		defer reader.Close()

		decompressedBody, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress message: %v", err)
		}

		return decompressedBody, nil
	}

	// Uncompressed messages are returned as-is
	return body, nil
}
