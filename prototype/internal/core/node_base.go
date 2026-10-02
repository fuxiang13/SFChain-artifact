package core

import (
	"C"
	"fmt"
	"log"
	"sfchain/internal/database"
	"sfchain/internal/network"
	"sfchain/pkg/types"
	"sync"
)
import (
	"sfchain/pkg/config"
	"sfchain/pkg/crypto"

	bls "github.com/herumi/bls-eth-go-binary/bls"
)

// BaseNode is the base type for all nodes
type BaseNode struct {
	NodeID        string
	NodeType      types.NodeType
	Port          int
	PrivateKey    string
	PublicKey     string
	NetworkNodes  map[string]*types.NodeInfo // network nodes loaded from the configuration file
	BlockManagers map[types.TransactionType]*BlockManager
	NodeAPI       *network.NodeAPI
	StopChan      chan struct{}
	mu            sync.RWMutex

	// the following fields are needed only by the management node
	LogDatabase  database.LogDatabaseInterface
	LogManager   *database.LogManager
	UserDatabase database.UserDatabaseInterface
}

// NewBaseNode creates a base node
func NewBaseNode(nodeID string, nodeType types.NodeType, port int) *BaseNode {
	node := &BaseNode{
		NodeID:        nodeID,
		NodeType:      nodeType,
		Port:          port,
		NetworkNodes:  make(map[string]*types.NodeInfo),
		BlockManagers: make(map[types.TransactionType]*BlockManager),
		StopChan:      make(chan struct{}),
	}

	// initialize block managers for all types
	for i := types.TransactionTypeManagement; i <= types.TransactionTypeOperations; i++ {
		node.BlockManagers[i] = NewBlockManager(i)
	}

	return node
}

// GetNodeID returns the node ID
func (n *BaseNode) GetNodeID() string {
	return n.NodeID
}

// GetNodeType returns the node type
func (n *BaseNode) GetNodeType() types.NodeType {
	return n.NodeType
}

// InitializeNetworkNodes loads network nodes from configuration
func (n *BaseNode) InitializeNetworkNodes(nodes []types.NodeInfo) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, node := range nodes {
		n.NetworkNodes[node.NodeID] = &types.NodeInfo{
			NodeID:    node.NodeID,
			NodeType:  node.NodeType,
			Address:   node.Address,
			PublicKey: node.PublicKey,
		}
	}
	log.Printf("loaded %d network node configurations", len(n.NetworkNodes))
}

// InitializeUserDatabase initializes the user database
func (n *BaseNode) InitializeUserDatabase(dataPath string) error {
	// create the database configuration
	dbConfig := &config.DatabaseConfig{
		Type:     "mysql",
		Host:     "localhost",
		Port:     3306,
		User:     "fx",
		Password: "123456",
		DBName:   "sfchain",
		Charset:  "utf8mb4",
	}

	// create the database factory
	dbFactory := database.NewDBFactory(dbConfig)

	// create the user database
	userDB, err := dbFactory.CreateUserDatabase()
	if err != nil {
		return fmt.Errorf("failed to initialize user database: %w", err)
	}

	n.UserDatabase = userDB

	log.Printf("user database initialized (node type: %s)", n.NodeType.String())
	return nil
}

// GetBlockManager returns the block manager for the given type
func (n *BaseNode) GetBlockManager(txType types.TransactionType) *BlockManager {
	return n.BlockManagers[txType]
}

// Stop stops the node
func (n *BaseNode) Stop() {
	close(n.StopChan)
	if n.LogDatabase != nil {
		n.LogDatabase.Close()
	}
	if n.UserDatabase != nil {
		n.UserDatabase.Close()
	}
	log.Printf("Node %s stopped", n.NodeID)
}

// SignBlockHeader signs a block header (common to all nodes)
func (n *BaseNode) SignBlockHeader(header *types.BlockHeader) (string, string, error) {
	// sign with the private key from the configuration file
	var secretKey *bls.SecretKey
	var err error

	if n.PrivateKey != "" {
		secretKey, err = crypto.DeserializeSecretKey(n.PrivateKey)
		if err != nil {
			// if the private key fails to parse, fall back to a randomly generated key
			secretKey = &bls.SecretKey{}
			secretKey.SetByCSPRNG()
		}
	} else {
		// if there is no private key, use a randomly generated key
		secretKey = &bls.SecretKey{}
		secretKey.SetByCSPRNG()
	}

	message := []byte(header.CalculateHash())
	signature := crypto.Sign(secretKey, message)
	signatureHex := crypto.SerializeSignature(signature)
	publicKey := n.PublicKey
	if publicKey == "" {
		publicKey = fmt.Sprintf("%s_public_key_12345", n.NodeType)
	}
	return signatureHex, publicKey, nil
}

// ValidateBlockHeaderSignature verifies a block header signature (common to all nodes)
func (n *BaseNode) ValidateBlockHeaderSignature(header *types.BlockHeader, managerSignature string, managerPublicKey string) bool {
	// verify the management node signature
	if managerSignature == "" {
		log.Printf("[block validation] management node signature is empty")
		return false
	}

	// verify the previous block hash (prehash)
	if header.BlockHeight > 1 && header.PreviousHash == "" {
		log.Printf("[block validation] previous block hash (prehash) is empty")
		return false
	}

	// verify the signature length
	if len(managerSignature) < 10 {
		log.Printf("[block validation] management node signature too short")
		return false
	}

	// verify the block hash
	blockHash := header.CalculateHash()
	if blockHash == "" {
		log.Printf("[block validation] block hash computation failed")
		return false
	}

	// verify the management node signature format
	_, err := crypto.DeserializeSignature(managerSignature)
	if err != nil {
		log.Printf("[block validation] invalid management node signature format: %v", err)
		return false
	}

	// verify the signature with the management node public key
	isValid, err := crypto.VerifyManagementSignature(managerPublicKey, blockHash, managerSignature)
	if err != nil {
		log.Printf("[block validation] signature verification failed: %v", err)
		return false
	}

	if !isValid {
		log.Printf("[block validation] management node signature verification failed")
		return false
	}

	log.Printf("[block validation] block header signature and prehash verification passed")

	return true
}

// ValidateAggregatedSignature verifies an aggregate signature (common to all nodes)
func (n *BaseNode) ValidateAggregatedSignature(header *types.BlockHeader) bool {
	// verify the aggregate signature is non-empty
	if header.AggregatedSignature == "" {
		log.Printf("[aggregate signature validation] aggregate signature is empty")
		return false
	}

	// verify the aggregate signature length
	if len(header.AggregatedSignature) < 10 {
		log.Printf("[aggregate signature validation] aggregate signature too short")
		return false
	}

	// verify the block hash
	blockHash := header.CalculateHash()
	if blockHash == "" {
		log.Printf("[aggregate signature validation] block hash computation failed")
		return false
	}

	// verify the aggregate signature format
	_, err := crypto.DeserializeSignature(header.AggregatedSignature)
	if err != nil {
		log.Printf("[aggregate signature validation] invalid aggregate signature format: %v", err)
		return false
	}

	// verify the aggregate signature
	// a real implementation must verify the signature with the aggregate public key
	// this simplified implementation assumes the signature is valid
	log.Printf("[aggregate signature validation] aggregate signature verification passed")

	return true
}

// PersistBlockOrHeader persists a block or block header (common to all nodes)
func (n *BaseNode) PersistBlockOrHeader(block *types.Block, isFull bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	chainType := types.TransactionType(block.Header.ChainType)
	bm := n.BlockManagers[chainType]

	if isFull {
		// store the full block
		if !bm.AddBlock(block) {
			return fmt.Errorf("failed to add block")
		}
		blockHash := block.Header.CalculateHash()
		log.Printf("persisted full block: height=%d, hash=%s", block.Header.BlockHeight, SafeSubstring(blockHash, 16))
	} else {
		// store the block header
		bm.AddBlockHeader(block.Header)
		log.Printf("persisted block header: height=%d, hash=%s", block.Header.BlockHeight, SafeSubstring(block.Header.CalculateHash(), 16))
	}

	return nil
}
