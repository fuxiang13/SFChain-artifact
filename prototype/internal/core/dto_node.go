package core

import (
	"C"
	"fmt"
	"log"
	"os"
	"sfchain/internal/network"
	"sfchain/pkg/config"
	"sfchain/pkg/crypto"
	"sfchain/pkg/types"
	"sync"

	bls "github.com/herumi/bls-eth-go-binary/bls"
)
import "time"

// DTONode is a data-transfer node (for the development, test, and operations node kinds)
type DTONode struct {
	BaseNode
	nodeCategory   types.NodeType // node category (development|test|operations)
	networkManager *network.NodeNetwork
	BlockManager   *PersistentBlockManager
	pendingHeaders map[string]*types.BlockHeader // blockHash -> header (awaiting aggregate signature verification)
	// most recent header successfully signed and returned (re-signs when the management node rebroadcasts and the local height is stale, covering sporadic signature HTTP loss)
	lastSigned     map[types.TransactionType]*lastSignedInfo
	waitingBlocks  map[string]*waitingBlockData // blockHash -> data awaiting predecessor blocks
	mu             sync.RWMutex
	chainProcessMu [5]sync.Mutex // serialize validation/signing at each chain height

	// local variable tracking the next block height of each chain
	chainNextHeight map[types.TransactionType]int

	// cache of the previous block hash (with aggregate signature), used to verify the prehash
	chainLastBlockHash map[types.TransactionType]string

	// block hash cache: key is"chainType:height", value is the block hash
	// avoids recomputing the block hash
	blockHashCache map[string]string

	// cached aggregate public key (aggregation of all node public keys), used to verify aggregate signatures
	cachedAggregatedPublicKey *bls.PublicKey
	cachedPublicKeysHash      string // used to detect public key changes

	// aggregate signature cache: key is"chainType:height", value is the aggregate signature hex string
	// used when generating the next block
	pendingAggregatedSignatures map[string]string

	// in-memory block cache: blockHash -> Block
	blockCache map[string]*types.Block

	// asynchronous persistence
	persistQueue   chan *dtoPersistTask
	persistCache   map[string]*dtoPersistTask
	persistCacheMu sync.RWMutex
}

// persistence task for a DTO node
type dtoPersistTask struct {
	block     *types.Block
	header    *types.BlockHeader
	isFull    bool
	chainType types.TransactionType
}

type waitingBlockData struct {
	block            *types.Block
	header           *types.BlockHeader
	managerSignature string
	isFull           bool
	chainType        types.TransactionType
	retryCount       int
}

// NewDTONode creates a data-transfer node
func NewDTONode(nodeID string, port int, nodeCategory types.NodeType) *DTONode {
	baseNode := NewBaseNode(nodeID, nodeCategory, port)

	node := &DTONode{
		nodeCategory:                nodeCategory,
		BaseNode:                    *baseNode,
		networkManager:              network.NewNodeNetwork(nodeID, fmt.Sprintf("127.0.0.1:%d", port), ""),
		pendingHeaders:              make(map[string]*types.BlockHeader),
		waitingBlocks:               make(map[string]*waitingBlockData),
		chainNextHeight:             make(map[types.TransactionType]int),
		chainLastBlockHash:          make(map[types.TransactionType]string),
		blockHashCache:              make(map[string]string),
		pendingAggregatedSignatures: make(map[string]string),
		lastSigned:                  make(map[types.TransactionType]*lastSignedInfo),
		blockCache:                  make(map[string]*types.Block),
		persistQueue:                make(chan *dtoPersistTask, 100),
		persistCache:                make(map[string]*dtoPersistTask),
	}

	// initialize the next block height of each chain to 1
	for i := types.TransactionTypeManagement; i <= types.TransactionTypeOperations; i++ {
		node.chainNextHeight[i] = 1
	}

	return node
}

// InitializeWithConfig initializes the node from configuration
func (en *DTONode) InitializeWithConfig(cfg *config.NodeConfig) error {
	en.PublicKey = cfg.Node.PublicKey
	en.PrivateKey = cfg.Node.PrivateKey
	en.networkManager = network.NewNodeNetwork(cfg.Node.NodeID, cfg.Node.Address, "nats://localhost:4222")

	// initialize network nodes
	typesNodes := en.convertConfigNodesToTypesNodes(cfg.Network.Nodes)
	en.InitializeNetworkNodes(typesNodes)

	// initialize the persistent block manager
	// empty dataPath because MySQL is used
	blockManager, err := NewPersistentBlockManager(en.NodeType, "", cfg.Node.PublicKey, &cfg.Database)
	if err != nil {
		return fmt.Errorf("failed to initialize BlockManager: %w", err)
	}
	en.BlockManager = blockManager

	// load the latest block height of each chain from the database
	en.loadChainHeightsFromDB()

	// start the persistence goroutine
	go en.persistenceWorker()

	return nil
}

// ProcessReceivedBlock processes a received full block (full processing flow for every chain)
type lastSignedInfo struct {
	blockHash string
	signature string
	publicKey string
	height    int64
}

func (en *DTONode) ProcessReceivedBlock(block *types.Block, isFull bool, managerSignature string) error {
	chainType := types.TransactionType(block.Header.ChainType)

	// run the full processing flow for every chain
	go en.processChainBlock(block, chainType, managerSignature, isFull)

	return nil
}

// processChainBlock processes a block of a single chain (full processing flow)
func (en *DTONode) processChainBlock(block *types.Block, chainType types.TransactionType, managerSignature string, isFull bool) error {
	if chainType < 1 || chainType > 4 || block == nil || block.Header == nil {
		return fmt.Errorf("invalid chain/block")
	}
	en.chainProcessMu[chainType].Lock()
	defer en.chainProcessMu[chainType].Unlock()
	blockHash := block.Header.CalculateHash()

	// 0. verify the block height matches the expected height
	expectedHeight := en.getNextBlockHeight(chainType)
	if block.Header.BlockHeight != expectedHeight {
		en.mu.RLock()
		ls := en.lastSigned[chainType]
		en.mu.RUnlock()
		if ls != nil && ls.height == int64(block.Header.BlockHeight) && ls.blockHash == blockHash {
			return en.sendSignatureToManagementNode(ls.blockHash, ls.signature, ls.publicKey, chainType)
		}
		en.addToWaitingQueue(blockHash, &waitingBlockData{
			block:            block,
			managerSignature: managerSignature,
			isFull:           isFull,
			chainType:        chainType,
			retryCount:       0,
		})
		return fmt.Errorf("block height mismatch: expected=%d, actual=%d", expectedHeight, block.Header.BlockHeight)
	}

	// 1. verify the prehash
	if err := en.verifyPrehash(block.Header, chainType); err != nil {
		en.addToWaitingQueue(blockHash, &waitingBlockData{
			block:            block,
			managerSignature: managerSignature,
			isFull:           isFull,
			chainType:        chainType,
			retryCount:       0,
		})
		return err
	}

	// 2. verify the management node signature
	managerPublicKey := en.getManagerPublicKey()
	if !en.ValidateBlockHeaderSignature(block.Header, managerSignature, managerPublicKey) {
		return fmt.Errorf("block header signature verification failed")
	}

	// 3. for a full block, verify the Merkle root
	if isFull {
		calculatedMerkleRoot := block.CalculateMerkleRoot()
		if block.Header.MerkleRoot != calculatedMerkleRoot {
			return fmt.Errorf("Merkle root verification failed")
		}
	}

	// 4. verify the AggregatedSignature
	if block.Header.BlockHeight > 1 {
		if err := en.verifyBlockAggregatedSignature(block.Header, chainType); err != nil {
			return fmt.Errorf("AggregatedSignature verification failed: %v", err)
		}
	} else {
		if block.Header.AggregatedSignature != "" {
			return fmt.Errorf("block 1's AggregatedSignature should be empty")
		}
	}

	// 5. sign the block header
	signature, publicKey, err := en.SignBlockHeader(block.Header)
	if err != nil {
		return fmt.Errorf("failed to sign block header: %v", err)
	}

	// 6. add the block to the in-memory cache immediately (without waiting for persistence)
	en.addBlockToCache(block, chainType)

	// 7. persist the block/header asynchronously (without waiting for completion)
	en.enqueuePersistTask(block, nil, isFull, chainType)

	// 8. send the block-processing confirmation to the management node
	en.sendBlockConfirmationToManagementNode(blockHash, int64(block.Header.BlockHeight), chainType)

	// 9. return the signature to the management node
	en.mu.Lock()
	en.lastSigned[chainType] = &lastSignedInfo{blockHash: blockHash, signature: signature, publicKey: publicKey, height: int64(block.Header.BlockHeight)}
	en.mu.Unlock()
	// Commit the local signing decision before sending: retry must replay the
	// same signature even if the response is lost.
	en.incrementChainHeight(chainType)
	if err := en.sendSignatureToManagementNode(blockHash, signature, publicKey, chainType); err != nil {
		return fmt.Errorf("failed to send signature: %v", err)
	}

	// 10. cache the block header awaiting the aggregate signature
	en.mu.Lock()
	en.pendingHeaders[blockHash] = block.Header
	en.mu.Unlock()

	// 11. after verification, update chainNextHeight

	// 12. process successor blocks in the waiting queue
	go en.processWaitingQueue(chainType)
	return nil
}

// addToWaitingQueue adds a block to the waiting queue
func (en *DTONode) addToWaitingQueue(blockHash string, data *waitingBlockData) {
	en.mu.Lock()
	defer en.mu.Unlock()
	en.waitingBlocks[blockHash] = data
}

// processWaitingQueue processes blocks in the waiting queue
func (en *DTONode) processWaitingQueue(chainType types.TransactionType) {
	maxRetries := 120 // retry limit while waiting for predecessor persistence (120x500ms=60s); too short a value wrongly drops waiting blocks during continuous block production and stalls consensus
	retryDelay := 500 // milliseconds

	for i := 0; i < maxRetries; i++ {
		en.mu.Lock()
		// find waiting blocks of this chain type and sort by height
		var waitingData []*waitingBlockData
		var toRemove []string
		for hash, data := range en.waitingBlocks {
			if data.chainType == chainType {
				data.retryCount++
				if data.retryCount <= maxRetries {
					waitingData = append(waitingData, data)
				} else {
					toRemove = append(toRemove, hash)
				}
			}
		}
		// remove timed-out blocks
		for _, hash := range toRemove {
			delete(en.waitingBlocks, hash)
		}
		en.mu.Unlock()

		if len(waitingData) == 0 {
			return
		}

		// sort by block height
		for j := 0; j < len(waitingData)-1; j++ {
			for k := j + 1; k < len(waitingData); k++ {
				// get the heights of the two blocks
				var heightJ, heightK int
				if waitingData[j].block != nil {
					heightJ = waitingData[j].block.Header.BlockHeight
				} else if waitingData[j].header != nil {
					heightJ = waitingData[j].header.BlockHeight
				}
				if waitingData[k].block != nil {
					heightK = waitingData[k].block.Header.BlockHeight
				} else if waitingData[k].header != nil {
					heightK = waitingData[k].header.BlockHeight
				}
				// sort ascending by height
				if heightJ > heightK {
					waitingData[j], waitingData[k] = waitingData[k], waitingData[j]
				}
			}
		}

		// try to process waiting blocks (in height order)
		for _, data := range waitingData {
			// get the header (prefer block's header, otherwise the standalone header)
			var header *types.BlockHeader
			if data.block != nil {
				header = data.block.Header
			} else if data.header != nil {
				header = data.header
			}

			if header == nil {
				continue
			}

			if err := en.verifyPrehash(header, data.chainType); err == nil {
				// prehash verified; remove from the waiting queue and process the block
				blockHash := header.CalculateHash()
				en.mu.Lock()
				delete(en.waitingBlocks, blockHash)
				en.mu.Unlock()

				if data.block != nil {
					go en.processChainBlock(data.block, data.chainType, data.managerSignature, data.isFull)
				} else {
					go en.processChainHeader(data.header, data.chainType, data.managerSignature)
				}
			}
		}

		// retry after waiting
		if i < maxRetries-1 {
			time.Sleep(time.Duration(retryDelay) * time.Millisecond)
		}
	}
}

// verifyPrehash verifies a block's prehash
func (en *DTONode) verifyPrehash(header *types.BlockHeader, chainType types.TransactionType) error {
	// check the block height equals the locally tracked next block height
	expectedHeight := en.getNextBlockHeight(chainType)
	if header.BlockHeight != expectedHeight {
		return fmt.Errorf("block height mismatch: expected=%d, actual=%d", expectedHeight, header.BlockHeight)
	}

	if header.BlockHeight == 1 {
		// first block: verify the prehash is the identity node's public key
		expectedPrehash := en.getNodePublicKey(chainType)
		if header.PreviousHash != expectedPrehash {
			return fmt.Errorf("first block prehash verification failed")
		}
	} else {
		// not the first block: verify the prehash equals the local previous block hash (including the aggregate signature)
		// prefer the cached hash, avoiding a database read every time
		en.mu.RLock()
		cachedHash, exists := en.chainLastBlockHash[chainType]
		en.mu.RUnlock()

		var expectedPrehash string
		if exists && cachedHash != "" {
			expectedPrehash = cachedHash
		} else {
			// cache miss: prefer the in-memory cache, otherwise read from the database
			lastBlock := en.getBlockFromCache(header.BlockHeight-1, chainType)
			if lastBlock == nil {
				return fmt.Errorf("previous block not found; cannot verify prehash")
			}

			// check block height continuity
			if lastBlock.Header.BlockHeight != header.BlockHeight-1 {
				return fmt.Errorf("block heights not consecutive")
			}

			expectedPrehash = lastBlock.Header.CalculateHash()
		}

		if header.PreviousHash != expectedPrehash {
			return fmt.Errorf("prehash verification failed")
		}
	}
	return nil
}

// ProcessReceivedBlockHeader processes a received block header (full processing flow for every chain)
func (en *DTONode) ProcessReceivedBlockHeader(header *types.BlockHeader, chainType types.TransactionType, managerSignature string) error {
	// run the full processing flow for every chain (synchronous)
	err := en.processChainHeader(header, chainType, managerSignature)
	if err != nil {
		return err
	}
	return nil
}

// processChainHeader processes a header of a single chain (full processing flow)
func (en *DTONode) processChainHeader(header *types.BlockHeader, chainType types.TransactionType, managerSignature string) error {
	if chainType < 1 || chainType > 4 || header == nil {
		return fmt.Errorf("invalid chain/header")
	}
	en.chainProcessMu[chainType].Lock()
	defer en.chainProcessMu[chainType].Unlock()
	blockHash := header.CalculateHash()

	// 0. verify the block height matches the expected height
	expectedHeight := en.getNextBlockHeight(chainType)
	if header.BlockHeight != expectedHeight {
		// re-sign: the management node rebroadcasts and this block was already processed locally (signature may have been lost); re-send the cached signature directly
		if header.BlockHeight < expectedHeight {
			en.mu.RLock()
			ls := en.lastSigned[chainType]
			en.mu.RUnlock()
			if ls != nil && ls.height == int64(header.BlockHeight) && ls.blockHash == blockHash {
				log.Printf("[re-sign] rebroadcast arrived and was already signed; re-sending signature: chain=%s, height=%d", chainType.String(), header.BlockHeight)
				go en.sendSignatureToManagementNode(ls.blockHash, ls.signature, ls.publicKey, chainType)
				return nil
			}
		}
		en.addToWaitingQueue(blockHash, &waitingBlockData{
			header:           header,
			managerSignature: managerSignature,
			isFull:           false,
			chainType:        chainType,
			retryCount:       0,
		})
		return fmt.Errorf("block header height mismatch")
	}

	// 1. verify the prehash
	if err := en.verifyPrehash(header, chainType); err != nil {
		en.addToWaitingQueue(blockHash, &waitingBlockData{
			header:           header,
			managerSignature: managerSignature,
			isFull:           false,
			chainType:        chainType,
			retryCount:       0,
		})
		return err
	}

	// 2. verify the management node signature
	managerPublicKey := en.getManagerPublicKey()
	if !en.ValidateBlockHeaderSignature(header, managerSignature, managerPublicKey) {
		return fmt.Errorf("block header signature verification failed")
	}

	// 3. verify the AggregatedSignature
	if header.BlockHeight > 1 {
		if err := en.verifyBlockAggregatedSignature(header, chainType); err != nil {
			return fmt.Errorf("AggregatedSignature verification failed")
		}
	} else {
		// block 1's AggregatedSignature should be empty
		if header.AggregatedSignature != "" {
			return fmt.Errorf("block 1's AggregatedSignature should be empty")
		}
	}

	// 4. sign the block header
	signature, publicKey, err := en.SignBlockHeader(header)
	if err != nil {
		return fmt.Errorf("failed to sign block header")
	}

	// 5. add the header to the in-memory cache immediately (without waiting for persistence)
	headerOnlyBlock := &types.Block{
		Header:       header,
		Transactions: []*types.Transaction{},
	}
	en.addBlockToCache(headerOnlyBlock, chainType)

	// 6. update the cached previous block hash
	en.updateCachedLastBlockHash(chainType, blockHash)

	// 7. persist the header asynchronously (without waiting for completion)
	en.enqueuePersistTask(nil, header, false, chainType)

	// 7.5 send the block-processing confirmation to the management node (block persisted successfully)
	en.sendBlockConfirmationToManagementNode(blockHash, int64(header.BlockHeight), chainType)

	// 8. return the signature to the management node
	en.mu.Lock()
	en.lastSigned[chainType] = &lastSignedInfo{blockHash: blockHash, signature: signature, publicKey: publicKey, height: int64(header.BlockHeight)}
	en.mu.Unlock()

	// 9. after verification, update chainNextHeight
	en.incrementChainHeight(chainType)
	if err := en.sendSignatureToManagementNode(blockHash, signature, publicKey, chainType); err != nil {
		return fmt.Errorf("failed to send signature")
	}

	// 10. process successor headers in the waiting queue
	go en.processWaitingQueue(chainType)

	return nil
}

// aggregateNetworkPublicKeys aggregates public keys from the node information in the configuration file
func (en *DTONode) aggregateNetworkPublicKeys() (*bls.PublicKey, error) {
	en.BaseNode.mu.RLock()
	defer en.BaseNode.mu.RUnlock()

	var pubKeys []*bls.PublicKey

	for _, nodeInfo := range en.NetworkNodes {
		// deserialize node public keys from the configuration file
		if nodeInfo.PublicKey != "" {
			pubKey, err := crypto.DeserializePublicKey(nodeInfo.PublicKey)
			if err != nil {
				continue
			}
			pubKeys = append(pubKeys, pubKey)
		}
	}

	if len(pubKeys) == 0 {
		return nil, fmt.Errorf("no valid public keys to aggregate")
	}

	// aggregate the public keys
	aggregatedPubKey := crypto.AggregatePublicKeys(pubKeys)
	return aggregatedPubKey, nil
}

func (en *DTONode) sendSignatureToManagementNode(blockHash, signature, publicKey string, chainType types.TransactionType) error {
	// RQ3 signature-refusal fault injection: with SFCHAIN_REFUSE=1 the node verifies but does not return a signature
	if os.Getenv("SFCHAIN_REFUSE") == "1" {
		log.Printf("\U0001F6D7 [refusal injection] SFCHAIN_REFUSE=1, skipping signature return: chain=%s, hash=%s", chainType.String(), blockHash[:16])
		return nil
	}
	// find the management node
	managementNode, err := en.findManagementNode()
	if err != nil {
		return err
	}

	// send the signature
	err = en.networkManager.SendSignature(blockHash, signature, publicKey, managementNode, chainType)
	if err != nil {
		log.Printf("[signature send failed] chain=%s, hash=%s, err=%v", chainType.String(), blockHash[:16], err)
	} else {
		log.Printf("[signature sent] chain=%s, hash=%s", chainType.String(), blockHash[:16])
	}
	return err
}

func (en *DTONode) findManagementNode() (*types.NodeInfo, error) {
	en.BaseNode.mu.RLock()
	defer en.BaseNode.mu.RUnlock()

	for _, node := range en.NetworkNodes {
		if node.NodeType == types.NodeTypeManagement {
			return node, nil
		}
	}

	return nil, fmt.Errorf("management node not found")
}

// GetNodeID implements the interface
func (en *DTONode) GetNodeID() string {
	return en.NodeID
}

// GetRegisteredNodes implements the interface
func (en *DTONode) GetRegisteredNodes() map[string]*types.NodeInfo {
	return en.NetworkNodes
}

// ReceiveSignature implements the interface
func (en *DTONode) ReceiveSignature(chainType types.TransactionType, blockHash, nodeID, signature, publicKey string) {
}

// ReceiveBlockConfirmation handles the block-confirmation message received by a DTO node
// for a DTO node the confirmation message carries no meaning; the DTO node is the sender of confirmations
func (en *DTONode) ReceiveBlockConfirmation(chainType types.TransactionType, blockHash string, nodeID string, height int64) {
}

// sendBlockConfirmationToManagementNode sends the block-processing confirmation to the management node
// called by the DTO node after successfully persisting a block
func (en *DTONode) sendBlockConfirmationToManagementNode(blockHash string, height int64, chainType types.TransactionType) {
	managementNode, err := en.findManagementNode()
	if err != nil {
		return
	}

	en.networkManager.SendBlockConfirmation(blockHash, height, managementNode, chainType)
}

// UpdateAggregatedSignature implements the interface
func (en *DTONode) UpdateAggregatedSignature(chainType types.TransactionType, blockHeight int64, signature string) {
	if en.BlockManager == nil {
		return
	}

	en.BlockManager.UpdateBlockWithAggregatedSignatureByHeight(blockHeight, chainType, signature)
}

// ProcessOtherChainHeader implements the interface
func (en *DTONode) ProcessOtherChainHeader(header *types.BlockHeader, chainType types.TransactionType) {
}

// GetNodeCategory returns the node category
func (en *DTONode) GetNodeCategory() types.NodeType {
	return en.nodeCategory
}

// convertConfigNodesToTypesNodes converts config.NodeInfo to types.NodeInfo
func (en *DTONode) convertConfigNodesToTypesNodes(configNodes []config.NodeInfo) []types.NodeInfo {
	var typesNodes []types.NodeInfo
	for _, configNode := range configNodes {
		typesNodes = append(typesNodes, types.NodeInfo{
			NodeID:    configNode.NodeID,
			NodeType:  types.NodeTypeFromString(configNode.NodeType),
			Port:      configNode.Port,
			Address:   configNode.Address,
			PublicKey: configNode.PublicKey,
		})
	}
	return typesNodes
}

// getNodePublicKey returns the public key of the node matching the chain type (for first-block prehash verification)
func (en *DTONode) getNodePublicKey(chainType types.TransactionType) string {
	en.BaseNode.mu.RLock()
	defer en.BaseNode.mu.RUnlock()

	for _, node := range en.NetworkNodes {
		switch chainType {
		case types.TransactionTypeDevelopment:
			if node.NodeType == types.NodeTypeDevelopment {
				return node.PublicKey
			}
		case types.TransactionTypeTest:
			if node.NodeType == types.NodeTypeTest {
				return node.PublicKey
			}
		case types.TransactionTypeOperations:
			if node.NodeType == types.NodeTypeOperations {
				return node.PublicKey
			}
		case types.TransactionTypeManagement:
			if node.NodeType == types.NodeTypeManagement {
				return node.PublicKey
			}
		}
	}
	return ""
}

// getNextBlockHeight returns the next block height of the given chain
func (en *DTONode) getNextBlockHeight(txType types.TransactionType) int {
	en.mu.RLock()
	defer en.mu.RUnlock()

	height, exists := en.chainNextHeight[txType]
	if !exists {
		return 1
	}
	return height
}

// incrementChainHeight advances the next block height of the given chain
func (en *DTONode) incrementChainHeight(txType types.TransactionType) {
	en.mu.Lock()
	defer en.mu.Unlock()

	en.chainNextHeight[txType]++
}

// updateCachedLastBlockHash updates the cached previous block hash
func (en *DTONode) updateCachedLastBlockHash(txType types.TransactionType, blockHash string) {
	en.mu.Lock()
	defer en.mu.Unlock()

	en.chainLastBlockHash[txType] = blockHash
}

// getManagerPublicKey returns the management node's public key (for verifying the management signature)
func (en *DTONode) getManagerPublicKey() string {
	en.BaseNode.mu.RLock()
	defer en.BaseNode.mu.RUnlock()

	for _, node := range en.NetworkNodes {
		if node.NodeType == types.NodeTypeManagement {
			return node.PublicKey
		}
	}
	return ""
}

// getAllSignerPublicKeys returns the public keys of all nodes that should sign (management node + all DTO nodes)
func (en *DTONode) getAllSignerPublicKeys() []string {
	en.BaseNode.mu.RLock()
	defer en.BaseNode.mu.RUnlock()

	var publicKeys []string
	for _, node := range en.NetworkNodes {
		// the management node and all DTO nodes should sign
		if node.NodeType == types.NodeTypeManagement ||
			node.NodeType == types.NodeTypeDevelopment ||
			node.NodeType == types.NodeTypeTest ||
			node.NodeType == types.NodeTypeOperations {
			publicKeys = append(publicKeys, node.PublicKey)
		}
	}
	return publicKeys
}

// verifyAggregatedSignature verifies an aggregate signature (using the cached aggregate public key)
func (en *DTONode) verifyAggregatedSignature(blockHash string, aggregatedSigHex string) bool {
	// 1. deserialize the aggregate signature
	aggregatedSig, err := crypto.DeserializeSignature(aggregatedSigHex)
	if err != nil {
		return false
	}

	// 2. get or create the cached aggregate public key
	aggregatedPubKey := en.getOrCreateCachedAggregatedPublicKey()
	if aggregatedPubKey == nil {
		return false
	}

	// 3. verify the aggregate signature
	message := []byte(blockHash)
	isValid := crypto.VerifyAggregatedSignature(aggregatedPubKey, message, aggregatedSig)

	return isValid
}

// verifyBlockAggregatedSignature verifies a block's AggregatedSignature field
// by design: block n's AggregatedSignature should be all nodes' aggregate signature over block n-1
func (en *DTONode) verifyBlockAggregatedSignature(header *types.BlockHeader, chainType types.TransactionType) error {
	if en.BlockManager == nil {
		return fmt.Errorf("BlockManager is nil")
	}

	// 1. get block n-1
	targetHeight := header.BlockHeight - 1

	prevBlock := en.getBlockFromCache(targetHeight, chainType)
	if prevBlock == nil {
		return fmt.Errorf("block n-1 not found")
	}

	// 2. get block n-1's hash (using the cache)
	prevBlockHash := en.getBlockHash(prevBlock, chainType)

	// 3. get the cached aggregate public key
	aggregatedPubKey := en.getOrCreateCachedAggregatedPublicKey()
	if aggregatedPubKey == nil {
		return fmt.Errorf("failed to get aggregate public key")
	}

	// 4. deserialize the aggregate signature
	aggregatedSigHex := header.AggregatedSignature
	aggregatedSig, err := crypto.DeserializeSignature(aggregatedSigHex)
	if err != nil {
		return fmt.Errorf("failed to deserialize aggregate signature")
	}

	// 5. verify the aggregate signature
	message := []byte(prevBlockHash)
	isValid := crypto.VerifyAggregatedSignature(aggregatedPubKey, message, aggregatedSig)
	if !isValid {
		return fmt.Errorf("aggregate signature verification failed")
	}

	return nil
}

// getBlockHash returns the block hash, using the cache to avoid recomputation
func (en *DTONode) getBlockHash(block *types.Block, chainType types.TransactionType) string {
	key := fmt.Sprintf("%s:%d", chainType.String(), block.Header.BlockHeight)

	// check the cache first
	en.mu.RLock()
	if hash, exists := en.blockHashCache[key]; exists {
		en.mu.RUnlock()
		return hash
	}
	en.mu.RUnlock()

	// cache miss; compute the hash
	hash := block.Header.CalculateHash()

	// update the cache
	en.mu.Lock()
	en.blockHashCache[key] = hash
	en.mu.Unlock()
	return hash
}

// getOrCreateCachedAggregatedPublicKey returns the cached aggregate public key, creating it if needed
func (en *DTONode) getOrCreateCachedAggregatedPublicKey() *bls.PublicKey {
	en.mu.RLock()
	// check the cache is valid
	if en.cachedAggregatedPublicKey != nil {
		en.mu.RUnlock()
		return en.cachedAggregatedPublicKey
	}
	en.mu.RUnlock()

	// cache miss; recompute
	en.mu.Lock()
	defer en.mu.Unlock()

	// double-check to avoid concurrent recomputation
	if en.cachedAggregatedPublicKey != nil {
		return en.cachedAggregatedPublicKey
	}

	// get all signers' public keys
	signerPubKeys := en.getAllSignerPublicKeys()
	if len(signerPubKeys) == 0 {
		return nil
	}

	// deserialize all public keys
	var pubKeys []*bls.PublicKey
	for _, pubKeyHex := range signerPubKeys {
		pubKey, err := crypto.DeserializePublicKey(pubKeyHex)
		if err != nil {
			continue
		}
		pubKeys = append(pubKeys, pubKey)
	}

	if len(pubKeys) == 0 {
		return nil
	}

	// aggregate the public keys
	aggregatedPubKey := crypto.AggregatePublicKeys(pubKeys)
	if aggregatedPubKey == nil {
		return nil
	}

	// cache the aggregate public key
	en.cachedAggregatedPublicKey = aggregatedPubKey

	return aggregatedPubKey
}

// clearAggregatedPublicKeyCache clears the aggregate public key cache (called when network nodes change)
func (en *DTONode) clearAggregatedPublicKeyCache() {
	en.mu.Lock()
	defer en.mu.Unlock()

	en.cachedAggregatedPublicKey = nil
	en.cachedPublicKeysHash = ""
}

// loadChainHeightsFromDB loads the latest block height of each chain from the database
func (en *DTONode) loadChainHeightsFromDB() {
	if en.BlockManager == nil {
		return
	}

	en.mu.Lock()
	defer en.mu.Unlock()

	// iterate all chain types
	for chainType := types.TransactionTypeManagement; chainType <= types.TransactionTypeOperations; chainType++ {
		// get the chain's latest block
		latestBlock := en.BlockManager.GetLastBlock(int(chainType))
		if latestBlock != nil {
			// set the next block height to the latest height + 1
			en.chainNextHeight[chainType] = latestBlock.Header.BlockHeight + 1
			// cache the previous block hash
			en.chainLastBlockHash[chainType] = latestBlock.Header.CalculateHash()
			// add the block to the in-memory cache
			en.blockCache[latestBlock.Header.CalculateHash()] = latestBlock
		}
	}
}

// addBlockToCache adds a block to the in-memory cache
func (en *DTONode) addBlockToCache(block *types.Block, chainType types.TransactionType) {
	if block == nil {
		return
	}

	blockHash := block.Header.CalculateHash()

	en.mu.Lock()
	en.blockCache[blockHash] = block
	en.chainLastBlockHash[chainType] = blockHash
	en.mu.Unlock()
}

// getBlockFromCache fetches a block from the in-memory cache
// memory-first design: a cache miss means the entry was cleaned (persisted successfully and no longer needed); the database is never queried
func (en *DTONode) getBlockFromCache(height int, chainType types.TransactionType) *types.Block {
	en.mu.RLock()
	defer en.mu.RUnlock()

	for _, block := range en.blockCache {
		if block.Header.BlockHeight == height && types.TransactionType(block.Header.ChainType) == chainType {
			return block
		}
	}
	// cache miss: the entry was cleaned; the database is no longer queried
	return nil
}

// persistenceWorker is the DTO node's asynchronous persistence worker goroutine
func (en *DTONode) persistenceWorker() {
	for {
		select {
		case task := <-en.persistQueue:
			if task != nil {
				en.persistDTOTask(task)
			}
		case <-en.StopChan:
			return
		}
	}
}

// persistDTOTask runs a DTO persistence task
func (en *DTONode) persistDTOTask(task *dtoPersistTask) {
	if task == nil {
		return
	}

	var blockToPersist *types.Block
	if task.isFull {
		blockToPersist = task.block
	} else {
		blockToPersist = &types.Block{
			Header:       task.header,
			Transactions: []*types.Transaction{},
		}
	}

	// build the cacheKey for deduplication
	cacheKey := fmt.Sprintf("%s:%d", task.chainType.String(), blockToPersist.Header.BlockHeight)

	// check whether already persisted (prevent duplicate persistence)
	en.persistCacheMu.RLock()
	_, exists := en.persistCache[cacheKey]
	en.persistCacheMu.RUnlock()

	if exists {
		return
	}

	// perform persistence
	if en.BlockManager != nil {
		for {
			if en.BlockManager.AddBlock(blockToPersist) {
				log.Printf("%s chain block %d persisted", task.chainType.String(), blockToPersist.Header.BlockHeight)

				// update the cache
				en.persistCacheMu.Lock()
				en.persistCache[cacheKey] = task
				en.persistCacheMu.Unlock()

				// Never roll the validation tip back when asynchronous I/O lags.
				// clean up expired block cache entries (keep the last block for prehash verification)
				en.cleanupBlockCache(task.chainType, blockToPersist.Header.BlockHeight)
				return
			}
			select {
			case <-en.StopChan:
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// enqueuePersistTask enqueues a persistence task
func (en *DTONode) enqueuePersistTask(block *types.Block, header *types.BlockHeader, isFull bool, chainType types.TransactionType) {
	task := &dtoPersistTask{
		block:     block,
		header:    header,
		isFull:    isFull,
		chainType: chainType,
	}

	select {
	case en.persistQueue <- task:
	case <-en.StopChan:
		log.Printf("shutdown before role custody enqueue")
	}
}

// cleanupBlockCache cleans up expired block cache entries
// rules: 1. only clean persisted blocks; 2. keep the last block of each chain
func (en *DTONode) cleanupBlockCache(chainType types.TransactionType, currentHeight int) {
	en.mu.Lock()
	defer en.mu.Unlock()

	// minimum height to keep for the chain (at least currentHeight-1, needed by the next block)
	minHeightToKeep := currentHeight - 1
	if minHeightToKeep < 1 {
		minHeightToKeep = 1
	}

	// collect blocks to delete
	var toDelete []string
	for blockHash, block := range en.blockCache {
		if types.TransactionType(block.Header.ChainType) != chainType {
			continue
		}

		// skip the chain's latest block (used for prehash verification)
		if block.Header.BlockHeight == currentHeight {
			continue
		}

		// skip blocks older than the minimum height
		if block.Header.BlockHeight < minHeightToKeep {
			// but keep it if the block is still in pendingHeaders (awaiting aggregate signature verification)
			if _, hasPending := en.pendingHeaders[blockHash]; hasPending {
				continue
			}
			toDelete = append(toDelete, blockHash)
		}
	}

	// delete expired blocks
	for _, hash := range toDelete {
		delete(en.blockCache, hash)
	}
}
