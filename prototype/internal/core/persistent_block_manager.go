package core

import (
	"fmt"
	"log"
	"sfchain/internal/database"
	"sfchain/pkg/config"
	"sfchain/pkg/types"
	"sync"
)

type PersistentBlockManager struct {
	db       database.BlockManagerInterface
	mu       sync.RWMutex
	dataPath string
	config   *config.DatabaseConfig
	nodeType string
}

func NewPersistentBlockManager(nodeType types.NodeType, dataPath string, publicKey string, dbConfig *config.DatabaseConfig) (*PersistentBlockManager, error) {
	// use the passed database configuration
	cfg := dbConfig

	// create the database factory
	dbFactory := database.NewDBFactory(cfg)

	// convert nodeType to a string
	nodeTypeStr := "management"
	switch nodeType {
	case types.NodeTypeDevelopment:
		nodeTypeStr = "development"
	case types.NodeTypeTest:
		nodeTypeStr = "test"
	case types.NodeTypeOperations:
		nodeTypeStr = "operations"
	default:
		nodeTypeStr = "management"
	}

	// create the block manager
	blockManager, err := dbFactory.CreateBlockManager(nodeTypeStr, publicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create block manager: %w", err)
	}

	return &PersistentBlockManager{
		db:       blockManager,
		dataPath: dataPath,
		config:   cfg,
		nodeType: nodeTypeStr,
	}, nil
}

func (pbm *PersistentBlockManager) Close() error {
	return pbm.db.Close()
}

func (pbm *PersistentBlockManager) AddBlock(block *types.Block) bool {
	pbm.mu.Lock()
	defer pbm.mu.Unlock()

	blockHash := block.Header.CalculateHash()

	log.Printf("[block persistence] starting to persist block: height=%d, hash=%s, chainType=%d, txs=%d",
		block.Header.BlockHeight, SafeSubstring(blockHash, 16), block.Header.ChainType, len(block.Transactions))

	// determine the chain type
	chainType := "management"
	switch block.Header.ChainType {
	case 1:
		chainType = "management"
	case 2:
		chainType = "development"
	case 3:
		chainType = "test"
	case 4:
		chainType = "operations"
	}

	// decide the storage mode based on node type and chain type
	var err error
	if pbm.nodeType == "management" {
		// management node: store full blocks for all chains
		err = pbm.db.AddBlock(block)
	} else {
		// DTO node: store the full block for its own chain, headers for other chains
		if pbm.nodeType == chainType {
			err = pbm.db.AddBlock(block)
		} else {
			err = pbm.db.AddBlockHeader(block)
		}
	}

	if err != nil {
		log.Printf("[block persistence] failed to persist block: %v", err)
		return false
	}

	log.Printf("[block persistence] block persisted: height=%d, hash=%s, chainType=%d",
		block.Header.BlockHeight, SafeSubstring(blockHash, 16), block.Header.ChainType)
	return true
}

func (pbm *PersistentBlockManager) GetLastBlock(chainType int) *types.Block {
	pbm.mu.RLock()
	defer pbm.mu.RUnlock()

	// use the int chainType directly, avoiding type conversion issues
	blockInterface, err := pbm.db.GetLatestBlock(chainType)
	if err != nil {
		log.Printf("failed to get latest block: %v", err)
		return nil
	}

	block, ok := blockInterface.(*types.Block)
	if !ok {
		log.Printf("block type conversion failed")
		return nil
	}

	return block
}

func (pbm *PersistentBlockManager) GetBlockByHeight(height int, chainType int) *types.Block {
	pbm.mu.RLock()
	defer pbm.mu.RUnlock()

	// use the int chainType directly, avoiding type conversion issues
	blockInterface, err := pbm.db.GetBlockByHeight(int64(height), chainType)
	if err != nil {
		log.Printf("failed to get block: %v", err)
		return nil
	}

	block, ok := blockInterface.(*types.Block)
	if !ok {
		log.Printf("block type conversion failed")
		return nil
	}

	return block
}

func (pbm *PersistentBlockManager) GetChainLength(chainType int) int {
	pbm.mu.RLock()
	defer pbm.mu.RUnlock()

	// use the int chainType directly, avoiding type conversion issues
	height, err := pbm.db.GetBlockchainHeight(chainType)
	if err != nil {
		log.Printf("failed to get chain height: %v", err)
		return 0
	}

	return int(height)
}

func (pbm *PersistentBlockManager) GetAllBlocks(chainType int) []*types.Block {
	pbm.mu.RLock()
	defer pbm.mu.RUnlock()

	// use the int chainType directly, avoiding type conversion issues
	height, err := pbm.db.GetBlockchainHeight(chainType)
	if err != nil {
		log.Printf("failed to get chain height: %v", err)
		return []*types.Block{}
	}

	blocksInterface, err := pbm.db.GetBlocksByRange(0, height, chainType)
	if err != nil {
		log.Printf("failed to get block range: %v", err)
		return []*types.Block{}
	}

	// convert to []*types.Block
	var blocks []*types.Block
	for _, blockInterface := range blocksInterface {
		if block, ok := blockInterface.(*types.Block); ok {
			blocks = append(blocks, block)
		}
	}

	return blocks
}

func (pbm *PersistentBlockManager) GetBlockByHash(hash string) *types.Block {
	pbm.mu.RLock()
	defer pbm.mu.RUnlock()

	// use the BlockManagerInterface GetBlock method
	block, err := pbm.db.GetBlock(hash)
	if err != nil {
		log.Printf("failed to get block: %v", err)
		return nil
	}

	// type assertion
	if typedBlock, ok := block.(*types.Block); ok {
		return typedBlock
	}

	return nil
}

func (pbm *PersistentBlockManager) UpdateBlockWithAggregatedSignature(blockHash string, aggregatedSignature string) error {
	pbm.mu.Lock()
	defer pbm.mu.Unlock()

	err := pbm.db.UpdateBlockWithAggregatedSignature(blockHash, aggregatedSignature)
	if err != nil {
		log.Printf("failed to update block aggregate signature: %v", err)
		return err
	}

	log.Printf("[block update] block aggregate signature updated: hash=%s", SafeSubstring(blockHash, 16))
	return nil
}

func (pbm *PersistentBlockManager) UpdateBlockWithAggregatedSignatureByHeight(blockHeight int64, chainType types.TransactionType, aggregatedSignature string) error {
	pbm.mu.Lock()
	defer pbm.mu.Unlock()

	err := pbm.db.UpdateBlockWithAggregatedSignatureByHeight(blockHeight, chainType, aggregatedSignature)
	if err != nil {
		log.Printf("failed to update block aggregate signature: %v", err)
		return err
	}

	log.Printf("[block update] block aggregate signature updated: height=%d", blockHeight)
	return nil
}
