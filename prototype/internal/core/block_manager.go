package core

import (
	"fmt"
	"log"
	"sfchain/pkg/types"
	"sync"
)

type BlockManager struct {
	chainType types.TransactionType
	chain     []*types.Block
	headers   []*types.BlockHeader
	mu        sync.RWMutex
}

func NewBlockManager(chainType types.TransactionType) *BlockManager {
	return &BlockManager{
		chainType: chainType,
		chain:     make([]*types.Block, 0),
		headers:   make([]*types.BlockHeader, 0),
	}
}

func (bm *BlockManager) AddBlock(block *types.Block) bool {
	if !bm.ValidateBlock(block, true) {
		return false
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.chain = append(bm.chain, block)
	return true
}

func (bm *BlockManager) AddBlockHeader(header *types.BlockHeader) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.headers = append(bm.headers, header)
}

func (bm *BlockManager) ValidateBlock(block *types.Block, isFullBlock bool) bool {
	blockHash := block.Header.CalculateHash()
	if blockHash == "" {
		return false
	}

	lastBlock := bm.GetLastBlock()
	if lastBlock != nil {
		lastBlockHash := lastBlock.Header.CalculateHash()
		if block.Header.PreviousHash != lastBlockHash {
			return false
		}
	}

	return true
}

func (bm *BlockManager) GetLastBlock() *types.Block {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	if len(bm.chain) == 0 {
		return nil
	}
	return bm.chain[len(bm.chain)-1]
}

func (bm *BlockManager) GetBlockByHeight(height int) *types.Block {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	for _, block := range bm.chain {
		if block.Header.BlockHeight == height {
			return block
		}
	}
	return nil
}

func (bm *BlockManager) GetChainLength() int {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return len(bm.chain)
}

func (bm *BlockManager) GetHeaderCount() int {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return len(bm.headers)
}

func (bm *BlockManager) GetBlocksInRange(startHeight, endHeight int) []*types.Block {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	var result []*types.Block
	for _, block := range bm.chain {
		if block.Header.BlockHeight >= startHeight && block.Header.BlockHeight <= endHeight {
			result = append(result, block)
		}
	}
	return result
}

func (bm *BlockManager) GetChainType() types.TransactionType {
	return bm.chainType
}

func (bm *BlockManager) GetBlockHeaders() []*types.BlockHeader {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return bm.headers
}

func (bm *BlockManager) UpdateBlockWithAggregatedSignature(blockHash string, aggregatedSignature string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// look up the block
	var block *types.Block
	for _, b := range bm.chain {
		currentHash := b.Header.CalculateHash()
		if currentHash == blockHash {
			block = b
			break
		}
	}

	if block == nil {
		return fmt.Errorf("block not found: %s", blockHash)
	}

	// update the block header's aggregate signature
	block.Header.AggregatedSignature = aggregatedSignature

	// compute the new block hash (including the aggregate signature)
	newBlockHash := block.Header.CalculateHash()

	log.Printf("[block update] block hash and aggregate signature updated: oldHash=%s, newHash=%s, chainType=%d",
		SafeSubstring(blockHash, 16), SafeSubstring(newBlockHash, 16), block.Header.ChainType)

	return nil
}
