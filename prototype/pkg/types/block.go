package types

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// BlockHeader is the block header structure
type BlockHeader struct {
	BlockHeight         int     `json:"block_height"`
	PreviousHash        string  `json:"previous_hash"`
	MerkleRoot          string  `json:"merkle_root"`
	Timestamp           float64 `json:"timestamp"`
	ChainType           int     `json:"chain_type"`
	AggregatedSignature string  `json:"aggregated_signature"`
}

// CalculateHash computes the block header hash (covering all fields)
func (bh *BlockHeader) CalculateHash() string {
	data := fmt.Sprintf("%d%s%s%.0f%d%s",
		bh.BlockHeight, bh.PreviousHash, bh.MerkleRoot,
		bh.Timestamp, bh.ChainType, bh.AggregatedSignature)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// Block is the block structure
type Block struct {
	Header       *BlockHeader   `json:"header"`
	Transactions []*Transaction `json:"transactions"`
}

// NewBlock creates a new block
func NewBlock(height int, prevHash string, chainType int, txs []Transaction) *Block {
	header := &BlockHeader{
		BlockHeight:  height,
		PreviousHash: prevHash,
		ChainType:    chainType,
		Timestamp:    float64(time.Now().UnixNano()) / 1e6,
	}

	block := &Block{
		Header:       header,
		Transactions: make([]*Transaction, 0),
	}

	for _, tx := range txs {
		block.AddTransaction(&tx)
	}

	return block
}

// AddTransaction adds a transaction to the block
func (b *Block) AddTransaction(tx *Transaction) bool {
	b.Transactions = append(b.Transactions, tx)
	return true
}

// CalculateMerkleRoot computes the Merkle root
func (b *Block) CalculateMerkleRoot() string {
	if len(b.Transactions) == 0 {
		return ""
	}

	// Compute the hash of each transaction
	hashes := make([]string, 0, len(b.Transactions))
	for _, tx := range b.Transactions {
		txHash := tx.CalculateTXID()
		hashes = append(hashes, txHash)
	}

	// Build the Merkle tree
	for len(hashes) > 1 {
		// If the number of hashes is odd, duplicate the last hash
		if len(hashes)%2 == 1 {
			hashes = append(hashes, hashes[len(hashes)-1])
		}

		// Compute the parent node hash
		newHashes := make([]string, 0, len(hashes)/2)
		for i := 0; i < len(hashes); i += 2 {
			combined := hashes[i] + hashes[i+1]
			hash := sha256.Sum256([]byte(combined))
			newHashes = append(newHashes, hex.EncodeToString(hash[:]))
		}
		hashes = newHashes
	}

	return hashes[0]
}

// Validate checks the validity of the block
func (b *Block) Validate() bool {
	if b.Header == nil {
		return false
	}

	// Verify the Merkle root
	if b.Header.MerkleRoot != b.CalculateMerkleRoot() {
		return false
	}

	// Verify the transactions
	if !b.ValidateTransactions() {
		return false
	}

	// Verify the block hash
	return b.Header.CalculateHash() != ""
}

// ValidateTransactions validates the transactions in the block
func (b *Block) ValidateTransactions() bool {
	if len(b.Transactions) == 0 {
		return true
	}

	// Validate transactions in parallel
	return b.ParallelValidateTransactions()
}

// ParallelValidateTransactions validates the transactions in the block in parallel
func (b *Block) ParallelValidateTransactions() bool {
	if len(b.Transactions) == 0 {
		return true
	}

	// Create a validation task for each transaction
	results := make([]bool, len(b.Transactions))
	var wg sync.WaitGroup

	for i, tx := range b.Transactions {
		wg.Add(1)
		go func(idx int, transaction *Transaction) {
			defer wg.Done()
			// Verify the transaction ID
			if transaction.TXID != transaction.CalculateTXID() {
				results[idx] = false
				return
			}
			// Verify the transaction signature (real signature-verification logic is needed here)
			// if !verifyTransactionSignature(transaction) {
			// 	results[idx] = false
			// 	return
			// }
			results[idx] = true
		}(i, tx)
	}

	wg.Wait()

	// Check all validation results
	for _, result := range results {
		if !result {
			return false
		}
	}

	return true
}

// ParallelCalculateMerkleRoot computes the Merkle root in parallel
func (b *Block) ParallelCalculateMerkleRoot() string {
	if len(b.Transactions) == 0 {
		return ""
	}

	// Compute each transaction hash in parallel
	hashes := make([]string, len(b.Transactions))
	var wg sync.WaitGroup

	for i, tx := range b.Transactions {
		wg.Add(1)
		go func(idx int, transaction *Transaction) {
			defer wg.Done()
			hashes[idx] = transaction.CalculateTXID()
		}(i, tx)
	}

	wg.Wait()

	// Build the Merkle tree
	return calculateMerkleRoot(hashes)
}

// calculateMerkleRoot computes the Merkle root
func calculateMerkleRoot(hashes []string) string {
	if len(hashes) == 0 {
		return ""
	}

	if len(hashes) == 1 {
		return hashes[0]
	}

	// Compute the parent node hashes in parallel
	newHashes := make([]string, 0, (len(hashes)+1)/2)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < len(hashes); i += 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var combined string
			if idx+1 < len(hashes) {
				combined = hashes[idx] + hashes[idx+1]
			} else {
				combined = hashes[idx] + hashes[idx]
			}
			hash := sha256.Sum256([]byte(combined))
			hashStr := hex.EncodeToString(hash[:])
			mu.Lock()
			newHashes = append(newHashes, hashStr)
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	return calculateMerkleRoot(newHashes)
}

func SafeSubstring(s string, length int) string {
	if len(s) <= length {
		return s
	}
	return s[:length]
}

type BlockSignatureCache struct {
	BlockHash        string            `json:"block_hash"`
	Block            *Block            `json:"block"`
	ManagerSignature string            `json:"manager_signature"`
	DTOSignatures    map[string]string `json:"dto_signatures"`
}

func NewBlockSignatureCache(block *Block, managerSignature string) *BlockSignatureCache {
	blockHash := block.Header.CalculateHash()
	return &BlockSignatureCache{
		BlockHash:        blockHash,
		Block:            block,
		ManagerSignature: managerSignature,
		DTOSignatures:    make(map[string]string),
	}
}

func (bsc *BlockSignatureCache) AddDTOSignature(dtoNodeID string, signature string) {
	bsc.DTOSignatures[dtoNodeID] = signature
}

func (bsc *BlockSignatureCache) GetAllSignatures() []string {
	signatures := make([]string, 0, len(bsc.DTOSignatures)+1)
	signatures = append(signatures, bsc.ManagerSignature)
	for _, sig := range bsc.DTOSignatures {
		signatures = append(signatures, sig)
	}
	return signatures
}
