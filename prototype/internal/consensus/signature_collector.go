package consensus

import (
	"sync"
	"time"
)

// SignatureInfo holds signature information
type SignatureInfo struct {
	Signature string    `json:"signature"`
	PublicKey string    `json:"public_key"`
	Timestamp time.Time `json:"timestamp"`
	NodeID    string    `json:"node_id"`
}

// SignatureCollector collects signatures
type SignatureCollector struct {
	pendingSignatures map[string]map[string]*SignatureInfo // block_hash -> node_id -> SignatureInfo
	signatureTimeout  time.Duration
	mu                sync.RWMutex
}

// NewSignatureCollector creates a new signature collector
func NewSignatureCollector() *SignatureCollector {
	return &SignatureCollector{
		pendingSignatures: make(map[string]map[string]*SignatureInfo),
		signatureTimeout:  30 * time.Second,
	}
}

// AddSignature adds a node signature
func (sc *SignatureCollector) AddSignature(blockHash, nodeID, signature, publicKey string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if _, exists := sc.pendingSignatures[blockHash]; !exists {
		sc.pendingSignatures[blockHash] = make(map[string]*SignatureInfo)
	}

	sc.pendingSignatures[blockHash][nodeID] = &SignatureInfo{
		Signature: signature,
		PublicKey: publicKey,
		Timestamp: time.Now(),
		NodeID:    nodeID,
	}

	// Start the cleanup timer
	go sc.cleanupExpiredSignatures(blockHash)
}

// GetSignatures returns all signatures for the given block
func (sc *SignatureCollector) GetSignatures(blockHash string) map[string]*SignatureInfo {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if signatures, exists := sc.pendingSignatures[blockHash]; exists {
		return signatures
	}
	return nil
}

// HasEnoughSignatures checks whether enough signatures have been collected
func (sc *SignatureCollector) HasEnoughSignatures(blockHash string, thresholdRatio float64) bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	signatures, exists := sc.pendingSignatures[blockHash]
	if !exists {
		return false
	}

	// Compute the required number of signatures (all 4 nodes)
	requiredSignatures := 4
	return len(signatures) >= requiredSignatures
}

// RemoveSignatures removes the signatures of the given block
func (sc *SignatureCollector) RemoveSignatures(blockHash string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.pendingSignatures, blockHash)
}

// cleanupExpiredSignatures cleans up expired signatures
func (sc *SignatureCollector) cleanupExpiredSignatures(blockHash string) {
	time.Sleep(sc.signatureTimeout)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if signatures, exists := sc.pendingSignatures[blockHash]; exists {
		now := time.Now()
		for nodeID, sigInfo := range signatures {
			if now.Sub(sigInfo.Timestamp) > sc.signatureTimeout {
				delete(signatures, nodeID)
			}
		}

		if len(signatures) == 0 {
			delete(sc.pendingSignatures, blockHash)
		}
	}
}

// GetSignatureCount returns the signature count for the given block
func (sc *SignatureCollector) GetSignatureCount(blockHash string) int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if signatures, exists := sc.pendingSignatures[blockHash]; exists {
		return len(signatures)
	}
	return 0
}

// GetAllPendingBlocks returns all blocks awaiting signatures
func (sc *SignatureCollector) GetAllPendingBlocks() []string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	var blocks []string
	for blockHash := range sc.pendingSignatures {
		blocks = append(blocks, blockHash)
	}
	return blocks
}
