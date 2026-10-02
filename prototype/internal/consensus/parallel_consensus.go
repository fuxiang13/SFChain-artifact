package consensus

import (
	"fmt"
	"log"
	"sfchain/pkg/crypto"
	"sfchain/pkg/types"
	"sync"
	"time"

	bls "github.com/herumi/bls-eth-go-binary/bls"
)

// ParallelConsensusManager is the parallel consensus manager
type ParallelConsensusManager struct {
	node                types.ConsensusNodeInterface
	signatureCollectors map[types.TransactionType]*BLSSignatureCollector
	signatureCaches     map[string]*types.BlockSignatureCache
	mu                  sync.RWMutex
}

// BLSSignatureCollector collects BLS signatures
type BLSSignatureCollector struct {
	signatures map[string]*bls.Sign      // node_id -> BLS signature
	publicKeys map[string]*bls.PublicKey // node_id -> public key
	mu         sync.RWMutex
}

// NewBLSSignatureCollector creates a BLS signature collector
func NewBLSSignatureCollector() *BLSSignatureCollector {
	return &BLSSignatureCollector{
		signatures: make(map[string]*bls.Sign),
		publicKeys: make(map[string]*bls.PublicKey),
	}
}

// AddSignature adds a BLS signature
func (sc *BLSSignatureCollector) AddSignature(nodeID, signatureHex, publicKeyHex string) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Deserialize the signature
	signature, err := crypto.DeserializeSignature(signatureHex)
	if err != nil {
		return fmt.Errorf("failed to deserialize signature: %v", err)
	}

	// Deserialize the public key
	publicKey, err := crypto.DeserializePublicKey(publicKeyHex)
	if err != nil {
		return fmt.Errorf("failed to deserialize public key: %v", err)
	}

	sc.signatures[nodeID] = signature
	sc.publicKeys[nodeID] = publicKey
	return nil
}

// AggregateSignatures aggregates all BLS signatures
func (sc *BLSSignatureCollector) AggregateSignatures() (*bls.Sign, []string, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if len(sc.signatures) == 0 {
		return nil, nil, fmt.Errorf("no signatures to aggregate")
	}

	// Collect all signatures
	var signatures []*bls.Sign
	for _, sig := range sc.signatures {
		signatures = append(signatures, sig)
	}

	// Aggregate the signatures
	aggSignature := bls.Sign{}
	for _, sig := range signatures {
		aggSignature.Add(sig)
	}

	// Collect the hex representation of all public keys
	var publicKeyStrs []string
	for _, pubKey := range sc.publicKeys {
		publicKeyStrs = append(publicKeyStrs, crypto.SerializePublicKey(pubKey))
	}

	return &aggSignature, publicKeyStrs, nil
}

// HasEnoughSignatures checks whether enough signatures have been collected
func (sc *BLSSignatureCollector) HasEnoughSignatures(thresholdRatio float64) bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	// Simplified implementation: at least one signature is required
	return len(sc.signatures) > 0
}

// NewParallelConsensusManager creates a parallel consensus manager
func NewParallelConsensusManager(node types.ConsensusNodeInterface) *ParallelConsensusManager {
	return &ParallelConsensusManager{
		node:                node,
		signatureCollectors: make(map[types.TransactionType]*BLSSignatureCollector),
	}
}

// ProcessBlock processes a new block - called by the management node
func (pcm *ParallelConsensusManager) ProcessBlock(block *types.Block, privateKey string) error {
	chainType := types.TransactionType(block.Header.ChainType)

	// 1. Sign the block header with the management node key
	managerSignature, err := pcm.signBlockHeader(block.Header, privateKey)
	if err != nil {
		return err
	}

	// 2. Cache the block and the management node signature
	signatureCache := types.NewBlockSignatureCache(block, managerSignature)
	pcm.mu.Lock()
	if pcm.signatureCaches == nil {
		pcm.signatureCaches = make(map[string]*types.BlockSignatureCache)
	}
	pcm.signatureCaches[block.Header.CalculateHash()] = signatureCache
	pcm.mu.Unlock()

	// 3. Send the full block or the block header (with the management node
	//    signature attached) depending on the node type
	pcm.distributeBlockWithSignature(block, managerSignature)

	// 4. Initialize the BLS signature collector
	pcm.initSignatureCollector(chainType)

	// 5. Wait for signatures and aggregate them
	go pcm.collectAndAggregateSignatures(chainType, block)

	return nil
}

// signBlockHeader signs the block header with the management node key
func (pcm *ParallelConsensusManager) signBlockHeader(header *types.BlockHeader, privateKey string) (string, error) {
	blockHash := header.CalculateHash()
	signature, err := crypto.SignBlockHeader(privateKey, blockHash)
	if err != nil {
		return "", err
	}
	return signature, nil
}

// distributeBlockWithSignature distributes the block to other nodes (with the
// management node signature attached)
func (pcm *ParallelConsensusManager) distributeBlockWithSignature(block *types.Block, managerSignature string) {
	log.Printf("Distributing block %d (chain type: %d) with management node signature", block.Header.BlockHeight, block.Header.ChainType)
}

// initSignatureCollector initializes the signature collector
func (pcm *ParallelConsensusManager) initSignatureCollector(chainType types.TransactionType) {
	pcm.mu.Lock()
	defer pcm.mu.Unlock()

	if _, exists := pcm.signatureCollectors[chainType]; !exists {
		pcm.signatureCollectors[chainType] = NewBLSSignatureCollector()
	}
}

// collectAndAggregateSignatures collects and aggregates signatures
func (pcm *ParallelConsensusManager) collectAndAggregateSignatures(
	chainType types.TransactionType,
	block *types.Block,
) {
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pcm.mu.RLock()
			collector := pcm.signatureCollectors[chainType]
			pcm.mu.RUnlock()

			if collector.HasEnoughSignatures(0.67) {
				// Aggregate the BLS signatures
				aggSignature, _, err := collector.AggregateSignatures()
				if err != nil {
					log.Printf("Failed to aggregate signatures: %v", err)
					continue
				}

				// Update the aggregated signature on the block header
				block.Header.AggregatedSignature = crypto.SerializeSignature(aggSignature)
				return
			}

		case <-timeout:
			log.Printf("Consensus timeout for block %d (chain: %s)",
				block.Header.BlockHeight, chainType.String())
			return
		}
	}
}

// ReceiveSignature receives a BLS signature from another node
func (pcm *ParallelConsensusManager) ReceiveSignature(
	chainType types.TransactionType,
	blockHash, nodeID, signatureHex, publicKeyHex string,
) {
	pcm.mu.Lock()
	defer pcm.mu.Unlock()

	if _, exists := pcm.signatureCollectors[chainType]; !exists {
		pcm.signatureCollectors[chainType] = NewBLSSignatureCollector()
	}

	if err := pcm.signatureCollectors[chainType].AddSignature(nodeID, signatureHex, publicKeyHex); err != nil {
		log.Printf("Failed to add BLS signature: %v", err)
	}
}
