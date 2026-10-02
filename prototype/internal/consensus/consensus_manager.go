package consensus

import (
	"log"
	"sfchain/pkg/types"
	"sync"
	"time"
)

// ConsensusManager is the consensus manager
type ConsensusManager struct {
	node               ConsensusNodeInterface
	networkNodes       map[string]*types.NodeInfo
	signatureCollector *SignatureCollector
	mu                 sync.RWMutex
	txPools            map[types.TransactionType]*TransactionPool
}

// NewConsensusManager creates a new consensus manager
func NewConsensusManager(node ConsensusNodeInterface) *ConsensusManager {
	cm := &ConsensusManager{
		node:               node,
		networkNodes:       make(map[string]*types.NodeInfo),
		signatureCollector: NewSignatureCollector(),
		txPools: map[types.TransactionType]*TransactionPool{
			types.TransactionTypeManagement:  NewTransactionPool(),
			types.TransactionTypeDevelopment: NewTransactionPool(),
			types.TransactionTypeTest:        NewTransactionPool(),
			types.TransactionTypeOperations:  NewTransactionPool(),
		},
	}

	// start the block generator
	go cm.blockGenerationLoop()
	return cm
}

// blockGenerationLoop is the block generation loop
func (cm *ConsensusManager) blockGenerationLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// generate blocks of different types in parallel
			go cm.generateBlock(types.TransactionTypeManagement)
			go cm.generateBlock(types.TransactionTypeDevelopment)
			go cm.generateBlock(types.TransactionTypeTest)
			go cm.generateBlock(types.TransactionTypeOperations)
		}
	}
}

// generateBlock generates a block
func (cm *ConsensusManager) generateBlock(txType types.TransactionType) {
	pool := cm.txPools[txType]
	txs := pool.GetTransactions(10) // package at most 10 transactions per block

	if len(txs) == 0 {
		return
	}

	// 1. get the current block height and previous block hash
	lastBlock := cm.node.GetLastBlock(txType)
	var height int
	var prevHash string
	if lastBlock != nil {
		height = lastBlock.Header.BlockHeight + 1
		prevHash = lastBlock.Header.CalculateHash()
	} else {
		height = 0
		prevHash = "0"
	}

	// 2. create the new block
	// convert the pointer slice to a non-pointer slice
	var transactionSlice []types.Transaction
	for _, tx := range txs {
		transactionSlice = append(transactionSlice, *tx)
	}

	block := types.NewBlock(
		height,
		prevHash,
		int(txType),
		transactionSlice,
	)

	// 3. sign the block
	if err := cm.node.SignBlock(block); err != nil {
		log.Printf("failed to sign %s block: %v", txType.String(), err)
		return
	}

	// 4. broadcast the block to the network (full block or header depending on node type)
	if err := cm.broadcastBlockToNetwork(block); err != nil {
		log.Printf("failed to broadcast %s block: %v", txType.String(), err)
		return
	}

	// 5. start the signature collection timeout timer
	go cm.collectSignaturesWithTimeout(block, txType)

	log.Printf("generated %s block (height: %d) with %d transactions, waiting for node signatures",
		txType.String(), height, len(txs))
}

// broadcastBlockToNetwork broadcasts a block according to node type
func (cm *ConsensusManager) broadcastBlockToNetwork(block *types.Block) error {
	registeredNodes := cm.node.GetRegisteredNodes()

	for _, nodeInfo := range registeredNodes {
		if nodeInfo.NodeID == cm.node.GetNodeID() {
			continue // do not send to self
		}

		// decide what to send based on the receiving node type
		if nodeInfo.NodeType == types.NodeTypeManagement {
			// send the full block to the management node
			if err := cm.sendFullBlock(nodeInfo, block); err != nil {
				log.Printf("failed to send full block to %s: %v", nodeInfo.NodeID, err)
			}
		} else {
			// send the block header to other node types
			if err := cm.sendBlockHeader(nodeInfo, block.Header); err != nil {
				log.Printf("failed to send block header to %s: %v", nodeInfo.NodeID, err)
			}
		}
	}
	return nil
}

// sendFullBlock sends a full block to the given node
func (cm *ConsensusManager) sendFullBlock(nodeInfo *types.NodeInfo, block *types.Block) error {
	// get the network manager from the management node and send the full block
	if mn, ok := cm.node.(interface {
		GetNetworkManager() interface {
			SendBlock(*types.Block, bool, *types.NodeInfo, string) error
		}
	}); ok {
		blockHash := block.Header.CalculateHash()
		// assume the management node has a method to get the management signature
		if signer, ok := cm.node.(interface{ GetManagerSignature(string) string }); ok {
			managerSignature := signer.GetManagerSignature(blockHash)
			return mn.GetNetworkManager().SendBlock(block, true, nodeInfo, managerSignature)
		}
	}
	log.Printf("sent full block to %s (%s)", nodeInfo.NodeID, nodeInfo.NodeType)
	return nil
}

// sendBlockHeader sends a block header to the given node
func (cm *ConsensusManager) sendBlockHeader(nodeInfo *types.NodeInfo, header *types.BlockHeader) error {
	// get the network manager from the management node and send the block header
	if mn, ok := cm.node.(interface {
		GetNetworkManager() interface {
			SendBlockHeader(*types.BlockHeader, *types.NodeInfo) error
		}
	}); ok {
		return mn.GetNetworkManager().SendBlockHeader(header, nodeInfo)
	}
	log.Printf("sent block header to %s (%s)", nodeInfo.NodeID, nodeInfo.NodeType)
	return nil
}

// collectSignaturesWithTimeout collects signatures (with timeout)
func (cm *ConsensusManager) collectSignaturesWithTimeout(block *types.Block, txType types.TransactionType) {
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	blockHash := block.Header.CalculateHash()

	// wait for signature collection, checking every 100ms
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// compute the required signature count (all 4 nodes)
	requiredSignatures := 4

	for {
		select {
		case <-timeout.C:
			// timeout handling
			signatureCount := cm.signatureCollector.GetSignatureCount(blockHash)
			log.Printf("timed out collecting %s block signatures (block hash: %s), collected %d/%d signatures",
				txType.String(), SafeSubstring(blockHash, 16), signatureCount, requiredSignatures)
			cm.signatureCollector.RemoveSignatures(blockHash)
			return

		case <-ticker.C:
			// check whether enough signatures were collected
			signatureCount := cm.signatureCollector.GetSignatureCount(blockHash)
			if signatureCount >= requiredSignatures {
				// enough signatures collected; aggregate them
				signatures := cm.signatureCollector.GetSignatures(blockHash)
				aggregatedSig := cm.aggregateSignatures(signatures)

				// update the block header's aggregate signature
				block.Header.AggregatedSignature = aggregatedSig

				// height/chain are used by performance analysis scripts to correlate per block (consensus completion = the moment just before persistence)
				log.Printf("block %s consensus completed, height=%d, chain=%s, aggregateSignature: %s, collected %d/%d signatures",
					SafeSubstring(blockHash, 16), block.Header.BlockHeight, txType.String(),
					SafeSubstring(aggregatedSig, 16), signatureCount, requiredSignatures)
				return
			} else {
				// record signature collection progress
				log.Printf("collecting %s block signatures (block hash: %s), collected %d/%d signatures",
					txType.String(), SafeSubstring(blockHash, 16), signatureCount, requiredSignatures)
			}
		}
	}
}

// aggregateSignatures aggregates signatures (BLS implementation)
func (cm *ConsensusManager) aggregateSignatures(signatures map[string]*SignatureInfo) string {
	// 1. build the signature map (nodeID -> signature string)
	sigMap := make(map[string]string)
	for nodeID, sigInfo := range signatures {
		sigMap[nodeID] = sigInfo.Signature
	}

	// 2. aggregate signatures with BLS
	aggregatedSig, err := cm.node.AggregateBLSSignatures(sigMap)
	if err != nil {
		log.Printf("BLS signature aggregation failed: %v", err)
		return ""
	}

	// 3. return the aggregate signature as hex
	return aggregatedSig
}

// AddTransaction adds a transaction to the database transaction pool
func (cm *ConsensusManager) AddTransaction(tx *types.Transaction) {
	// transactions are now stored directly in the database transaction pool; the in-memory pool is no longer used
	log.Printf("transaction %s added to database transaction pool", tx.TXID[:16])
}

// GetUnendorsedTransactions returns all unendorsed transactions
func (cm *ConsensusManager) GetUnendorsedTransactions() []*types.Transaction {
	// this method is now used by the standalone endorsement service
	// the management node no longer needs to fetch unendorsed transactions
	return nil
}

// TransactionPool is a transaction pool
type TransactionPool struct {
	txs []*types.Transaction
	mu  sync.RWMutex
}

// NewTransactionPool creates a transaction pool
func NewTransactionPool() *TransactionPool {
	return &TransactionPool{
		txs: make([]*types.Transaction, 0),
	}
}

// AddTransaction adds a transaction
func (tp *TransactionPool) AddTransaction(tx *types.Transaction) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.txs = append(tp.txs, tx)
}

// GetTransactions returns endorsed transactions (checking that endorsement users are the operator)
func (tp *TransactionPool) GetTransactions(max int) []*types.Transaction {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	if len(tp.txs) == 0 {
		return nil
	}

	// collect only endorsed transactions that pass verification
	var validEndorsedTxs []*types.Transaction

	for _, tx := range tp.txs {
		if len(tx.Endorsements) > 0 && tp.validateEndorsements(tx) {
			validEndorsedTxs = append(validEndorsedTxs, tx)
		}
	}

	if len(validEndorsedTxs) == 0 {
		return nil
	}

	// sort by timestamp (earliest first)
	tp.sortByTimestamp(validEndorsedTxs)

	count := len(validEndorsedTxs)
	if count > max {
		count = max
	}

	txs := validEndorsedTxs[:count]

	// remove fetched transactions from the pool
	for _, tx := range txs {
		tp.removeTransaction(tx.TXID)
	}

	return txs
}

// validateEndorsements checks that endorsement users are the transaction operator
func (tp *TransactionPool) validateEndorsements(tx *types.Transaction) bool {
	// get the transaction operator ID
	operatorID, ok := tx.LogData["user_id"].(string)
	if !ok || operatorID == "" {
		return false // no operator information; validation fails
	}

	// check each endorsement user against the operator
	for _, endorsement := range tx.Endorsements {
		if endorsement.UserID != operatorID {
			log.Printf("warning: transaction %s has endorsement user %s that is not the operator %s",
				tx.TXID[:16], endorsement.UserID, operatorID)
			return false
		}
	}

	return true
}

// sortByTimestamp sorts by timestamp (earliest first)
func (tp *TransactionPool) sortByTimestamp(txs []*types.Transaction) {
	for i := 0; i < len(txs)-1; i++ {
		for j := i + 1; j < len(txs); j++ {
			if txs[i].Timestamp > txs[j].Timestamp {
				txs[i], txs[j] = txs[j], txs[i]
			}
		}
	}
}

// GetUnendorsedTransactions returns unendorsed transactions
func (tp *TransactionPool) GetUnendorsedTransactions() []*types.Transaction {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	var unendorsed []*types.Transaction
	for _, tx := range tp.txs {
		if len(tx.Endorsements) == 0 {
			unendorsed = append(unendorsed, tx)
		}
	}
	return unendorsed
}

// removeTransaction removes the given transaction from the pool
func (tp *TransactionPool) removeTransaction(txID string) {
	for i, tx := range tp.txs {
		if tx.TXID == txID {
			tp.txs = append(tp.txs[:i], tp.txs[i+1:]...)
			return
		}
	}
}
