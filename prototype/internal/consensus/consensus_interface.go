package consensus

import "sfchain/pkg/types"

// ConsensusNodeInterface is the consensus node interface
type ConsensusNodeInterface interface {
	GetNodeID() string
	GetNodeType() types.NodeType
	BroadcastBlock(block *types.Block) error
	GetRegisteredNodes() map[string]*types.NodeInfo
	ReceiveSignature(chainType types.TransactionType, blockHash, nodeID, signature, publicKey string)
	ReceiveBlockConfirmation(chainType types.TransactionType, blockHash string, nodeID string, height int64)
	UpdateAggregatedSignature(chainType types.TransactionType, blockHeight int64, signature string)
	ProcessOtherChainHeader(header *types.BlockHeader, chainType types.TransactionType)

	// Added methods
	GetLastBlock(txType types.TransactionType) *types.Block
	SignBlock(block *types.Block) error
	AggregateBLSSignatures(signatures map[string]string) (string, error)
}
