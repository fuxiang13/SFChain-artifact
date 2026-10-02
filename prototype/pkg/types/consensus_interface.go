package types

// ConsensusNodeInterface is the consensus node interface
type ConsensusNodeInterface interface {
	GetNodeID() string
	GetRegisteredNodes() map[string]*NodeInfo
	ReceiveSignature(chainType TransactionType, blockHash, nodeID, signature, publicKey string)
	ReceiveBlockConfirmation(chainType TransactionType, blockHash string, nodeID string, height int64)
	UpdateAggregatedSignature(chainType TransactionType, blockHeight int64, signature string)
	ProcessOtherChainHeader(header *BlockHeader, chainType TransactionType)
}
