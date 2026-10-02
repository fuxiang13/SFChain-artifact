package types

// NetworkMessage is the base network message structure
type NetworkMessage struct {
	Type  string      `json:"type"`  // block, block_header, signature, etc.
	Chain int         `json:"chain"` // TransactionType
	Data  interface{} `json:"data"`  // actual payload
	From  string      `json:"from"`  // sending node ID
	To    string      `json:"to"`    // receiving node ID (optional)
}

// BlockMessage is the block message (carrying the management node signature and the previous block aggregate signature)
type BlockMessage struct {
	Block                 *Block `json:"block"`
	IsFull                bool   `json:"is_full"`                       // whether this is a full block
	ManagerSignature      string `json:"manager_signature"`             // management node signature over the block header
	PreviousBlockHeight   int64  `json:"previous_block_height"`         // height of the previous block (for lookup)
	PreviousAggregatedSig string `json:"previous_aggregated_signature"` // aggregate signature of the previous block
}

// BlockHeaderMessage is the block-header message (carrying the management node signature and the previous block aggregate signature)
type BlockHeaderMessage struct {
	Header                *BlockHeader `json:"header"`
	ManagerSignature      string       `json:"manager_signature"`             // management node signature over the block header
	PreviousBlockHeight   int64        `json:"previous_block_height"`         // height of the previous block (for lookup)
	PreviousAggregatedSig string       `json:"previous_aggregated_signature"` // aggregate signature of the previous block
}

// SignatureMessage is the signature message
type SignatureMessage struct {
	BlockHash string `json:"block_hash"`
	Signature string `json:"signature"`
	PublicKey string `json:"public_key"`
}

// AggregatedSignatureMessage is the aggregate-signature message
type AggregatedSignatureMessage struct {
	BlockHeight int64  `json:"block_height"`
	Signature   string `json:"signature"`
}

// BlockConfirmationMessage is the block-processing confirmation message
// A DTO node sends this confirmation after successfully persisting a block
type BlockConfirmationMessage struct {
	BlockHash string `json:"block_hash"` // hash of the confirmed block
	Height    int64  `json:"height"`     // block height
}
