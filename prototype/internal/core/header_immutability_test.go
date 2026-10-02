package core

import (
	"sfchain/pkg/types"
	"testing"
)

func TestCachedHeaderLookupDoesNotRewriteSignedCommitment(t *testing.T) {
	h := &types.BlockHeader{BlockHeight: 1, ChainType: 1, Timestamp: 1234}
	b := &types.Block{Header: h}
	digest := h.CalculateHash()
	node := &ManagementNode{
		blockCache:                  map[string]*types.Block{digest: b},
		pendingAggregatedSignatures: map[string]string{"management:1": "aggregate-for-successor"},
	}
	found := node.getBlockByHeight(1, types.TransactionTypeManagement)
	if found == nil || found.Header.CalculateHash() != digest || found.Header.AggregatedSignature != "" {
		t.Fatal("lookup rewrote the sealed header with its own aggregate")
	}
}

func TestSealedTransactionCannotBeReadmitted(t *testing.T) {
	tx := &types.Transaction{TXID: "one", TxType: types.TransactionTypeManagement}
	pool := &TransactionPoolDB{
		packagingPool:        map[types.TransactionType][]*types.Transaction{},
		pendingStatusUpdates: map[string]*types.Transaction{},
	}
	pool.AddToPackagingPool(tx)
	pool.MarkTransactionsAsBlocked(tx.TxType, []string{tx.TXID})
	pool.AddToPackagingPool(tx)
	pool.AddToPackagingPoolBatch([]*types.Transaction{tx})
	if pool.GetPackagingPoolSize(tx.TxType) != 0 {
		t.Fatal("sealed transaction was readmitted after a status-write retry")
	}
}
