package database

import (
	"fmt"
	"log"
	"sfchain/pkg/types"
	"sync"
)

// TransactionPoolDatabase is the in-memory transaction pool database
type TransactionPoolDatabase struct {
	transactions map[string]*types.Transaction
	mu           sync.RWMutex
}

// NewTransactionPoolDatabase creates a transaction pool database
func NewTransactionPoolDatabase(dataPath string) (*TransactionPoolDatabase, error) {
	return &TransactionPoolDatabase{
		transactions: make(map[string]*types.Transaction),
	}, nil
}

// Close closes the database
func (tpd *TransactionPoolDatabase) Close() error {
	tpd.mu.Lock()
	defer tpd.mu.Unlock()
	return nil
}

// AddTransaction adds a transaction to the pool
func (tpd *TransactionPoolDatabase) AddTransaction(tx *types.Transaction) error {
	tpd.mu.Lock()
	defer tpd.mu.Unlock()

	tpd.transactions[tx.TXID] = tx

	txIDDisplay := tx.TXID
	if len(tx.TXID) > 16 {
		txIDDisplay = tx.TXID[:16]
	}
	log.Printf("Transaction added to pool: %s", txIDDisplay)
	return nil
}

// GetUnendorsedTransactions returns all transactions without endorsements
func (tpd *TransactionPoolDatabase) GetUnendorsedTransactions() ([]interface{}, error) {
	tpd.mu.RLock()
	defer tpd.mu.RUnlock()

	var transactions []interface{}
	for _, tx := range tpd.transactions {
		if len(tx.Endorsements) == 0 {
			transactions = append(transactions, tx)
		}
	}

	return transactions, nil
}

// UpdateTransactionEndorsements updates a transaction's endorsements
func (tpd *TransactionPoolDatabase) UpdateTransactionEndorsements(txID string, endorsements interface{}) error {
	tpd.mu.Lock()
	defer tpd.mu.Unlock()

	tx, exists := tpd.transactions[txID]
	if !exists {
		return WrapDatabaseError("UpdateTransactionEndorsements", fmt.Errorf("transaction not found: %s", txID))
	}

	// Convert endorsements to []types.Endorsement
	if endos, ok := endorsements.([]types.Endorsement); ok {
		tx.Endorsements = endos
	} else {
		return WrapDatabaseError("UpdateTransactionEndorsements", fmt.Errorf("invalid endorsement format"))
	}

	return nil
}

// GetTransaction returns the transaction with the given ID
func (tpd *TransactionPoolDatabase) GetTransaction(txID string) (*types.Transaction, error) {
	tpd.mu.RLock()
	defer tpd.mu.RUnlock()

	tx, exists := tpd.transactions[txID]
	if !exists {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}

	return tx, nil
}

// RemoveTransaction removes a transaction from the pool
func (tpd *TransactionPoolDatabase) RemoveTransaction(txID string) error {
	tpd.mu.Lock()
	defer tpd.mu.Unlock()

	delete(tpd.transactions, txID)
	log.Printf("Transaction removed from pool: %s", txID[:16])
	return nil
}

// GetAllTransactions returns all transactions (for debugging)
func (tpd *TransactionPoolDatabase) GetAllTransactions() []*types.Transaction {
	tpd.mu.RLock()
	defer tpd.mu.RUnlock()

	transactions := make([]*types.Transaction, 0, len(tpd.transactions))
	for _, tx := range tpd.transactions {
		transactions = append(transactions, tx)
	}

	return transactions
}

// GetTransactionsByType returns transactions of the given type
func (tpd *TransactionPoolDatabase) GetTransactionsByType(txType interface{}) []*types.Transaction {
	tpd.mu.RLock()
	defer tpd.mu.RUnlock()

	var transactions []*types.Transaction
	for _, tx := range tpd.transactions {
		if tx.TxType == txType {
			transactions = append(transactions, tx)
		}
	}

	return transactions
}

// GetPendingTransactions returns pending transactions
func (tpd *TransactionPoolDatabase) GetPendingTransactions(limit int) []*types.Transaction {
	tpd.mu.RLock()
	defer tpd.mu.RUnlock()

	var transactions []*types.Transaction
	for _, tx := range tpd.transactions {
		if len(tx.Endorsements) == 0 {
			transactions = append(transactions, tx)
			if limit > 0 && len(transactions) >= limit {
				break
			}
		}
	}

	return transactions
}

// UpdateTransactionStatus updates a transaction's status
func (tpd *TransactionPoolDatabase) UpdateTransactionStatus(txID, status string) error {
	tpd.mu.Lock()
	defer tpd.mu.Unlock()

	_, exists := tpd.transactions[txID]
	if !exists {
		return WrapDatabaseError("UpdateTransactionStatus", fmt.Errorf("transaction not found: %s", txID))
	}

	// A status field can be added here if needed
	return nil
}

// GetTransactionCount returns the number of transactions
func (tpd *TransactionPoolDatabase) GetTransactionCount() int {
	tpd.mu.RLock()
	defer tpd.mu.RUnlock()

	return len(tpd.transactions)
}

// Clear empties the transaction pool
func (tpd *TransactionPoolDatabase) Clear() error {
	tpd.mu.Lock()
	defer tpd.mu.Unlock()

	tpd.transactions = make(map[string]*types.Transaction)
	return nil
}
