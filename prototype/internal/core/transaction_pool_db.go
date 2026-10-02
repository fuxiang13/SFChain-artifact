package core

import (
	"log"
	"sfchain/internal/database"
	"sfchain/internal/database/mysql"
	"sfchain/pkg/config"
	"sfchain/pkg/types"
	"sync"
	"time"
)

// TransactionPoolDB wraps the transaction pool database interface
type TransactionPoolDB struct {
	db database.TransactionPoolDatabaseInterface

	// packaging pool: caches endorsed transactions; key is the transaction type, value is the transaction slice
	packagingPool map[types.TransactionType][]*types.Transaction
	packagingMu   sync.RWMutex
	// Session-local replay guard: SQL status retries must not repackage a
	// transaction that has already been sealed. Protected by packagingMu.
	sealedIDs map[string]bool

	// user public key cache: userID -> publicKey
	userPublicKeys map[string]string
	userKeyMu      sync.RWMutex

	// pending status update queue: after a transaction is packaged, its status change is queued here for asynchronous database update
	pendingStatusUpdates map[string]*types.Transaction // txID -> transaction with updated status
	pendingMu            sync.RWMutex
}

// NewTransactionPoolDB creates the transaction pool database wrapper
func NewTransactionPoolDB(cfg *config.DatabaseConfig) (*TransactionPoolDB, error) {
	dbFactory := database.NewDBFactory(cfg)
	db, err := dbFactory.CreateTransactionPoolDatabase()
	if err != nil {
		return nil, err
	}
	return &TransactionPoolDB{
		db:                   db,
		packagingPool:        make(map[types.TransactionType][]*types.Transaction),
		sealedIDs:            make(map[string]bool),
		userPublicKeys:       make(map[string]string),
		pendingStatusUpdates: make(map[string]*types.Transaction),
	}, nil
}

// AddTransaction adds a transaction to the transaction pool database
func (tpd *TransactionPoolDB) AddTransaction(tx *types.Transaction) error {
	if tpd.db == nil {
		return nil
	}
	return tpd.db.AddTransaction(tx)
}

// GetEndorsedTransactions fetches endorsed transactions from the packaging pool
func (tpd *TransactionPoolDB) GetEndorsedTransactions(txType types.TransactionType, max int) ([]*types.Transaction, error) {
	tpd.packagingMu.Lock()
	txs, exists := tpd.packagingPool[txType]
	var out []*types.Transaction
	if exists && len(txs) > 0 {
		if len(txs) <= max {
			out = txs
		} else {
			out = txs[:max]
		}
	}
	tpd.packagingMu.Unlock()
	// hot path: no log IO while holding the lock
	if !exists || len(txs) == 0 {
		return []*types.Transaction{}, nil
	}
	return out, nil
}

// DumpPackagingPoolState prints the state of all packaging pools
func (tpd *TransactionPoolDB) DumpPackagingPoolState() {
	tpd.packagingMu.RLock()
	defer tpd.packagingMu.RUnlock()

	log.Printf("[packaging pool - state] ===== full packaging pool state =====")
	for txType := types.TransactionTypeManagement; txType <= types.TransactionTypeOperations; txType++ {
		txs := tpd.packagingPool[txType]
		log.Printf("[packaging pool - state] %s chain: %d transactions", txType.String(), len(txs))

		// show details of the first 5 transactions
		showCount := len(txs)
		if showCount > 5 {
			showCount = 5
		}
		for i := 0; i < showCount; i++ {
			tx := txs[i]
			log.Printf("   [%d] TXID=%s, Endorsed=%v, UpdatedAt=%.2f",
				i, SafeSubstring(tx.TXID, 16), tx.Endorsed, tx.UpdatedAt)
		}
		if len(txs) > 5 {
			log.Printf("   ... %d more", len(txs)-5)
		}
	}
	log.Printf("[packaging pool - state] ===== end of packaging pool state =====")
}

// LoadEndorsedTransactionsToPool loads all endorsed transactions from the database into the packaging pool
func (tpd *TransactionPoolDB) LoadEndorsedTransactionsToPool() error {
	if tpd.db == nil {
		return nil
	}

	// iterate all transaction types
	chainTypes := []types.TransactionType{
		types.TransactionTypeManagement,
		types.TransactionTypeDevelopment,
		types.TransactionTypeTest,
		types.TransactionTypeOperations,
	}

	tpd.packagingMu.Lock()
	defer tpd.packagingMu.Unlock()

	for _, chainType := range chainTypes {
		tpd.loadEndorsedTransactionsFromDBLocked(chainType)
	}

	return nil
}

// loadEndorsedTransactionsFromDBLocked loads endorsed transactions of the given type from the database (caller must hold the lock)
func (tpd *TransactionPoolDB) loadEndorsedTransactionsFromDBLocked(txType types.TransactionType) error {
	if tpd.db == nil {
		return nil
	}

	endorsedTxsInterface, err := tpd.db.GetEndorsedTransactions(txType.String(), 0)
	if err != nil {
		return err
	}

	var txs []*types.Transaction
	for _, txInterface := range endorsedTxsInterface {
		if tx, ok := txInterface.(*types.Transaction); ok && !tpd.sealedIDs[tx.TXID] {
			txs = append(txs, tx)
		}
	}

	// if the database has no endorsed transactions (e.g. endorsement-service push mode: status not persisted), or the in-memory packaging pool already has
	// pending transactions (in push mode the DB snapshot lacks endorsements; overwriting would lose them), skip the overwrite
	if len(txs) == 0 || len(tpd.packagingPool[txType]) > 0 {
		return nil
	}

	tpd.packagingPool[txType] = txs
	return nil
}

// LoadUserPublicKeysToCache loads all users' public keys from the database into the cache
func (tpd *TransactionPoolDB) LoadUserPublicKeysToCache(userDB database.UserDatabaseInterface) error {
	if userDB == nil {
		return nil
	}

	// get all users
	users, err := userDB.GetAllUsers()
	if err != nil {
		return err
	}

	tpd.userKeyMu.Lock()
	defer tpd.userKeyMu.Unlock()

	for _, userInterface := range users {
		if user, ok := userInterface.(*mysql.User); ok {
			tpd.userPublicKeys[user.UserID] = user.PublicKey
		} else if user, ok := userInterface.(*database.User); ok {
			tpd.userPublicKeys[user.UserID] = user.PublicKey
		}
	}

	return nil
}

// GetUserPublicKey returns a user's public key from the cache
func (tpd *TransactionPoolDB) GetUserPublicKey(userID string) (string, bool) {
	tpd.userKeyMu.RLock()
	defer tpd.userKeyMu.RUnlock()

	pubKey, exists := tpd.userPublicKeys[userID]
	return pubKey, exists
}

// RemoveFromPackagingPool removes packaged transactions from the packaging pool
func (tpd *TransactionPoolDB) RemoveFromPackagingPool(txType types.TransactionType, txIDs []string) {
	tpd.packagingMu.Lock()
	defer tpd.packagingMu.Unlock()

	txs, exists := tpd.packagingPool[txType]
	if !exists {
		return
	}

	// build a txID set for fast lookup
	txIDSet := make(map[string]bool)
	if tpd.sealedIDs == nil {
		tpd.sealedIDs = make(map[string]bool)
	}
	for _, id := range txIDs {
		txIDSet[id] = true
		tpd.sealedIDs[id] = true
	}

	// filter out packaged transactions
	var remaining []*types.Transaction
	for _, tx := range txs {
		if !txIDSet[tx.TXID] {
			remaining = append(remaining, tx)
		}
	}

	tpd.packagingPool[txType] = remaining
	log.Printf("[packaging pool] removed %d %s transactions from the packaging pool, %d remaining", len(txIDs), txType.String(), len(remaining))
}

// MarkTransactionsAsBlocked marks transactions as packaged (blocked)
// 1. remove transactions from the packaging pool
// 2. put transactions into the pending update queue (keeping each transaction's own UpdatedAt), awaiting asynchronous persistence to the database
func (tpd *TransactionPoolDB) MarkTransactionsAsBlocked(txType types.TransactionType, txIDs []string) {
	if len(txIDs) == 0 {
		return
	}

	tpd.packagingMu.Lock()
	defer tpd.packagingMu.Unlock()

	tpd.pendingMu.Lock()
	defer tpd.pendingMu.Unlock()

	// build a txID set for fast lookup
	txIDSet := make(map[string]bool)
	if tpd.sealedIDs == nil {
		tpd.sealedIDs = make(map[string]bool)
	}
	for _, id := range txIDs {
		txIDSet[id] = true
		tpd.sealedIDs[id] = true
	}

	// filter out packaged transactions and add them to the pending update queue
	var remaining []*types.Transaction
	for _, tx := range tpd.packagingPool[txType] {
		if txIDSet[tx.TXID] {
			// tx.UpdatedAt was already set to its own time in generateBlock
			// add to the pending update queue
			tpd.pendingStatusUpdates[tx.TXID] = tx
		} else {
			remaining = append(remaining, tx)
		}
	}

	tpd.packagingPool[txType] = remaining
	log.Printf("[packaging pool] marked %d %s transactions as blocked, added to the pending update queue", len(txIDs), txType.String())
}

// GetPendingStatusUpdates returns pending transactions and removes them from the queue
// returns all transactions pending a database update
func (tpd *TransactionPoolDB) GetPendingStatusUpdates() ([]string, map[string]float64, error) {
	tpd.pendingMu.Lock()
	defer tpd.pendingMu.Unlock()

	if len(tpd.pendingStatusUpdates) == 0 {
		return nil, nil, nil
	}

	txIDs := make([]string, 0, len(tpd.pendingStatusUpdates))
	updatedAts := make(map[string]float64)

	for txID, tx := range tpd.pendingStatusUpdates {
		txIDs = append(txIDs, txID)
		updatedAts[txID] = tx.UpdatedAt
	}

	// clear the pending update queue
	tpd.pendingStatusUpdates = make(map[string]*types.Transaction)

	log.Printf("[pending update queue] got %d pending transactions", len(txIDs))
	return txIDs, updatedAts, nil
}

// AddToPackagingPool adds a transaction to the packaging pool (called when a new transaction is endorsed)
// hot path: no log IO while holding the lock, avoiding lock contention with the sealing loop amplifying latency
func (tpd *TransactionPoolDB) AddToPackagingPool(tx *types.Transaction) {
	if tx == nil {
		return
	}

	tpd.packagingMu.Lock()
	if tpd.sealedIDs[tx.TXID] {
		tpd.packagingMu.Unlock()
		return
	}
	for _, existing := range tpd.packagingPool[tx.TxType] {
		if existing != nil && existing.TXID == tx.TXID {
			tpd.packagingMu.Unlock()
			return
		}
	}
	tpd.packagingPool[tx.TxType] = append(tpd.packagingPool[tx.TxType], tx)
	n := len(tpd.packagingPool[tx.TxType])
	tpd.packagingMu.Unlock()

	if n == 1 || n%500 == 0 {
		log.Printf("[packaging pool] %s chain now has %d pending transactions", tx.TxType.String(), n)
	}
}

// AddToPackagingPoolBatch adds a batch of transactions to the packaging pool (push-mode hot path: single lock, whole batch enqueued)
func (tpd *TransactionPoolDB) AddToPackagingPoolBatch(txs []*types.Transaction) {
	if len(txs) == 0 {
		return
	}
	tpd.packagingMu.Lock()
	for _, tx := range txs {
		if tx != nil && !tpd.sealedIDs[tx.TXID] {
			duplicate := false
			for _, existing := range tpd.packagingPool[tx.TxType] {
				if existing != nil && existing.TXID == tx.TXID {
					duplicate = true
					break
				}
			}
			if !duplicate {
				tpd.packagingPool[tx.TxType] = append(tpd.packagingPool[tx.TxType], tx)
			}
		}
	}
	tpd.packagingMu.Unlock()
}

// GetPackagingPoolSize returns the packaging pool size
func (tpd *TransactionPoolDB) GetPackagingPoolSize(txType types.TransactionType) int {
	tpd.packagingMu.RLock()
	defer tpd.packagingMu.RUnlock()

	return len(tpd.packagingPool[txType])
}

// GetAllPackagingPoolSize returns the total size of all packaging pools
func (tpd *TransactionPoolDB) GetAllPackagingPoolSize() int {
	tpd.packagingMu.RLock()
	defer tpd.packagingMu.RUnlock()

	total := 0
	for _, txs := range tpd.packagingPool {
		total += len(txs)
	}
	return total
}

// GetEndorsedTransactionsCount returns the number of endorsed transactions
func (tpd *TransactionPoolDB) GetEndorsedTransactionsCount(txType types.TransactionType) (int, error) {
	// return the actual in-memory count rather than the database count
	// this avoids the timer still thinking there are enough transactions after MarkTransactionsAsBlocked removed them
	tpd.packagingMu.RLock()
	defer tpd.packagingMu.RUnlock()

	txs, exists := tpd.packagingPool[txType]
	if !exists {
		return 0, nil
	}
	return len(txs), nil
}

// validateEndorsements checks that endorsement users are the transaction operator
func (tpd *TransactionPoolDB) validateEndorsements(tx *types.Transaction) bool {
	// get the transaction operator ID
	operatorID := ""

	// prefer the transaction's UserID field
	if tx.UserID != "" {
		operatorID = tx.UserID
	} else {
		// try to get the UserID from LogData
		if id, ok := tx.LogData["UserID"].(string); ok && id != "" {
			operatorID = id
		} else if id, ok := tx.LogData["user_id"].(string); ok && id != "" {
			operatorID = id
		} else {
			log.Printf("warning: transaction %s has no operator information", tx.TXID[:16])
			return false // no operator information; validation fails
		}
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

// RemoveTransactions removes transactions from the pool
func (tpd *TransactionPoolDB) RemoveTransactions(txIDs []string) error {
	if tpd.db == nil {
		return nil
	}

	for _, txID := range txIDs {
		if err := tpd.db.RemoveTransaction(txID); err != nil {
			log.Printf("failed to remove transaction %s: %v", txID[:16], err)
		}
	}
	return nil
}

// UpdateTransactionStatus updates a transaction status
func (tpd *TransactionPoolDB) UpdateTransactionStatus(txID string, status string) error {
	if tpd.db == nil {
		return nil
	}
	return tpd.db.UpdateTransactionStatus(txID, status)
}

// AddTransactionsBatch writes transactions to the pool in batch (multi-row INSERT)
func (tpd *TransactionPoolDB) AddTransactionsBatch(txs []interface{}) error {
	if tpd.db == nil || len(txs) == 0 {
		return nil
	}
	return tpd.db.AddTransactionsBatch(txs)
}

// UpdateTransactionsEndorsementsBatch writes back endorsements in batch (single UPDATE ... CASE)
func (tpd *TransactionPoolDB) UpdateTransactionsEndorsementsBatch(txs []interface{}) error {
	if tpd.db == nil || len(txs) == 0 {
		return nil
	}
	return tpd.db.UpdateTransactionsEndorsementsBatch(txs)
}

// UpdateTransactionsStatusBatch updates transaction statuses in batch
func (tpd *TransactionPoolDB) UpdateTransactionsStatusBatch(txIDs []string, status string, updatedAt time.Time) error {
	if tpd.db == nil || len(txIDs) == 0 {
		return nil
	}

	// call the underlying batch update method
	return tpd.db.UpdateTransactionsStatusBatch(txIDs, status, updatedAt)
}

// Close closes the database
func (tpd *TransactionPoolDB) Close() error {
	if tpd.db == nil {
		return nil
	}
	return tpd.db.Close()
}
