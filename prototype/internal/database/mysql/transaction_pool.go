package mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"sfchain/pkg/types"
)

// TransactionPoolDatabase is the MySQL implementation of the transaction pool database
type TransactionPoolDatabase struct {
	db  *DB
	mu  sync.RWMutex
	txs map[string]*types.Transaction
}

// NewTransactionPoolDatabase creates a new MySQL transaction pool database
func NewTransactionPoolDatabase(config Config) (*TransactionPoolDatabase, error) {
	db, err := NewDB(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create database connection: %w", err)
	}

	txdb := &TransactionPoolDatabase{
		db:  db,
		txs: make(map[string]*types.Transaction),
	}

	// initialize the table schema
	if err := txdb.initTables(); err != nil {
		return nil, fmt.Errorf("failed to initialize table schema: %w", err)
	}

	// load all transactions
	if err := txdb.loadAllTransactions(); err != nil {
		log.Printf("failed to load transaction data: %v", err)
	}

	return txdb, nil
}

// initTables initializes the table schema
func (txdb *TransactionPoolDatabase) initTables() error {
	// create the transactions table
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS transactions (
		tx_id VARCHAR(64) PRIMARY KEY,
		data JSON NOT NULL,
		user_id VARCHAR(64) NOT NULL,
		management_signature TEXT,
		user_signature TEXT,
		type VARCHAR(32) NOT NULL,
		status VARCHAR(32) NOT NULL,
		created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
		updated_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
	`
	_, err := txdb.db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create transactions table: %w", err)
	}

	// create indexes (if not present)
	indexes := []struct {
		name  string
		query string
	}{
		{"idx_transactions_type", "CREATE INDEX idx_transactions_type ON transactions(`type`)"},
		{"idx_transactions_status", "CREATE INDEX idx_transactions_status ON transactions(status)"},
		{"idx_transactions_created_at", "CREATE INDEX idx_transactions_created_at ON transactions(created_at)"},
	}

	for _, idx := range indexes {
		// check whether the index already exists
		var count int
		err := txdb.db.QueryRow("SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'transactions' AND index_name = ?", idx.name).Scan(&count)
		if err == nil && count == 0 {
			if _, err := txdb.db.Exec(idx.query); err != nil {
				log.Printf("failed to create index %s: %v", idx.name, err)
			}
		}
	}

	log.Printf("transaction pool database table schema initialized successfully")
	return nil
}

// Close closes the database
func (txdb *TransactionPoolDatabase) Close() error {
	return txdb.db.Close()
}

// GetDB returns the database connection
func (txdb *TransactionPoolDatabase) GetDB() *DB {
	return txdb.db
}

// AddTransaction adds a transaction
func (txdb *TransactionPoolDatabase) AddTransaction(tx interface{}) error {
	txObj, ok := tx.(*types.Transaction)
	if !ok {
		return NewDatabaseError("AddTransaction", fmt.Errorf("invalid transaction type"))
	}

	if txObj.TXID == "" {
		return NewDatabaseError("AddTransaction", fmt.Errorf("transaction ID must not be empty"))
	}

	data, err := json.Marshal(txObj)
	if err != nil {
		return WrapDatabaseError("AddTransaction", fmt.Errorf("failed to serialize transaction: %w", err))
	}

	// extract the management node signature
	managementSignature := txObj.CreatorSignature

	// extract the user signature (if any)
	userSignature := ""
	if len(txObj.Endorsements) > 0 {
		userSignature = txObj.Endorsements[0].Signature
	}

	// insert into the database
	query := `INSERT INTO transactions 
		(tx_id, data, user_id, management_signature, user_signature, type, status, created_at, updated_at) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) 
		ON DUPLICATE KEY UPDATE 
			data = VALUES(data), 
			user_id = VALUES(user_id), 
			management_signature = VALUES(management_signature), 
			user_signature = VALUES(user_signature), 
			type = VALUES(type), 
			updated_at = VALUES(updated_at)`

	_, err = txdb.db.Exec(query,
		txObj.TXID,
		data,
		txObj.UserID,
		managementSignature,
		userSignature,
		txObj.TxType.String(),
		"pending",
		time.Now(),
		time.Now(),
	)
	if err != nil {
		return WrapDatabaseError("AddTransaction", fmt.Errorf("failed to save transaction: %w", err))
	}

	// update the in-memory cache
	txdb.mu.Lock()
	txdb.txs[txObj.TXID] = txObj
	txdb.mu.Unlock()

	return nil
}

// AddTransactionsBatch puts unendorsed transactions into the pool in batch (single multi-row INSERT)
func (txdb *TransactionPoolDatabase) AddTransactionsBatch(txs []interface{}) error {
	if len(txs) == 0 {
		return nil
	}
	const cols = 9 // tx_id, data, user_id, management_signature, user_signature, type, status, created_at, updated_at
	// MySQL single-statement placeholder limit is 65535; shard by row count
	const maxRows = 65000 / cols
	now := time.Now()
	total := 0
	for start := 0; start < len(txs); start += maxRows {
		end := start + maxRows
		if end > len(txs) {
			end = len(txs)
		}
		chunk := txs[start:end]

		var sb strings.Builder
		sb.WriteString(`INSERT INTO transactions
		(tx_id, data, user_id, management_signature, user_signature, type, status, created_at, updated_at)
		VALUES `)
		args := make([]interface{}, 0, len(chunk)*cols)
		for i, txInterface := range chunk {
			txObj, ok := txInterface.(*types.Transaction)
			if !ok || txObj.TXID == "" {
				return NewDatabaseError("AddTransactionsBatch", fmt.Errorf("invalid transaction type or empty transaction ID"))
			}
			data, err := json.Marshal(txObj)
			if err != nil {
				return WrapDatabaseError("AddTransactionsBatch", fmt.Errorf("failed to serialize transaction: %w", err))
			}
			userSignature := ""
			if len(txObj.Endorsements) > 0 {
				userSignature = txObj.Endorsements[0].Signature
			}
			sb.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?)")
			if i < len(chunk)-1 {
				sb.WriteString(",")
			}
			args = append(args,
				txObj.TXID, string(data), txObj.UserID, txObj.CreatorSignature,
				userSignature, txObj.TxType.String(), "pending", now, now)
		}
		query := sb.String() + ` ON DUPLICATE KEY UPDATE
			data = VALUES(data), user_id = VALUES(user_id), updated_at = VALUES(updated_at)`
		if _, err := txdb.db.Exec(query, args...); err != nil {
			return WrapDatabaseError("AddTransactionsBatch", fmt.Errorf("failed to save transactions in batch: %w", err))
		}
		total += len(chunk)

		txdb.mu.Lock()
		for _, txInterface := range chunk {
			if txObj, ok := txInterface.(*types.Transaction); ok {
				txdb.txs[txObj.TXID] = txObj
			}
		}
		txdb.mu.Unlock()
	}
	log.Printf("[batch insert] %d transactions added to the pool", total)
	return nil
}

// GetTransaction gets a transaction
func (txdb *TransactionPoolDatabase) GetTransaction(txID string) (interface{}, error) {
	txdb.mu.RLock()
	if tx, exists := txdb.txs[txID]; exists {
		txdb.mu.RUnlock()
		return tx, nil
	}
	txdb.mu.RUnlock()

	// query the database
	query := `SELECT data FROM transactions WHERE tx_id = ?`
	row := txdb.db.QueryRow(query, txID)

	var data []byte
	if err := row.Scan(&data); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("transaction not found")
		}
		return nil, WrapDatabaseError("GetTransaction", fmt.Errorf("failed to get transaction: %w", err))
	}

	var tx types.Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		return nil, WrapDatabaseError("GetTransaction", fmt.Errorf("failed to deserialize transaction: %w", err))
	}

	// update the in-memory cache
	txdb.mu.Lock()
	txdb.txs[tx.TXID] = &tx
	txdb.mu.Unlock()

	return &tx, nil
}

// GetAllTransactions returns all transactions
func (txdb *TransactionPoolDatabase) GetAllTransactions() []interface{} {
	txdb.mu.RLock()
	defer txdb.mu.RUnlock()

	transactions := make([]interface{}, 0, len(txdb.txs))
	for _, tx := range txdb.txs {
		transactions = append(transactions, tx)
	}

	return transactions
}

// GetTransactionsByType returns transactions by type
func (txdb *TransactionPoolDatabase) GetTransactionsByType(txType interface{}) []interface{} {
	txdb.mu.RLock()
	defer txdb.mu.RUnlock()

	var transactions []interface{}
	for _, tx := range txdb.txs {
		if tx.TxType.String() == fmt.Sprint(txType) {
			transactions = append(transactions, tx)
		}
	}

	return transactions
}

// GetPendingTransactions returns pending transactions
func (txdb *TransactionPoolDatabase) GetPendingTransactions(limit int) []interface{} {
	query := `SELECT data FROM transactions WHERE status = 'pending' ORDER BY created_at ASC`

	var rows *sql.Rows
	var err error
	if limit > 0 {
		query += " LIMIT ?"
		rows, err = txdb.db.Query(query, limit)
	} else {
		rows, err = txdb.db.Query(query)
	}

	if err != nil {
		log.Printf("failed to query pending transactions: %v", err)
		return []interface{}{}
	}
	defer rows.Close()

	var transactions []interface{}
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			log.Printf("failed to parse transaction data: %v", err)
			continue
		}

		var tx types.Transaction
		if err := json.Unmarshal(data, &tx); err != nil {
			log.Printf("failed to deserialize transaction: %v", err)
			continue
		}

		// return only unendorsed transactions (Endorsed is false)
		if !tx.Endorsed {
			transactions = append(transactions, &tx)
		}
	}

	return transactions
}

// RemoveTransaction removes a transaction
func (txdb *TransactionPoolDatabase) RemoveTransaction(txID string) error {
	// delete from the database
	query := `DELETE FROM transactions WHERE tx_id = ?`
	_, err := txdb.db.Exec(query, txID)
	if err != nil {
		return WrapDatabaseError("RemoveTransaction", fmt.Errorf("failed to delete transaction: %w", err))
	}

	// update the in-memory cache
	txdb.mu.Lock()
	delete(txdb.txs, txID)
	txdb.mu.Unlock()

	return nil
}

// UpdateTransactionStatus updates a transaction status
func (txdb *TransactionPoolDatabase) UpdateTransactionStatus(txID string, status string) error {
	query := `UPDATE transactions SET status = ?, updated_at = ? WHERE tx_id = ?`
	_, err := txdb.db.Exec(query, status, time.Now(), txID)
	if err != nil {
		return WrapDatabaseError("UpdateTransactionStatus", fmt.Errorf("failed to update transaction status: %w", err))
	}

	return nil
}

// UpdateTransactionsStatusBatch updates transaction statuses in batch
func (txdb *TransactionPoolDatabase) UpdateTransactionsStatusBatch(txIDs []string, status string, updatedAt time.Time) error {
	if len(txIDs) == 0 {
		return nil
	}

	// build the batch UPDATE SQL updating status and updated_at
	placeholders := make([]string, len(txIDs))
	args := make([]interface{}, 0, len(txIDs)+2)
	args = append(args, status, updatedAt)
	for i, txID := range txIDs {
		placeholders[i] = "?"
		args = append(args, txID)
	}

	query := fmt.Sprintf("UPDATE transactions SET status = ?, updated_at = ? WHERE tx_id IN (%s)", strings.Join(placeholders, ","))
	_, err := txdb.db.Exec(query, args...)
	if err != nil {
		return WrapDatabaseError("UpdateTransactionsStatusBatch", fmt.Errorf("failed to update transaction statuses in batch: %w", err))
	}

	log.Printf("[batch update] updated status of %d transactions to %s", len(txIDs), status)
	return nil
}

// GetTransactionCount returns the transaction count
func (txdb *TransactionPoolDatabase) GetTransactionCount() int {
	txdb.mu.RLock()
	defer txdb.mu.RUnlock()

	return len(txdb.txs)
}

// GetUnendorsedTransactions returns unendorsed transactions
func (txdb *TransactionPoolDatabase) GetUnendorsedTransactions() ([]interface{}, error) {
	query := `SELECT data FROM transactions WHERE status = 'pending' ORDER BY created_at ASC`

	rows, err := txdb.db.Query(query)
	if err != nil {
		return nil, WrapDatabaseError("GetUnendorsedTransactions", fmt.Errorf("failed to query unendorsed transactions: %w", err))
	}
	defer rows.Close()

	var transactions []interface{}
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			log.Printf("failed to parse transaction data: %v", err)
			continue
		}

		var tx types.Transaction
		if err := json.Unmarshal(data, &tx); err != nil {
			log.Printf("failed to deserialize transaction: %v", err)
			continue
		}

		// return only unendorsed transactions (Endorsed is false)
		if !tx.Endorsed {
			transactions = append(transactions, &tx)
		}
	}

	return transactions, nil
}

// GetEndorsedTransactionsCount returns the number of endorsed transactions
func (txdb *TransactionPoolDatabase) GetEndorsedTransactionsCount(txType interface{}) (int, error) {
	var txTypeStr string

	switch v := txType.(type) {
	case string:
		txTypeStr = v
	case types.TransactionType:
		txTypeStr = v.String()
	default:
		if strer, ok := interface{}(v).(fmt.Stringer); ok {
			txTypeStr = strer.String()
		} else {
			txTypeStr = "management"
		}
	}

	query := "SELECT COUNT(*) FROM transactions WHERE status = 'endorsed' AND `type` = ?"

	var count int
	err := txdb.db.QueryRow(query, txTypeStr).Scan(&count)
	if err != nil {
		return 0, WrapDatabaseError("GetEndorsedTransactionsCount", fmt.Errorf("failed to query endorsed transaction count: %w", err))
	}

	return count, nil
}

// GetEndorsedTransactions returns endorsed transactions
func (txdb *TransactionPoolDatabase) GetEndorsedTransactions(txType interface{}, limit int) ([]interface{}, error) {
	var txTypeStr string

	// handle different txType parameter types
	switch v := txType.(type) {
	case string:
		txTypeStr = v
	case types.TransactionType:
		txTypeStr = v.String()
	default:
		// try the String() method
		if strer, ok := interface{}(v).(fmt.Stringer); ok {
			txTypeStr = strer.String()
		} else {
			txTypeStr = "management"
		}
	}

	query := "SELECT data FROM transactions WHERE status = 'endorsed' AND `type` = ? ORDER BY created_at ASC"
	log.Printf("[db query] GetEndorsedTransactions: txType=%s, limit=%d, query=%s", txTypeStr, limit, query)

	var rows *sql.Rows
	var err error
	if limit > 0 {
		query += " LIMIT ?"
		rows, err = txdb.db.Query(query, txTypeStr, limit)
	} else {
		rows, err = txdb.db.Query(query, txTypeStr)
	}

	// extra debugging: check all transactions in endorsed state
	allEndorsedQuery := "SELECT COUNT(*) FROM transactions WHERE status = 'endorsed'"
	var allEndorsedCount int
	if err := txdb.db.QueryRow(allEndorsedQuery).Scan(&allEndorsedCount); err == nil {
		log.Printf("[db query] endorsed transaction count in database: %d", allEndorsedCount)
	}

	// check transactions of a specific type
	typeCountQuery := "SELECT COUNT(*) FROM transactions WHERE status = 'endorsed' AND `type` = ?"
	var typeCount int
	if err := txdb.db.QueryRow(typeCountQuery, txTypeStr).Scan(&typeCount); err == nil {
		log.Printf("[db query] endorsed transaction count for type %s: %d", txTypeStr, typeCount)
	}

	if err != nil {
		return nil, WrapDatabaseError("GetEndorsedTransactions", fmt.Errorf("failed to query endorsed transactions: %w", err))
	}
	defer rows.Close()

	var transactions []interface{}
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			log.Printf("failed to parse transaction data: %v", err)
			continue
		}

		var tx types.Transaction
		if err := json.Unmarshal(data, &tx); err != nil {
			log.Printf("failed to deserialize transaction: %v", err)
			continue
		}

		transactions = append(transactions, &tx)
	}

	return transactions, nil
}

// UpdateTransactionEndorsements updates transaction endorsement state and data
func (txdb *TransactionPoolDatabase) UpdateTransactionEndorsements(txID string, endorsements interface{}) error {
	// get the current transaction data
	txInterface, err := txdb.GetTransaction(txID)
	if err != nil {
		return WrapDatabaseError("UpdateTransactionEndorsements", fmt.Errorf("failed to get transaction data: %w", err))
	}

	tx, ok := txInterface.(*types.Transaction)
	if !ok {
		return WrapDatabaseError("UpdateTransactionEndorsements", fmt.Errorf("invalid transaction type"))
	}

	// update the endorsement data
	if endorsementsSlice, ok := endorsements.([]types.Endorsement); ok {
		tx.Endorsements = endorsementsSlice
		tx.Endorsed = true
	}

	// serialize the updated transaction data
	data, err := json.Marshal(tx)
	if err != nil {
		return WrapDatabaseError("UpdateTransactionEndorsements", fmt.Errorf("failed to serialize transaction: %w", err))
	}

	// extract the user signature
	userSignature := ""
	if len(tx.Endorsements) > 0 {
		userSignature = tx.Endorsements[0].Signature
	}

	// update the transaction data and status in the database
	query := `UPDATE transactions SET data = ?, user_signature = ?, status = 'endorsed', updated_at = ? WHERE tx_id = ?`
	_, err = txdb.db.Exec(query, data, userSignature, time.Now(), txID)
	if err != nil {
		return WrapDatabaseError("UpdateTransactionEndorsements", fmt.Errorf("failed to update transaction endorsement: %w", err))
	}

	// update the in-memory cache
	txdb.mu.Lock()
	txdb.txs[txID] = tx
	txdb.mu.Unlock()

	return nil
}

// UpdateTransactionsEndorsementsBatch writes back endorsements in batch (each item is a *types.Transaction already carrying endorsements).
// a single UPDATE ... CASE updates data / user_signature / status / updated_at together, eliminating per-tx write amplification.
func (txdb *TransactionPoolDatabase) UpdateTransactionsEndorsementsBatch(txs []interface{}) error {
	if len(txs) == 0 {
		return nil
	}
	// shard: 200 rows per shard (data is multi-KB JSON; bounds single-statement size)
	const rowsPerStmt = 200
	now := time.Now()
	for start := 0; start < len(txs); start += rowsPerStmt {
		end := start + rowsPerStmt
		if end > len(txs) {
			end = len(txs)
		}
		chunk := txs[start:end]

		var sb strings.Builder
		sb.WriteString(`UPDATE transactions SET
			data = CASE tx_id `)
		args := make([]interface{}, 0, len(chunk)*4+len(chunk)+1)
		for _, txInterface := range chunk {
			txObj, ok := txInterface.(*types.Transaction)
			if !ok || txObj.TXID == "" {
				return NewDatabaseError("UpdateTransactionsEndorsementsBatch", fmt.Errorf("invalid transaction type or empty transaction ID"))
			}
			data, err := json.Marshal(txObj)
			if err != nil {
				return WrapDatabaseError("UpdateTransactionsEndorsementsBatch", fmt.Errorf("failed to serialize transaction: %w", err))
			}
			sb.WriteString("WHEN ? THEN ? ")
			args = append(args, txObj.TXID, string(data))
		}
		sb.WriteString("END, user_signature = CASE tx_id ")
		for _, txInterface := range chunk {
			txObj := txInterface.(*types.Transaction)
			userSignature := ""
			if len(txObj.Endorsements) > 0 {
				userSignature = txObj.Endorsements[0].Signature
			}
			sb.WriteString("WHEN ? THEN ? ")
			args = append(args, txObj.TXID, userSignature)
		}
		sb.WriteString("END, status = 'endorsed', updated_at = ? WHERE tx_id IN (")
		args = append(args, now) // placeholders follow SQL text order: data-case, sig-case, updated_at, IN list
		for i, txInterface := range chunk {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("?")
			args = append(args, txInterface.(*types.Transaction).TXID)
		}
		// status guard: the endorsement service reads "unendorsed snapshot"; at write-back time the batch may already have been
		// set to blocked by the management node's block sealing - the status must not be changed back to endorsed (otherwise the packaging pool
		// reload would fetch already on-chain transactions back, causing duplicate sealing)
		sb.WriteString(") AND status != 'blocked'")

		if _, err := txdb.db.Exec(sb.String(), args...); err != nil {
			return WrapDatabaseError("UpdateTransactionsEndorsementsBatch", fmt.Errorf("failed to update transaction endorsements in batch: %w", err))
		}

		txdb.mu.Lock()
		for _, txInterface := range chunk {
			if txObj, ok := txInterface.(*types.Transaction); ok {
				txdb.txs[txObj.TXID] = txObj
			}
		}
		txdb.mu.Unlock()
	}
	return nil
}

// Clear clears the transaction pool
func (txdb *TransactionPoolDatabase) Clear() error {
	// delete all transactions from the database
	query := `DELETE FROM transactions`
	_, err := txdb.db.Exec(query)
	if err != nil {
		return WrapDatabaseError("Clear", fmt.Errorf("failed to clear transaction pool: %w", err))
	}

	// clear the in-memory cache
	txdb.mu.Lock()
	txdb.txs = make(map[string]*types.Transaction)
	txdb.mu.Unlock()

	return nil
}

// loadAllTransactions loads all transactions
func (txdb *TransactionPoolDatabase) loadAllTransactions() error {
	query := `SELECT data FROM transactions`
	rows, err := txdb.db.Query(query)
	if err != nil {
		return WrapDatabaseError("loadAllTransactions", fmt.Errorf("failed to query transactions: %w", err))
	}
	defer rows.Close()

	txdb.mu.Lock()
	defer txdb.mu.Unlock()

	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			log.Printf("failed to parse transaction data: %v", err)
			continue
		}

		var tx types.Transaction
		if err := json.Unmarshal(data, &tx); err != nil {
			log.Printf("failed to deserialize transaction: %v", err)
			continue
		}

		txdb.txs[tx.TXID] = &tx
	}

	log.Printf("loaded %d transactions", len(txdb.txs))
	return nil
}
