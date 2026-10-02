package database

import (
	"fmt"
	"log"
	"sfchain/pkg/types"
	"time"
)

// LogDatabaseInterface is the log database interface
type LogDatabaseInterface interface {
	Close() error
	AddLog(log interface{}) error
	AddLogs(logs []interface{}) error
	GetLog(logID string) (interface{}, error)
	QueryLogs(query interface{}) ([]interface{}, error)
	GetPendingLogs(limit int) ([]interface{}, error)
	// GetPendingLogsOrdered returns one deterministic, ascending snapshot of
	// the prepared workload for the current RQ1 protocol.
	GetPendingLogsOrdered(limit int) ([]interface{}, error)
	MarkAsProcessed(logID, txID string) error
	// MarkAsProcessedBatch marks logs in batch; each entry of refs is [2]string{logID, txID}
	MarkAsProcessedBatch(refs [][2]string) error
	GetStats() (interface{}, error)
	BuildTransactionLogs(chainType interface{}, limit int) ([]map[string]interface{}, error)
	GetLogsByTXID(txID string) ([]interface{}, error)
	DeleteOldLogs(beforeTimestamp float64) (int, error)
	IsProcessed(txID string) bool
	GetPendingCount() int
	GetDBPath() string
}

// UserDatabaseInterface is the user database interface
type UserDatabaseInterface interface {
	Close() error
	AddUser(user interface{}) error
	GetUser(id string) (interface{}, error)
	GetUserByPublicKey(publicKey string) (interface{}, error)
	GetAllUsers() ([]interface{}, error)
	UpdateUser(user interface{}) error
	DeleteUser(id string) error
	GetUsersByRole(role string) ([]interface{}, error)
	UserExists(id string) bool
}

// TransactionPoolDatabaseInterface is the transaction pool database interface
type TransactionPoolDatabaseInterface interface {
	Close() error
	AddTransaction(tx interface{}) error
	AddTransactionsBatch(txs []interface{}) error
	GetTransaction(txID string) (interface{}, error)
	GetAllTransactions() []interface{}
	GetTransactionsByType(txType interface{}) []interface{}
	GetPendingTransactions(limit int) []interface{}
	RemoveTransaction(txID string) error
	UpdateTransactionStatus(txID, status string) error
	UpdateTransactionsStatusBatch(txIDs []string, status string, updatedAt time.Time) error
	GetTransactionCount() int
	Clear() error
	GetUnendorsedTransactions() ([]interface{}, error)
	GetEndorsedTransactions(txType interface{}, limit int) ([]interface{}, error)
	GetEndorsedTransactionsCount(txType interface{}) (int, error)
	UpdateTransactionEndorsements(txID string, endorsements interface{}) error
	// UpdateTransactionsEndorsementsBatch writes back endorsements in batch (each
	// entry is a *types.Transaction already carrying endorsement data; a single
	// SQL statement updates data/user_signature/status together)
	UpdateTransactionsEndorsementsBatch(txs []interface{}) error
}

// BlockManagerInterface is the block manager interface
type BlockManagerInterface interface {
	Close() error
	AddBlock(block interface{}) error
	AddBlockHeader(block interface{}) error
	GetBlock(blockHash string) (interface{}, error)
	GetBlockByHeight(height int64, blockType interface{}) (interface{}, error)
	GetLatestBlock(blockType interface{}) (interface{}, error)
	GetBlocksByRange(startHeight, endHeight int64, blockType interface{}) ([]interface{}, error)
	GetBlockchainHeight(blockType interface{}) (int64, error)
	GetBlockCount(blockType interface{}) (int64, error)
	StoreBlockByNodeType(nodeType string, block *types.Block, managerPublicKey string) error
	VerifyBlock(block *types.Block, expectedPublicKey string, managerPublicKey string) error
	UpdateBlockWithAggregatedSignature(blockHash string, aggregatedSignature string) error
	UpdateBlockWithAggregatedSignatureByHeight(blockHeight int64, chainType types.TransactionType, aggregatedSignature string) error
}

// User holds user information
type User struct {
	UserID           string    `json:"user_id"`
	UserName         string    `json:"user_name"`
	Role             string    `json:"role"`
	PublicKey        string    `json:"public_key"`
	PrivateKey       string    `json:"private_key"`
	Email            string    `json:"email"`
	Department       string    `json:"department"`
	CreatedAt        time.Time `json:"created_at"`
	IsActive         bool      `json:"is_active"`
	EndorsementCount int       `json:"endorsement_count"`
}

// DatabaseError represents a database error
type DatabaseError struct {
	Operation string
	Err       error
}

func (de *DatabaseError) Error() string {
	return fmt.Sprintf("Database operation '%s' failed: %v", de.Operation, de.Err)
}

func (de *DatabaseError) Unwrap() error {
	return de.Err
}

// NewDatabaseError creates a database error
func NewDatabaseError(operation string, err error) *DatabaseError {
	return &DatabaseError{
		Operation: operation,
		Err:       err,
	}
}

// WrapDatabaseError wraps a database error
func WrapDatabaseError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &DatabaseError{
		Operation: operation,
		Err:       err,
	}
}

// LogCategory is the log category
type LogCategory string

const (
	CategoryManagement  LogCategory = "management"
	CategoryDevelopment LogCategory = "development"
	CategoryTest        LogCategory = "test"
	CategoryOperations  LogCategory = "operations"
)

// LogLevel is the log level
type LogLevel string

const (
	LevelError   LogLevel = "error"
	LevelWarning LogLevel = "warning"
	LevelInfo    LogLevel = "info"
)

// LogQuery is a log query
type LogQuery struct {
	Category  LogCategory
	Level     string
	UserID    string
	Module    string
	Project   string
	StartTime float64
	EndTime   float64
	Status    string
	Processed *bool
	Limit     int
	Offset    int
}

// LogStats holds log statistics
type LogStats struct {
	TotalCount     int
	ProcessedCount int
	PendingCount   int
	CategoryStats  map[string]int
	LevelStats     map[string]int
	LatestLogs     []interface{}
}

// SoftwareFactoryLog is a software factory log
type SoftwareFactoryLog struct {
	ID        string      `json:"id"`
	Category  LogCategory `json:"category"`
	Timestamp float64     `json:"timestamp"`
	Level     string      `json:"level"`
	Message   string      `json:"message"`
	UserID    string      `json:"user_id"`
	Module    string      `json:"module"`
	Project   string      `json:"project"`
	Operation string      `json:"operation"`
	Status    string      `json:"status"`
	Processed bool        `json:"processed"`
	TXID      string      `json:"tx_id"`
	CreatedAt time.Time   `json:"created_at"`
}

// Validate validates the log data
func (l *SoftwareFactoryLog) Validate() bool {
	return l.ID != "" && l.Category != "" && l.Timestamp > 0
}

// ToTransactionLog converts the log to the transaction log format
func (l *SoftwareFactoryLog) ToTransactionLog() map[string]interface{} {
	return map[string]interface{}{
		"id":         l.ID,
		"category":   l.Category,
		"timestamp":  l.Timestamp,
		"level":      l.Level,
		"message":    l.Message,
		"user_id":    l.UserID,
		"module":     l.Module,
		"project":    l.Project,
		"operation":  l.Operation,
		"status":     l.Status,
		"processed":  l.Processed,
		"tx_id":      l.TXID,
		"created_at": l.CreatedAt,
	}
}

// BoolPtr returns a pointer to a bool (exported)
func BoolPtr(b bool) *bool {
	return &b
}

// boolPtr returns a pointer to a bool (internal use)
func boolPtr(b bool) *bool {
	return &b
}

// Log is a logging function
func Log(format string, v ...interface{}) {
	log.Printf(format, v...)
}
