package types

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// TransactionType is the transaction type enum
type TransactionType int

const (
	TransactionTypeManagement TransactionType = iota + 1
	TransactionTypeDevelopment
	TransactionTypeTest
	TransactionTypeOperations
)

func (tt TransactionType) String() string {
	switch tt {
	case TransactionTypeManagement:
		return "management"
	case TransactionTypeDevelopment:
		return "development"
	case TransactionTypeTest:
		return "test"
	case TransactionTypeOperations:
		return "operations"
	default:
		return "unknown"
	}
}

// TransactionTypeFromString returns the TransactionType matching the given string
func TransactionTypeFromString(s string) TransactionType {
	switch s {
	case "management":
		return TransactionTypeManagement
	case "development":
		return TransactionTypeDevelopment
	case "test":
		return TransactionTypeTest
	case "operations":
		return TransactionTypeOperations
	default:
		return TransactionTypeManagement
	}
}

// Endorsement is the endorsement information structure
type Endorsement struct {
	UserID    string  `json:"user_id"`
	PublicKey string  `json:"public_key"`
	Signature string  `json:"signature"`
	Timestamp float64 `json:"timestamp"`
	Role      string  `json:"role"`
}

// Transaction is the transaction data structure
type Transaction struct {
	TXID             string                 `json:"tx_id"`
	TxType           TransactionType        `json:"tx_type"`
	UserID           string                 `json:"user_id"`
	LogData          map[string]interface{} `json:"log_data"`
	Timestamp        float64                `json:"timestamp"`
	CreatorPublicKey string                 `json:"creator_public_key"`
	CreatorSignature string                 `json:"creator_signature"`
	Endorsements     []Endorsement          `json:"endorsements"`
	Endorsed         bool                   `json:"endorsed"`
	UpdatedAt        float64                `json:"updated_at"` // in-memory update timestamp
	RQ1RunID         string                 `json:"rq1_run_id,omitempty"`
}

// CalculateTXID computes the transaction ID
func (t *Transaction) CalculateTXID() string {
	// Build a struct containing UserID and LogData for hashing
	txData := struct {
		UserID  string                 `json:"user_id"`
		LogData map[string]interface{} `json:"log_data"`
	}{
		UserID:  t.UserID,
		LogData: t.LogData,
	}
	data, _ := json.Marshal(txData)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
