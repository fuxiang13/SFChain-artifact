package database

import "sfchain/pkg/types"

// ManagementNodeInterface is the management node interface, used to break the circular dependency
type ManagementNodeInterface interface {
	CreateTransaction(txType types.TransactionType, logData map[string]interface{}) (*types.Transaction, error)
	GetNodeID() string
}
