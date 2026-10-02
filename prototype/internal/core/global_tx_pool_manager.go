package core

import (
	"log"
	"sync"

	"sfchain/internal/database"
	"sfchain/pkg/config"
)

// GlobalTxPoolManager is the global transaction pool database manager, used to
// share the database connection
type GlobalTxPoolManager struct {
	txPoolDB    database.TransactionPoolDatabaseInterface
	mu          sync.RWMutex
	config      *config.DatabaseConfig
	initialized bool
}

var (
	globalTxPoolManager *GlobalTxPoolManager
	txPoolInitOnce      sync.Once
)

// GetGlobalTxPoolManager returns the global transaction pool database manager instance
func GetGlobalTxPoolManager() *GlobalTxPoolManager {
	return globalTxPoolManager
}

// InitGlobalTxPoolManager initializes the global transaction pool database manager
func InitGlobalTxPoolManager(cfg *config.DatabaseConfig) error {
	var initErr error
	txPoolInitOnce.Do(func() {
		// Create the transaction pool database via the database factory
		dbFactory := database.NewDBFactory(cfg)
		txPoolDB, err := dbFactory.CreateTransactionPoolDatabase()
		if err != nil {
			initErr = err
			log.Printf("Failed to initialize global transaction pool database: %v", err)
			return
		}

		globalTxPoolManager = &GlobalTxPoolManager{
			txPoolDB:    txPoolDB,
			config:      cfg,
			initialized: true,
		}
		log.Printf("Global transaction pool database manager initialized successfully")
	})

	if globalTxPoolManager != nil && globalTxPoolManager.IsInitialized() {
		log.Printf("Global transaction pool database manager already initialized, reusing existing instance")
		return nil
	}

	return initErr
}

// GetDB returns the transaction pool database instance
func (gtpm *GlobalTxPoolManager) GetDB() database.TransactionPoolDatabaseInterface {
	gtpm.mu.RLock()
	defer gtpm.mu.RUnlock()
	return gtpm.txPoolDB
}

// IsInitialized reports whether the manager is initialized
func (gtpm *GlobalTxPoolManager) IsInitialized() bool {
	gtpm.mu.RLock()
	defer gtpm.mu.RUnlock()
	return gtpm.initialized
}

// Close closes the database connection
func (gtpm *GlobalTxPoolManager) Close() error {
	gtpm.mu.Lock()
	defer gtpm.mu.Unlock()

	if gtpm.txPoolDB != nil {
		_ = gtpm.txPoolDB.Close()
		gtpm.txPoolDB = nil
		gtpm.initialized = false
	}

	return nil
}
