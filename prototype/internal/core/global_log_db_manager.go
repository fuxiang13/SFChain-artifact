package core

import (
	"sync"

	"sfchain/internal/database"
	"sfchain/internal/database/mysql"
)

// GlobalLogDBManager is the global log database manager, used to share the
// database connection
type GlobalLogDBManager struct {
	logDB       database.LogDatabaseInterface
	mu          sync.RWMutex
	initialized bool
}

var (
	globalLogDBManager *GlobalLogDBManager
)

// GetGlobalLogDBManager returns the global log database manager instance
func GetGlobalLogDBManager() *GlobalLogDBManager {
	return globalLogDBManager
}

// InitGlobalLogDBManager initializes the global log database manager
func InitGlobalLogDBManager() error {
	// If already initialized, return directly
	if globalLogDBManager != nil && globalLogDBManager.IsInitialized() {
		// log.Printf("global log DB manager already initialized, reusing existing instance")
		return nil
	}

	// Create the default MySQL configuration
	mysqlConfig := mysql.Config{
		Host:     "localhost",
		Port:     3306,
		User:     "fx",
		Password: "123456",
		DBName:   "sfchain",
		Charset:  "utf8mb4",
	}

	// Try to initialize the MySQL log database
	logDB, err := mysql.NewLogDatabase(mysqlConfig)
	if err != nil {
		// log.Printf("failed to initialize global log database: %v", err)
		// Keep any existing manager unchanged
		if globalLogDBManager == nil {
			// Create an empty manager
			globalLogDBManager = &GlobalLogDBManager{
				logDB:       nil,
				initialized: false,
			}
			// log.Printf("empty log DB manager created; used automatically once the database is available")
		}
		return nil
	}

	// Initialization succeeded: create or update the manager
	globalLogDBManager = &GlobalLogDBManager{
		logDB:       logDB,
		initialized: true,
	}
	// log.Printf("global log DB manager initialized successfully, using MySQL database")

	// Return nil so the system can still start up
	return nil
}

// GetDB returns the log database instance
func (gldm *GlobalLogDBManager) GetDB() database.LogDatabaseInterface {
	gldm.mu.RLock()
	defer gldm.mu.RUnlock()
	return gldm.logDB
}

// IsInitialized reports whether the manager is initialized
func (gldm *GlobalLogDBManager) IsInitialized() bool {
	gldm.mu.RLock()
	defer gldm.mu.RUnlock()
	return gldm.initialized
}

// Close closes the database connection
func (gldm *GlobalLogDBManager) Close() error {
	gldm.mu.Lock()
	defer gldm.mu.Unlock()

	if gldm.logDB != nil {
		_ = gldm.logDB.Close()
		gldm.logDB = nil
		gldm.initialized = false
	}

	return nil
}
