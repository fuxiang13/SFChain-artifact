package core

import (
	"log"
	"sfchain/internal/database"
	"sfchain/pkg/config"
	"sync"
)

// UserDatabaseManager is the global user database manager
type UserDatabaseManager struct {
	mu     sync.RWMutex
	db     database.UserDatabaseInterface
	config *config.DatabaseConfig
	closed bool
}

var (
	userDBManager   *UserDatabaseManager
	userDBManagerMu sync.Mutex
)

// InitGlobalUserDatabaseManager initializes the global user database manager
func InitGlobalUserDatabaseManager(cfg *config.DatabaseConfig) error {
	userDBManagerMu.Lock()
	defer userDBManagerMu.Unlock()

	// If already initialized, return directly
	if userDBManager != nil && userDBManager.db != nil {
		log.Printf("Global user database manager already initialized, reusing existing instance")
		return nil
	}

	// Try to initialize
	dbFactory := database.NewDBFactory(cfg)
	db, dbErr := dbFactory.CreateUserDatabase()
	if dbErr != nil {
		log.Printf("Failed to initialize global user database: %v", dbErr)
		// Keep any existing manager unchanged
		if userDBManager == nil {
			// Create an empty manager
			userDBManager = &UserDatabaseManager{
				db:     nil,
				config: cfg,
				closed: false,
			}
			log.Printf("Empty user database manager created; it will be used automatically once the database is available")
		}
		return dbErr
	}

	// Initialization succeeded: create or update the manager
	userDBManager = &UserDatabaseManager{
		db:     db,
		config: cfg,
		closed: false,
	}
	log.Printf("Global user database manager initialized successfully")

	return nil
}

// GetGlobalUserDatabaseManager returns the global user database manager
func GetGlobalUserDatabaseManager() *UserDatabaseManager {
	return userDBManager
}

// GetDB returns the user database instance
func (m *UserDatabaseManager) GetDB() database.UserDatabaseInterface {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.db
}

// Close closes the user database manager
func (m *UserDatabaseManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	if m.db != nil {
		if err := m.db.Close(); err != nil {
			return err
		}
		m.db = nil
	}

	m.closed = true
	log.Println("Global user database manager closed")
	return nil
}

// IsInitialized reports whether the user database manager is initialized
func (m *UserDatabaseManager) IsInitialized() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.db != nil
}
