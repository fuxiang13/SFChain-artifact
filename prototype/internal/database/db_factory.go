package database

import (
	"fmt"
	"log"

	"sfchain/internal/database/mysql"
	"sfchain/pkg/config"
)

// DBFactory is the database factory, used to create different database instances
type DBFactory struct {
	config *config.DatabaseConfig
}

// NewDBFactory creates a new database factory
func NewDBFactory(cfg *config.DatabaseConfig) *DBFactory {
	return &DBFactory{
		config: cfg,
	}
}

// CreateLogDatabase creates the log database
func (df *DBFactory) CreateLogDatabase() (LogDatabaseInterface, error) {
	switch df.config.Type {
	case "mysql":
		// Build the MySQL configuration
		mysqlConfig := mysql.Config{
			Host:     df.config.Host,
			Port:     df.config.Port,
			User:     df.config.User,
			Password: df.config.Password,
			DBName:   df.config.DBName,
			Charset:  df.config.Charset,
		}

		// Create the MySQL log database
		logDB, err := mysql.NewLogDatabase(mysqlConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create MySQL log database: %w", err)
		}

		log.Printf("Using MySQL log database")
		return logDB, nil

	default:
		return nil, fmt.Errorf("unsupported database type: %s", df.config.Type)
	}
}

// CreateUserDatabase creates the user database
func (df *DBFactory) CreateUserDatabase() (UserDatabaseInterface, error) {
	switch df.config.Type {
	case "mysql":
		// Build the MySQL configuration
		mysqlConfig := mysql.Config{
			Host:     df.config.Host,
			Port:     df.config.Port,
			User:     df.config.User,
			Password: df.config.Password,
			DBName:   df.config.DBName,
			Charset:  df.config.Charset,
		}

		// Create the MySQL user database
		userDB, err := mysql.NewUserDatabase(mysqlConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create MySQL user database: %w", err)
		}

		log.Printf("Using MySQL user database")
		return userDB, nil

	default:
		return nil, fmt.Errorf("unsupported database type: %s", df.config.Type)
	}
}

// CreateTransactionPoolDatabase creates the transaction pool database
func (df *DBFactory) CreateTransactionPoolDatabase() (TransactionPoolDatabaseInterface, error) {
	switch df.config.Type {
	case "mysql":
		// Build the MySQL configuration
		mysqlConfig := mysql.Config{
			Host:     df.config.Host,
			Port:     df.config.Port,
			User:     df.config.User,
			Password: df.config.Password,
			DBName:   df.config.DBName,
			Charset:  df.config.Charset,
		}

		// Create the MySQL transaction pool database
		txDB, err := mysql.NewTransactionPoolDatabase(mysqlConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create MySQL transaction pool database: %w", err)
		}

		log.Printf("Using MySQL transaction pool database")
		return txDB, nil

	default:
		return nil, fmt.Errorf("unsupported database type: %s", df.config.Type)
	}
}

// CreateBlockManager creates the block manager
func (df *DBFactory) CreateBlockManager(nodeType string, publicKey string) (BlockManagerInterface, error) {
	switch df.config.Type {
	case "mysql":
		// Build the MySQL configuration
		mysqlConfig := mysql.Config{
			Host:     df.config.Host,
			Port:     df.config.Port,
			User:     df.config.User,
			Password: df.config.Password,
			DBName:   df.config.DBName,
			Charset:  df.config.Charset,
		}

		// Create the MySQL block manager
		blockManager, err := mysql.NewBlockManager(mysqlConfig, nodeType, publicKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create MySQL block manager: %w", err)
		}

		log.Printf("Using MySQL block manager")
		return blockManager, nil

	default:
		return nil, fmt.Errorf("unsupported database type: %s", df.config.Type)
	}
}
