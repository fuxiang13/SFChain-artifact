package mysql

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Config is the MySQL configuration
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	Charset  string
}

// DB manages the MySQL database connection
type DB struct {
	*sql.DB
	config Config
	mu     sync.RWMutex
}

// NewDB creates a new MySQL database connection
func NewDB(config Config) (*DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=true&loc=Local",
		config.User,
		config.Password,
		config.Host,
		config.Port,
		config.DBName,
		config.Charset,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Set connection pool parameters
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("database connection test failed: %w", err)
	}

	log.Printf("MySQL database connected: %s:%d/%s", config.Host, config.Port, config.DBName)

	return &DB{
		DB:     db,
		config: config,
	}, nil
}

// Close closes the database connection
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.DB != nil {
		return db.DB.Close()
	}

	return nil
}

// GetConfig returns the database configuration
func (db *DB) GetConfig() Config {
	return db.config
}

// Transaction executes a transaction
func (db *DB) Transaction(fn func(*sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Printf("Failed to roll back transaction: %v", rollbackErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
