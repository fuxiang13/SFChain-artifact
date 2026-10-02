-- MySQL database schema design
-- This file should be kept consistent with the table definitions in the code
-- The in-code table definitions live in the following files:
-- - internal/database/mysql/transaction_pool.go (transactions table)
-- - internal/database/mysql/user_database.go (users table)
-- - internal/database/mysql/log_database.go (software_factory_logs table)
-- - internal/database/mysql/block_manager.go (block, block-header, and blockchain-info tables)

-- Create the database
CREATE DATABASE IF NOT EXISTS sfchain DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE sfchain;

-- ============================================
-- Common tables
-- ============================================

-- Transactions table
-- Corresponding code: internal/database/mysql/transaction_pool.go
CREATE TABLE IF NOT EXISTS transactions (
    tx_id VARCHAR(64) PRIMARY KEY,
    data JSON NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    management_signature TEXT,
    user_signature TEXT,
    type VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_type (type),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Processed-transactions table
CREATE TABLE IF NOT EXISTS processed_transactions (
    tx_id VARCHAR(64) PRIMARY KEY,
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_processed_at (processed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Users table
-- Corresponding code: internal/database/mysql/user_database.go
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(64) PRIMARY KEY,
    user_name VARCHAR(64) NOT NULL,
    role VARCHAR(32) NOT NULL,
    public_key VARCHAR(256) NOT NULL,
    private_key VARCHAR(256),
    email VARCHAR(128) NOT NULL,
    department VARCHAR(64),
    created_at DATETIME NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    endorsement_count INT DEFAULT 0,
    INDEX idx_role (role),
    INDEX idx_email (email),
    INDEX idx_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Software-factory logs table
-- Corresponding code: internal/database/mysql/log_database.go
CREATE TABLE IF NOT EXISTS software_factory_logs (
    id VARCHAR(64) PRIMARY KEY,
    category VARCHAR(32) NOT NULL,
    timestamp DOUBLE NOT NULL,
    level VARCHAR(16) NOT NULL,
    message TEXT NOT NULL,
    user_id VARCHAR(64),
    module VARCHAR(64),
    project VARCHAR(64),
    operation VARCHAR(64),
    status VARCHAR(32),
    processed BOOLEAN DEFAULT FALSE,
    tx_id VARCHAR(64),
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_category (category),
    INDEX idx_processed (processed),
    INDEX idx_timestamp (timestamp),
    INDEX idx_tx_id (tx_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Management-node tables (man_*)
-- Corresponding code: the initTables function in internal/database/mysql/block_manager.go
-- The management node creates the block tables and block-header tables of all 4 chains
-- ============================================

-- Management node - management-chain block table
CREATE TABLE IF NOT EXISTS man_blocks_management (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    transactions JSON,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_block_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Management node - management-chain block-header table
CREATE TABLE IF NOT EXISTS man_headers_management (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Management node - development-chain block table
CREATE TABLE IF NOT EXISTS man_blocks_development (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    transactions JSON,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_block_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Management node - development-chain block-header table
CREATE TABLE IF NOT EXISTS man_headers_development (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Management node - test-chain block table
CREATE TABLE IF NOT EXISTS man_blocks_test (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    transactions JSON,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_block_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Management node - test-chain block-header table
CREATE TABLE IF NOT EXISTS man_headers_test (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Management node - operations-chain block table
CREATE TABLE IF NOT EXISTS man_blocks_operations (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    transactions JSON,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_block_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Management node - operations-chain block-header table
CREATE TABLE IF NOT EXISTS man_headers_operations (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Management-node blockchain-info table
CREATE TABLE IF NOT EXISTS man_blockchain_info (
    chain_type VARCHAR(32) PRIMARY KEY,
    latest_block_hash VARCHAR(64) DEFAULT '',
    latest_block_height INT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Development-node tables (dev_*)
-- The development node creates its own chain's block table and every chain's block-header tables
-- ============================================

-- Development node - development-chain block table
CREATE TABLE IF NOT EXISTS dev_blocks (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    transactions JSON,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_block_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Development node - development-chain block-header table
CREATE TABLE IF NOT EXISTS dev_headers_development (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Development node - other chains' block-header tables (management, test, operations)
CREATE TABLE IF NOT EXISTS dev_headers_management (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS dev_headers_test (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS dev_headers_operations (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Development-node blockchain-info table
CREATE TABLE IF NOT EXISTS dev_blockchain_info (
    chain_type VARCHAR(32) PRIMARY KEY,
    latest_block_hash VARCHAR(64) DEFAULT '',
    latest_block_height INT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Test-node tables (test_*)
-- The test node creates its own chain's block table and every chain's block-header tables
-- ============================================

-- Test node - test-chain block table
CREATE TABLE IF NOT EXISTS test_blocks (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    transactions JSON,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_block_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Test node - test-chain block-header table
CREATE TABLE IF NOT EXISTS test_headers_test (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Test node - other chains' block-header tables (management, development, operations)
CREATE TABLE IF NOT EXISTS test_headers_management (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS test_headers_development (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS test_headers_operations (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Test-node blockchain-info table
CREATE TABLE IF NOT EXISTS test_blockchain_info (
    chain_type VARCHAR(32) PRIMARY KEY,
    latest_block_hash VARCHAR(64) DEFAULT '',
    latest_block_height INT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Operations-node tables (ops_*)
-- The operations node creates its own chain's block table and every chain's block-header tables
-- ============================================

-- Operations node - operations-chain block table
CREATE TABLE IF NOT EXISTS ops_blocks (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    transactions JSON,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_block_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Operations node - operations-chain block-header table
CREATE TABLE IF NOT EXISTS ops_headers_operations (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Operations node - other chains' block-header tables (management, development, test)
CREATE TABLE IF NOT EXISTS ops_headers_management (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ops_headers_development (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ops_headers_test (
    block_hash VARCHAR(64) PRIMARY KEY,
    previous_hash VARCHAR(128) NOT NULL,
    block_height INT NOT NULL,
    timestamp DOUBLE NOT NULL,
    merkle_root VARCHAR(64),
    chain_type INT NOT NULL,
    aggregated_signature VARCHAR(256),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_header_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Operations-node blockchain-info table
CREATE TABLE IF NOT EXISTS ops_blockchain_info (
    chain_type VARCHAR(32) PRIMARY KEY,
    latest_block_hash VARCHAR(64) DEFAULT '',
    latest_block_height INT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Initialize blockchain info
-- ============================================

-- Management-node blockchain-info initialization
INSERT INTO man_blockchain_info (chain_type, latest_block_hash, latest_block_height)
VALUES 
('management', '', 0),
('development', '', 0),
('test', '', 0),
('operations', '', 0)
ON DUPLICATE KEY UPDATE last_updated = CURRENT_TIMESTAMP;

-- Development-node blockchain-info initialization
INSERT INTO dev_blockchain_info (chain_type, latest_block_hash, latest_block_height)
VALUES 
('management', '', 0),
('development', '', 0),
('test', '', 0),
('operations', '', 0)
ON DUPLICATE KEY UPDATE last_updated = CURRENT_TIMESTAMP;

-- Test-node blockchain-info initialization
INSERT INTO test_blockchain_info (chain_type, latest_block_hash, latest_block_height)
VALUES 
('management', '', 0),
('development', '', 0),
('test', '', 0),
('operations', '', 0)
ON DUPLICATE KEY UPDATE last_updated = CURRENT_TIMESTAMP;

-- Operations-node blockchain-info initialization
INSERT INTO ops_blockchain_info (chain_type, latest_block_hash, latest_block_height)
VALUES 
('management', '', 0),
('development', '', 0),
('test', '', 0),
('operations', '', 0)
ON DUPLICATE KEY UPDATE last_updated = CURRENT_TIMESTAMP;

-- Generate disposable signing identities with bin/seedusers after schema creation.
