package mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"sfchain/pkg/crypto"
	"sfchain/pkg/types"
)

// BlockManager is the MySQL implementation of the block manager
type BlockManager struct {
	db       *DB
	nodeType string
	mu       sync.RWMutex
	blocks   map[string]*types.Block
}

// NewBlockManager creates a new MySQL block manager
func NewBlockManager(config Config, nodeType string, publicKey string) (*BlockManager, error) {
	db, err := NewDB(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create database connection: %w", err)
	}

	bm := &BlockManager{
		db:       db,
		nodeType: nodeType,
		blocks:   make(map[string]*types.Block),
	}

	// initialize the table schema
	if err := bm.initTables(nodeType, publicKey); err != nil {
		return nil, fmt.Errorf("failed to initialize table schema: %w", err)
	}

	// load recent blocks
	if err := bm.loadRecentBlocks(); err != nil {
		log.Printf("failed to load block data: %v", err)
	}

	return bm, nil
}

// getTablePrefix returns the table prefix for a node type
func (bm *BlockManager) getTablePrefix() string {
	// define short names for node types
	switch bm.nodeType {
	case "management":
		return "man"
	case "development":
		return "dev"
	case "test":
		return "test"
	case "operations":
		return "ops"
	default:
		return "node"
	}
}

// getBlockTableName returns the block table name
func (bm *BlockManager) getBlockTableName(chainType string) string {
	prefix := bm.getTablePrefix()
	if bm.nodeType == "management" {
		return fmt.Sprintf("%s_blocks_%s", prefix, chainType)
	} else if bm.nodeType == chainType {
		return fmt.Sprintf("%s_blocks", prefix)
	} else {
		return ""
	}
}

// getHeaderTableName returns the block header table name
func (bm *BlockManager) getHeaderTableName(chainType string) string {
	prefix := bm.getTablePrefix()
	return fmt.Sprintf("%s_headers_%s", prefix, chainType)
}

// getInfoTableName returns the blockchain info table name
func (bm *BlockManager) getInfoTableName() string {
	return fmt.Sprintf("%s_blockchain_info", bm.getTablePrefix())
}

// initTables initializes the table schema
func (bm *BlockManager) initTables(nodeType string, publicKey string) error {
	// define the four chain types
	chainTypes := []string{"management", "development", "test", "operations"}

	// create different tables per node type
	switch nodeType {
	case "management":
		// management node: create 8 tables (blocks and headers of the four chains)
		for _, chainType := range chainTypes {
			// create the block table
			blockTable := bm.getBlockTableName(chainType)
			createBlockTable := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				block_hash VARCHAR(64) PRIMARY KEY,
				previous_hash VARCHAR(128) NOT NULL,
				block_height INT NOT NULL,
				timestamp DOUBLE NOT NULL,
				transactions JSON,
				merkle_root VARCHAR(64),
				chain_type INT NOT NULL,
				aggregated_signature VARCHAR(256),
				created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
			`, blockTable)
			_, err := bm.db.Exec(createBlockTable)
			if err != nil {
				return fmt.Errorf("failed to create %s table: %w", blockTable, err)
			}

			// create the block header table
			headerTable := bm.getHeaderTableName(chainType)
			createHeaderTable := fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS %s (
					block_hash VARCHAR(64) PRIMARY KEY,
					previous_hash VARCHAR(128) NOT NULL,
					block_height INT NOT NULL,
					timestamp DOUBLE NOT NULL,
					merkle_root VARCHAR(64),
					chain_type INT NOT NULL,
					aggregated_signature VARCHAR(256),
					created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
				`, headerTable)
			_, err = bm.db.Exec(createHeaderTable)
			if err != nil {
				return fmt.Errorf("failed to create %s table: %w", headerTable, err)
			}

			// add a block_height unique index to the block and header tables
			blockHeightIndex := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_block_height ON %s(block_height)`, chainType, blockTable)
			_, _ = bm.db.Exec(blockHeightIndex)
			headerHeightIndex := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_header_height ON %s(block_height)`, chainType, headerTable)
			_, _ = bm.db.Exec(headerHeightIndex)
		}
	case "development", "test", "operations":
		// DTO node: create 5 tables (own chain's block and header, headers of the other three chains)
		for _, chainType := range chainTypes {
			if chainType == nodeType {
				// own chain: create block and header tables
				// create the block table
				blockTable := bm.getBlockTableName(chainType)
				createBlockTable := fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS %s (
					block_hash VARCHAR(64) PRIMARY KEY,
					previous_hash VARCHAR(128) NOT NULL,
					block_height INT NOT NULL,
					timestamp DOUBLE NOT NULL,
					transactions JSON,
					merkle_root VARCHAR(64),
					chain_type INT NOT NULL,
					aggregated_signature VARCHAR(256),
					created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
				`, blockTable)
				_, err := bm.db.Exec(createBlockTable)
				if err != nil {
					return fmt.Errorf("failed to create %s table: %w", blockTable, err)
				}

				// create the block header table
				headerTable := bm.getHeaderTableName(chainType)
				createHeaderTable := fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS %s (
					block_hash VARCHAR(64) PRIMARY KEY,
					previous_hash VARCHAR(128) NOT NULL,
					block_height INT NOT NULL,
					timestamp DOUBLE NOT NULL,
					merkle_root VARCHAR(64),
					chain_type INT NOT NULL,
					aggregated_signature VARCHAR(256),
					created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
				`, headerTable)
				_, err = bm.db.Exec(createHeaderTable)
				if err != nil {
					return fmt.Errorf("failed to create %s table: %w", headerTable, err)
				}

				// add a block_height unique index to the own chain's block and header tables
				blockHeightIndex := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_block_height ON %s(block_height)`, chainType, blockTable)
				_, _ = bm.db.Exec(blockHeightIndex)
				headerHeightIndex := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_header_height ON %s(block_height)`, chainType, headerTable)
				_, _ = bm.db.Exec(headerHeightIndex)
			} else {
				// other chains: create header tables only
				headerTable := bm.getHeaderTableName(chainType)
				createHeaderTable := fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS %s (
					block_hash VARCHAR(64) PRIMARY KEY,
					previous_hash VARCHAR(128) NOT NULL,
					block_height INT NOT NULL,
					timestamp DOUBLE NOT NULL,
					merkle_root VARCHAR(64),
					chain_type INT NOT NULL,
					aggregated_signature VARCHAR(256),
					created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
				`, headerTable)
				_, err := bm.db.Exec(createHeaderTable)
				if err != nil {
					return fmt.Errorf("failed to create %s table: %w", headerTable, err)
				}

				// add a block_height unique index to the other chains' header tables
				headerHeightIndex := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_header_height ON %s(block_height)`, chainType, headerTable)
				_, _ = bm.db.Exec(headerHeightIndex)
			}
		}
	}

	// create the blockchain_info table
	infoTable := bm.getInfoTableName()
	createInfoTable := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		chain_type VARCHAR(32) PRIMARY KEY,
		latest_block_hash VARCHAR(64) DEFAULT '',
		latest_block_height INT DEFAULT 0,
		last_updated TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
	`, infoTable)
	_, err := bm.db.Exec(createInfoTable)
	if err != nil {
		return fmt.Errorf("failed to create blockchain_info table: %w", err)
	}

	// initialize the blockchain info
	for _, chainType := range chainTypes {
		var count int
		infoTable := bm.getInfoTableName()
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE chain_type = ?", infoTable)
		if err := bm.db.QueryRow(query, chainType).Scan(&count); err != nil {
			log.Printf("failed to check blockchain info: %v", err)
			continue
		}

		if count == 0 {
			insertQuery := fmt.Sprintf("INSERT INTO %s (chain_type, latest_block_hash, latest_block_height) VALUES (?, '', 0)", infoTable)
			if _, err := bm.db.Exec(insertQuery, chainType); err != nil {
				log.Printf("failed to initialize %s chain info: %v", chainType, err)
			}
		}
	}

	// no longer auto-create the genesis block; create empty tables only

	log.Printf("block manager table schema initialized successfully, node-specific tables created")
	return nil
}

// initBlockchainInfo initializes the blockchain info
func (bm *BlockManager) initBlockchainInfo() error {
	// initialize blockchain info for the four chains
	chainTypes := []string{"management", "development", "test", "operations"}

	for _, chainType := range chainTypes {
		// check whether it already exists
		var count int
		query := `SELECT COUNT(*) FROM blockchain_info WHERE chain_type = ?`
		if err := bm.db.QueryRow(query, chainType).Scan(&count); err != nil {
			log.Printf("failed to check blockchain info: %v", err)
			continue
		}

		// if it does not exist, insert the default value
		if count == 0 {
			insertQuery := `INSERT INTO blockchain_info (chain_type, latest_block_hash, latest_block_height) VALUES (?, '', 0)`
			if _, err := bm.db.Exec(insertQuery, chainType); err != nil {
				log.Printf("failed to initialize %s chain info: %v", chainType, err)
			}
		}
	}

	log.Printf("blockchain info initialized successfully")
	return nil
}

// Close closes the database
func (bm *BlockManager) Close() error {
	return bm.db.Close()
}

// AddBlock adds a block
func (bm *BlockManager) AddBlock(block interface{}) error {
	blockObj, ok := block.(*types.Block)
	if !ok {
		return NewDatabaseError("AddBlock", fmt.Errorf("invalid block type"))
	}

	// determine the chain type
	chainType := "management"
	if blockObj.Header != nil {
		switch blockObj.Header.ChainType {
		case 1:
			chainType = "management"
		case 2:
			chainType = "development"
		case 3:
			chainType = "test"
		case 4:
			chainType = "operations"
		default:
			chainType = "management"
		}
	}

	// use the corresponding block table
	blockTable := bm.getBlockTableName(chainType)
	if blockTable == "" {
		return NewDatabaseError("AddBlock", fmt.Errorf("the current node type cannot store full blocks of this chain"))
	}

	// insert the full block into the chain's block table
	transactions, err := json.Marshal(blockObj.Transactions)
	if err != nil {
		return WrapDatabaseError("AddBlock", fmt.Errorf("failed to serialize transactions: %w", err))
	}

	query := fmt.Sprintf(`INSERT INTO %s 
		(block_hash, previous_hash, block_height, timestamp, transactions, merkle_root, chain_type, aggregated_signature) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?) 
		ON DUPLICATE KEY UPDATE 
			previous_hash = VALUES(previous_hash),
			block_height = VALUES(block_height),
			timestamp = VALUES(timestamp),
			transactions = VALUES(transactions),
			merkle_root = VALUES(merkle_root),
			chain_type = VALUES(chain_type),
			aggregated_signature = VALUES(aggregated_signature)`, blockTable)

	// compute the block hash
	blockHash := blockObj.Header.CalculateHash()

	_, err = bm.db.Exec(query,
		blockHash,
		blockObj.Header.PreviousHash,
		blockObj.Header.BlockHeight,
		blockObj.Header.Timestamp,
		transactions,
		blockObj.Header.MerkleRoot,
		blockObj.Header.ChainType,
		blockObj.Header.AggregatedSignature,
	)
	if err != nil {
		return WrapDatabaseError("AddBlock", fmt.Errorf("failed to save block: %w", err))
	}

	// also update the chain's header table
	headerTable := bm.getHeaderTableName(chainType)
	headerQuery := fmt.Sprintf(`INSERT INTO %s 
		(block_hash, previous_hash, block_height, timestamp, merkle_root, chain_type, aggregated_signature) 
		VALUES (?, ?, ?, ?, ?, ?, ?) 
		ON DUPLICATE KEY UPDATE 
			previous_hash = VALUES(previous_hash),
			block_height = VALUES(block_height),
			timestamp = VALUES(timestamp),
			merkle_root = VALUES(merkle_root),
			chain_type = VALUES(chain_type),
			aggregated_signature = VALUES(aggregated_signature)`, headerTable)

	_, err = bm.db.Exec(headerQuery,
		blockHash,
		blockObj.Header.PreviousHash,
		blockObj.Header.BlockHeight,
		blockObj.Header.Timestamp,
		blockObj.Header.MerkleRoot,
		blockObj.Header.ChainType,
		blockObj.Header.AggregatedSignature,
	)
	if err != nil {
		log.Printf("failed to update block header: %v", err)
	}

	// update the blockchain info
	if err := bm.updateBlockchainInfo(chainType, blockHash, blockObj.Header.BlockHeight); err != nil {
		log.Printf("failed to update blockchain info: %v", err)
	}

	// update the in-memory cache
	bm.mu.Lock()
	bm.blocks[blockHash] = blockObj
	bm.mu.Unlock()

	log.Printf("block added: %s (chain type: %s, height: %d)", SafeSubstring(blockHash, 16), chainType, blockObj.Header.BlockHeight)
	return nil
}

// UpdateBlockWithAggregatedSignature updates a block's aggregate signature and hash
func (bm *BlockManager) UpdateBlockWithAggregatedSignature(blockHash string, aggregatedSignature string) error {
	log.Printf("[block update] starting to update block aggregate signature: blockHash=%s, aggregatedSignature=%s", SafeSubstring(blockHash, 16), SafeSubstring(aggregatedSignature, 32))

	// find the block, either in the in-memory cache or in the database
	var blockObj *types.Block
	var exists bool

	// search the in-memory cache first
	bm.mu.RLock()
	blockObj, exists = bm.blocks[blockHash]
	bm.mu.RUnlock()

	if exists {
		log.Printf("[block update] block found in in-memory cache: blockHash=%s", SafeSubstring(blockHash, 16))
	} else {
		log.Printf("[block update] block not found in in-memory cache, querying database: blockHash=%s", SafeSubstring(blockHash, 16))
		// if not in memory, look it up in the database
		blockInterface, err := bm.GetBlock(blockHash)
		if err == nil {
			if block, ok := blockInterface.(*types.Block); ok {
				blockObj = block
				exists = true
				log.Printf("[block update] block found in database: blockHash=%s", SafeSubstring(blockHash, 16))
			}
		} else {
			log.Printf("[block update] failed to look up block in database: %v", err)
		}
	}

	if !exists {
		log.Printf("[block update] block not found: blockHash=%s", SafeSubstring(blockHash, 16))
		return NewDatabaseError("UpdateBlockWithAggregatedSignature", fmt.Errorf("block not found: %s", blockHash))
	}

	// update the block header's aggregate signature
	// first compute the old block hash (with the old AggregatedSignature)
	oldBlockHash := blockObj.Header.CalculateHash()

	// update the AggregatedSignature
	blockObj.Header.AggregatedSignature = aggregatedSignature

	// compute the new block hash (with the new AggregatedSignature)
	newBlockHash := blockObj.Header.CalculateHash()
	log.Printf("[block update] computing new block hash: oldHash=%s, newHash=%s", SafeSubstring(oldBlockHash, 16), SafeSubstring(newBlockHash, 16))

	// determine the chain type
	blockChainType := "management"
	if blockObj.Header != nil {
		switch blockObj.Header.ChainType {
		case 1:
			blockChainType = "management"
		case 2:
			blockChainType = "development"
		case 3:
			blockChainType = "test"
		case 4:
			blockChainType = "operations"
		default:
			blockChainType = "management"
		}
	}
	log.Printf("[block update] blockchain type: blockChainType=%s, nodeType=%s", blockChainType, bm.nodeType)

	// decide whether to update the full block:
	// 1. the management node updates full blocks of all chains
	// 2. a DTO node updates full blocks of its own chain only
	shouldUpdateFullBlock := (bm.nodeType == "management") || (bm.nodeType == blockChainType)
	log.Printf("[block update] full-block update needed: shouldUpdateFullBlock=%v", shouldUpdateFullBlock)

	// if the full block must be updated, update only the aggregate signature, not the hash field
	if shouldUpdateFullBlock {
		blockTable := bm.getBlockTableName(blockChainType)
		if blockTable == "" {
			log.Printf("[block update] cannot get block table name: chainType=%s, nodeType=%s", blockChainType, bm.nodeType)
			return NewDatabaseError("UpdateBlockWithAggregatedSignature", fmt.Errorf("cannot get block table name: chainType=%s, nodeType=%s", blockChainType, bm.nodeType))
		}
		log.Printf("[block update] preparing to update full block table (aggregate signature only): table=%s", blockTable)
		query := fmt.Sprintf(`UPDATE %s SET aggregated_signature = ? WHERE block_hash = ?`, blockTable)
		_, err := bm.db.Exec(query, aggregatedSignature, blockHash)
		if err != nil {
			log.Printf("[block update] failed to update full block: %v", err)
			return WrapDatabaseError("UpdateBlockWithAggregatedSignature", fmt.Errorf("failed to update block: %w", err))
		}
		log.Printf("[block update] full block updated successfully (aggregate signature only)")
	}

	// update the header table (all nodes update it; aggregate signature only, not the hash field)
	headerTable := bm.getHeaderTableName(blockChainType)
	log.Printf("[block update] preparing to update header table (aggregate signature only): table=%s", headerTable)
	headerQuery := fmt.Sprintf(`UPDATE %s SET aggregated_signature = ? WHERE block_hash = ?`, headerTable)
	_, err := bm.db.Exec(headerQuery, aggregatedSignature, blockHash)
	if err != nil {
		log.Printf("[block update] failed to update block header: %v", err)
		return WrapDatabaseError("UpdateBlockWithAggregatedSignature", fmt.Errorf("failed to update block header: %w", err))
	}
	log.Printf("[block update] header updated successfully (aggregate signature only)")

	// update blockchain info (set latest_block_hash to the new hash including the aggregate signature)
	log.Printf("[block update] preparing to update blockchain info: blockChainType=%s, newBlockHash=%s, height=%d",
		blockChainType, SafeSubstring(newBlockHash, 16), blockObj.Header.BlockHeight)
	if err := bm.updateBlockchainInfo(blockChainType, newBlockHash, blockObj.Header.BlockHeight); err != nil {
		log.Printf("[block update] failed to update blockchain info: %v", err)
	} else {
		log.Printf("[block update] blockchain info updated successfully")
	}

	// update the in-memory cache
	bm.mu.Lock()
	delete(bm.blocks, blockHash)
	bm.blocks[newBlockHash] = blockObj
	bm.mu.Unlock()

	log.Printf("block updated: %s (chain type: %s, height: %d)", SafeSubstring(newBlockHash, 16), blockChainType, blockObj.Header.BlockHeight)
	return nil
}

// UpdateBlockWithAggregatedSignatureByHeight updates a block's aggregate signature and hash by height
func (bm *BlockManager) UpdateBlockWithAggregatedSignatureByHeight(blockHeight int64, chainType types.TransactionType, aggregatedSignature string) error {
	log.Printf("[block update] starting to update block aggregate signature (by height): blockHeight=%d, chainType=%s, aggregatedSignature=%s", blockHeight, chainType.String(), SafeSubstring(aggregatedSignature, 32))

	// find the block, either in the in-memory cache or in the database
	var blockObj *types.Block
	var exists bool

	// search the in-memory cache first
	bm.mu.RLock()
	for _, block := range bm.blocks {
		if int64(block.Header.BlockHeight) == blockHeight && block.Header.ChainType == int(chainType) {
			blockObj = block
			exists = true
			break
		}
	}
	bm.mu.RUnlock()

	if exists {
		log.Printf("[block update] block found in in-memory cache: blockHeight=%d", blockHeight)
	} else {
		log.Printf("[block update] block not found in in-memory cache, querying database: blockHeight=%d", blockHeight)
		// if not in memory, look it up in the database
		blockInterface, err := bm.GetBlockByHeight(blockHeight, chainType)
		if err == nil {
			if block, ok := blockInterface.(*types.Block); ok {
				blockObj = block
				exists = true
				log.Printf("[block update] block found in database: blockHeight=%d", blockHeight)
			}
		} else {
			log.Printf("[block update] failed to look up block in database: %v", err)
		}
	}

	if !exists {
		log.Printf("[block update] block not found: blockHeight=%d", blockHeight)
		return NewDatabaseError("UpdateBlockWithAggregatedSignatureByHeight", fmt.Errorf("block not found: height=%d", blockHeight))
	}

	// update the block header's aggregate signature
	// first compute the old block hash (with the old AggregatedSignature)
	oldBlockHash := blockObj.Header.CalculateHash()

	// update the AggregatedSignature
	blockObj.Header.AggregatedSignature = aggregatedSignature

	// compute the new block hash (with the new AggregatedSignature)
	newBlockHash := blockObj.Header.CalculateHash()
	log.Printf("[block update] computing new block hash: oldHash=%s, newHash=%s", SafeSubstring(oldBlockHash, 16), SafeSubstring(newBlockHash, 16))

	// determine the chain type
	blockChainType := "management"
	if blockObj.Header != nil {
		switch blockObj.Header.ChainType {
		case 1:
			blockChainType = "management"
		case 2:
			blockChainType = "development"
		case 3:
			blockChainType = "test"
		case 4:
			blockChainType = "operations"
		default:
			blockChainType = "management"
		}
	}
	log.Printf("[block update] blockchain type: blockChainType=%s, nodeType=%s", blockChainType, bm.nodeType)

	// decide whether to update the full block:
	// 1. the management node updates full blocks of all chains
	// 2. a DTO node updates full blocks of its own chain only
	shouldUpdateFullBlock := (bm.nodeType == "management") || (bm.nodeType == blockChainType)
	log.Printf("[block update] full-block update needed: shouldUpdateFullBlock=%v", shouldUpdateFullBlock)

	// if the full block must be updated, update the block table
	if shouldUpdateFullBlock {
		blockTable := bm.getBlockTableName(blockChainType)
		if blockTable == "" {
			log.Printf("[block update] cannot get block table name: chainType=%s, nodeType=%s", blockChainType, bm.nodeType)
			return NewDatabaseError("UpdateBlockWithAggregatedSignatureByHeight", fmt.Errorf("cannot get block table name: chainType=%s, nodeType=%s", blockChainType, bm.nodeType))
		}
		log.Printf("[block update] preparing to update full block table: table=%s", blockTable)
		query := fmt.Sprintf(`UPDATE %s SET block_hash = ?, aggregated_signature = ? WHERE block_height = ?`, blockTable)
		_, err := bm.db.Exec(query, newBlockHash, aggregatedSignature, blockHeight)
		if err != nil {
			log.Printf("[block update] failed to update full block: %v", err)
			return WrapDatabaseError("UpdateBlockWithAggregatedSignatureByHeight", fmt.Errorf("failed to update block: %w", err))
		}
		log.Printf("[block update] full block updated successfully")
	}

	// update the header table (all nodes update the header table)
	headerTable := bm.getHeaderTableName(blockChainType)
	log.Printf("[block update] preparing to update header table: table=%s", headerTable)
	headerQuery := fmt.Sprintf(`UPDATE %s SET block_hash = ?, aggregated_signature = ? WHERE block_height = ?`, headerTable)
	_, err := bm.db.Exec(headerQuery, newBlockHash, aggregatedSignature, blockHeight)
	if err != nil {
		log.Printf("[block update] failed to update block header: %v", err)
		return WrapDatabaseError("UpdateBlockWithAggregatedSignatureByHeight", fmt.Errorf("failed to update block header: %w", err))
	}
	log.Printf("[block update] header updated successfully")

	// update blockchain info (set latest_block_hash to the new hash including the aggregate signature)
	log.Printf("[block update] preparing to update blockchain info: blockChainType=%s, newBlockHash=%s, height=%d",
		blockChainType, SafeSubstring(newBlockHash, 16), blockObj.Header.BlockHeight)
	if err := bm.updateBlockchainInfo(blockChainType, newBlockHash, blockObj.Header.BlockHeight); err != nil {
		log.Printf("[block update] failed to update blockchain info: %v", err)
	} else {
		log.Printf("[block update] blockchain info updated successfully")
	}

	// update the in-memory cache
	bm.mu.Lock()
	delete(bm.blocks, oldBlockHash)
	bm.blocks[newBlockHash] = blockObj
	bm.mu.Unlock()

	log.Printf("block updated: %s (chain type: %s, height: %d)", SafeSubstring(newBlockHash, 16), blockChainType, blockObj.Header.BlockHeight)
	return nil
}

// AddBlockHeader adds a block header
func (bm *BlockManager) AddBlockHeader(block interface{}) error {
	blockObj, ok := block.(*types.Block)
	if !ok {
		return NewDatabaseError("AddBlockHeader", fmt.Errorf("invalid block type"))
	}

	// determine the chain type
	chainType := "management"
	if blockObj.Header != nil {
		switch blockObj.Header.ChainType {
		case 1:
			chainType = "management"
		case 2:
			chainType = "development"
		case 3:
			chainType = "test"
		case 4:
			chainType = "operations"
		default:
			chainType = "management"
		}
	}

	// use the corresponding header table
	headerTable := bm.getHeaderTableName(chainType)

	// compute the block hash
	blockHash := blockObj.Header.CalculateHash()

	// insert the header into the chain's header table
	query := fmt.Sprintf(`INSERT INTO %s 
		(block_hash, previous_hash, block_height, timestamp, merkle_root, chain_type, aggregated_signature) 
		VALUES (?, ?, ?, ?, ?, ?, ?) 
		ON DUPLICATE KEY UPDATE 
			previous_hash = VALUES(previous_hash),
			block_height = VALUES(block_height),
			timestamp = VALUES(timestamp),
			merkle_root = VALUES(merkle_root),
			chain_type = VALUES(chain_type),
			aggregated_signature = VALUES(aggregated_signature)`, headerTable)

	_, err := bm.db.Exec(query,
		blockHash,
		blockObj.Header.PreviousHash,
		blockObj.Header.BlockHeight,
		blockObj.Header.Timestamp,
		blockObj.Header.MerkleRoot,
		blockObj.Header.ChainType,
		blockObj.Header.AggregatedSignature,
	)
	if err != nil {
		return WrapDatabaseError("AddBlockHeader", fmt.Errorf("failed to save block header: %w", err))
	}

	// update the blockchain info
	if err := bm.updateBlockchainInfo(chainType, blockHash, blockObj.Header.BlockHeight); err != nil {
		log.Printf("failed to update blockchain info: %v", err)
	}

	log.Printf("header added: %s (chain type: %s, height: %d)", SafeSubstring(blockHash, 16), chainType, blockObj.Header.BlockHeight)
	return nil
}

// GetBlock returns a block
func (bm *BlockManager) GetBlock(blockHash string) (interface{}, error) {
	log.Printf("[get block] starting to get block: hash=%s", SafeSubstring(blockHash, 16))

	bm.mu.RLock()
	if block, exists := bm.blocks[blockHash]; exists {
		bm.mu.RUnlock()
		log.Printf("[get block] block found in in-memory cache: hash=%s", SafeSubstring(blockHash, 16))
		return block, nil
	}
	bm.mu.RUnlock()
	log.Printf("[get block] block not found in in-memory cache, querying database: hash=%s", SafeSubstring(blockHash, 16))

	// query the block across all chain-type tables
	chainTypes := []string{"management", "development", "test", "operations"}

	// prefer the full block table
	for _, chainType := range chainTypes {
		blockTable := bm.getBlockTableName(chainType)
		if blockTable != "" {
			query := fmt.Sprintf(`SELECT block_hash, '%s' as chain_type, previous_hash, block_height, timestamp, transactions, merkle_root, chain_type, aggregated_signature 
				FROM %s 
				WHERE block_hash = ?`, chainType, blockTable)

			log.Printf("[get block] querying full block table: table=%s, hash=%s", blockTable, SafeSubstring(blockHash, 16))
			row := bm.db.QueryRow(query, blockHash)
			block, err := bm.scanBlock(row)
			if err == nil {
				// update the in-memory cache
				bm.mu.Lock()
				bm.blocks[blockHash] = block
				bm.mu.Unlock()

				log.Printf("[get block] block found in full block table: hash=%s, chainType=%s", SafeSubstring(blockHash, 16), chainType)
				return block, nil
			}
		}
	}

	// if no full block exists, try the header table
	for _, chainType := range chainTypes {
		headerTable := bm.getHeaderTableName(chainType)
		query := fmt.Sprintf(`SELECT block_hash, '%s' as chain_type, previous_hash, block_height, timestamp, NULL as transactions, merkle_root, chain_type, aggregated_signature 
			FROM %s 
			WHERE block_hash = ?`, chainType, headerTable)

		log.Printf("[get block] querying header table: table=%s, hash=%s", headerTable, SafeSubstring(blockHash, 16))
		row := bm.db.QueryRow(query, blockHash)
		block, err := bm.scanBlock(row)
		if err == nil {
			// update the in-memory cache
			bm.mu.Lock()
			bm.blocks[blockHash] = block
			bm.mu.Unlock()

			log.Printf("[get block] block found in header table: hash=%s, chainType=%s", SafeSubstring(blockHash, 16), chainType)
			return block, nil
		}
	}

	log.Printf("[get block] block not found: hash=%s", SafeSubstring(blockHash, 16))
	return nil, NewDatabaseError("GetBlock", fmt.Errorf("block not found"))
}

// GetBlockByHeight returns a block by height
func (bm *BlockManager) GetBlockByHeight(height int64, blockType interface{}) (interface{}, error) {
	var blockTypeStr string

	// convert blockType to int, then to the chain-type string
	var intVal int
	switch v := blockType.(type) {
	case string:
		blockTypeStr = v
	case int:
		intVal = v
	case int8:
		intVal = int(v)
	case int16:
		intVal = int(v)
	case int32:
		intVal = int(v)
	case int64:
		intVal = int(v)
	default:
		blockTypeStr = "management"
	}

	// if an int was obtained, convert to the chain-type string
	if intVal != 0 || blockTypeStr == "" {
		switch intVal {
		case 1:
			blockTypeStr = "management"
		case 2:
			blockTypeStr = "development"
		case 3:
			blockTypeStr = "test"
		case 4:
			blockTypeStr = "operations"
		default:
			blockTypeStr = "management"
		}
	}

	// use the corresponding block table
	blockTable := bm.getBlockTableName(blockTypeStr)
	log.Printf("[GetBlockByHeight] querying block: height=%d, chainType=%s, nodeType=%s, blockTable=%s",
		height, blockTypeStr, bm.nodeType, blockTable)

	// query the full block table
	if blockTable != "" {
		query := fmt.Sprintf(`SELECT block_hash, '%s' as chain_type, previous_hash, block_height, timestamp, transactions, merkle_root, %d as chain_type_int, aggregated_signature
			FROM %s
			WHERE block_height = ?`, blockTypeStr, intVal, blockTable)

		log.Printf("[GetBlockByHeight] executing query: %s, height=%d", query, height)
		row := bm.db.QueryRow(query, height)
		block, err := bm.scanBlock(row)
		if err == nil {
			log.Printf("[GetBlockByHeight] found in block table: height=%d, chainType=%d, hash=%s",
				block.Header.BlockHeight, block.Header.ChainType, block.Header.CalculateHash())
			return block, nil
		}
		log.Printf("[GetBlockByHeight] not found in block table: %v", err)
	} else {
		log.Printf("[GetBlockByHeight] block table name is empty; skip the block table query")
	}

	// if no full block exists, try the header table
	headerTable := bm.getHeaderTableName(blockTypeStr)
	log.Printf("[GetBlockByHeight] querying header table: table=%s, height=%d", headerTable, height)
	query := fmt.Sprintf(`SELECT block_hash, '%s' as chain_type, previous_hash, block_height, timestamp, NULL as transactions, merkle_root, chain_type, aggregated_signature
		FROM %s
		WHERE block_height = ?`, blockTypeStr, headerTable)

	row := bm.db.QueryRow(query, height)
	block, err := bm.scanBlock(row)
	if err == nil {
		log.Printf("[GetBlockByHeight] found in header table: height=%d, chainType=%d, hash=%s",
			block.Header.BlockHeight, block.Header.ChainType, block.Header.CalculateHash())
	} else {
		log.Printf("ℹ[GetBlockByHeight] not found in header table either: %v", err)
	}
	return block, err
}

// GetLatestBlock returns the latest block
func (bm *BlockManager) GetLatestBlock(blockType interface{}) (interface{}, error) {
	var blockTypeStr string

	// convert blockType to int, then to the chain-type string
	var intVal int
	switch v := blockType.(type) {
	case string:
		blockTypeStr = v
	case int:
		intVal = v
	case int8:
		intVal = int(v)
	case int16:
		intVal = int(v)
	case int32:
		intVal = int(v)
	case int64:
		intVal = int(v)
	default:
		blockTypeStr = "management"
	}

	// if an int was obtained, convert to the chain-type string
	if intVal != 0 || blockTypeStr == "" {
		switch intVal {
		case 1:
			blockTypeStr = "management"
		case 2:
			blockTypeStr = "development"
		case 3:
			blockTypeStr = "test"
		case 4:
			blockTypeStr = "operations"
		default:
			blockTypeStr = "management"
		}
	}

	infoTable := bm.getInfoTableName()
	query := fmt.Sprintf(`SELECT latest_block_height FROM %s WHERE chain_type = ?`, infoTable)

	log.Printf("[get latest block] preparing to query latest block height: table=%s, chainType=%s", infoTable, blockTypeStr)

	row := bm.db.QueryRow(query, blockTypeStr)

	var latestBlockHeight int64
	if err := row.Scan(&latestBlockHeight); err != nil {
		if err == sql.ErrNoRows {
			log.Printf("[get latest block] blockchain info not found: chainType=%s", blockTypeStr)
			return nil, fmt.Errorf("blockchain info not found")
		}
		return nil, WrapDatabaseError("GetLatestBlock", fmt.Errorf("failed to get latest block height: %w", err))
	}

	log.Printf("[get latest block] query succeeded: chainType=%s, latestBlockHeight=%d", blockTypeStr, latestBlockHeight)

	if latestBlockHeight == 0 {
		log.Printf("[get latest block] latest block height is 0: chainType=%s", blockTypeStr)
		return nil, fmt.Errorf("no blocks")
	}

	// get the latest block by height, avoiding hash mismatch issues
	return bm.GetBlockByHeight(latestBlockHeight, blockTypeStr)
}

// GetBlocksByRange returns blocks by range
func (bm *BlockManager) GetBlocksByRange(startHeight, endHeight int64, blockType interface{}) ([]interface{}, error) {
	var blockTypeStr string

	// convert blockType to int, then to the chain-type string
	var intVal int
	switch v := blockType.(type) {
	case string:
		blockTypeStr = v
	case int:
		intVal = v
	case int8:
		intVal = int(v)
	case int16:
		intVal = int(v)
	case int32:
		intVal = int(v)
	case int64:
		intVal = int(v)
	default:
		blockTypeStr = "management"
	}

	// if an int was obtained, convert to the chain-type string
	if intVal != 0 || blockTypeStr == "" {
		switch intVal {
		case 1:
			blockTypeStr = "management"
		case 2:
			blockTypeStr = "development"
		case 3:
			blockTypeStr = "test"
		case 4:
			blockTypeStr = "operations"
		default:
			blockTypeStr = "management"
		}
	}

	// use the corresponding block table
	blockTable := bm.getBlockTableName(blockTypeStr)

	// query the full block table
	var blocks []interface{}
	if blockTable != "" {
		chainTypeInt := 1
		if blockTypeStr == "development" {
			chainTypeInt = 2
		} else if blockTypeStr == "test" {
			chainTypeInt = 3
		} else if blockTypeStr == "operations" {
			chainTypeInt = 4
		}
		query := fmt.Sprintf(`SELECT block_hash, '%s' as chain_type, previous_hash, block_height, timestamp, transactions, merkle_root, %d as chain_type_int, aggregated_signature
			FROM %s
			WHERE block_height BETWEEN ? AND ?
			ORDER BY block_height ASC`, blockTypeStr, chainTypeInt, blockTable)

		rows, err := bm.db.Query(query, startHeight, endHeight)
		if err != nil {
			return nil, WrapDatabaseError("GetBlocksByRange", fmt.Errorf("failed to query blocks: %w", err))
		}
		defer rows.Close()

		for rows.Next() {
			block, err := bm.scanBlockRow(rows)
			if err != nil {
				log.Printf("failed to parse block data: %v", err)
				continue
			}
			blocks = append(blocks, block)

			// update the in-memory cache
			bm.mu.Lock()
			cachedBlockHash := block.Header.CalculateHash()
			bm.blocks[cachedBlockHash] = block
			bm.mu.Unlock()
		}
	}

	// if no full block was found, try the header table
	if len(blocks) == 0 {
		headerTable := bm.getHeaderTableName(blockTypeStr)
		chainTypeInt := 1
		if blockTypeStr == "development" {
			chainTypeInt = 2
		} else if blockTypeStr == "test" {
			chainTypeInt = 3
		} else if blockTypeStr == "operations" {
			chainTypeInt = 4
		}
		query := fmt.Sprintf(`SELECT block_hash, '%s' as chain_type, previous_hash, block_height, timestamp, NULL as transactions, merkle_root, %d as chain_type_int, aggregated_signature
			FROM %s
			WHERE block_height BETWEEN ? AND ?
			ORDER BY block_height ASC`, blockTypeStr, chainTypeInt, headerTable)

		rows, err := bm.db.Query(query, startHeight, endHeight)
		if err != nil {
			return nil, WrapDatabaseError("GetBlocksByRange", fmt.Errorf("failed to query block headers: %w", err))
		}
		defer rows.Close()

		for rows.Next() {
			block, err := bm.scanBlockRow(rows)
			if err != nil {
				log.Printf("failed to parse header data: %v", err)
				continue
			}
			blocks = append(blocks, block)
		}
	}

	return blocks, nil
}

// GetBlockchainHeight returns the blockchain height
func (bm *BlockManager) GetBlockchainHeight(blockType interface{}) (int64, error) {
	var blockTypeStr string

	// convert blockType to int, then to the chain-type string
	var intVal int
	switch v := blockType.(type) {
	case string:
		blockTypeStr = v
	case int:
		intVal = v
	case int8:
		intVal = int(v)
	case int16:
		intVal = int(v)
	case int32:
		intVal = int(v)
	case int64:
		intVal = int(v)
	default:
		blockTypeStr = "management"
	}

	// if an int was obtained, convert to the chain-type string
	if intVal != 0 || blockTypeStr == "" {
		switch intVal {
		case 1:
			blockTypeStr = "management"
		case 2:
			blockTypeStr = "development"
		case 3:
			blockTypeStr = "test"
		case 4:
			blockTypeStr = "operations"
		default:
			blockTypeStr = "management"
		}
	}

	infoTable := bm.getInfoTableName()
	query := fmt.Sprintf(`SELECT latest_block_height FROM %s WHERE chain_type = ?`, infoTable)
	row := bm.db.QueryRow(query, blockTypeStr)

	var height int64
	if err := row.Scan(&height); err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("blockchain info not found")
		}
		return 0, WrapDatabaseError("GetBlockchainHeight", fmt.Errorf("failed to get blockchain height: %w", err))
	}

	return height, nil
}

// GetBlockCount returns the block count
func (bm *BlockManager) GetBlockCount(blockType interface{}) (int64, error) {
	var blockTypeStr string

	// convert blockType to int, then to the chain-type string
	var intVal int
	switch v := blockType.(type) {
	case string:
		blockTypeStr = v
	case int:
		intVal = v
	case int8:
		intVal = int(v)
	case int16:
		intVal = int(v)
	case int32:
		intVal = int(v)
	case int64:
		intVal = int(v)
	default:
		blockTypeStr = "management"
	}

	// if an int was obtained, convert to the chain-type string
	if intVal != 0 || blockTypeStr == "" {
		switch intVal {
		case 1:
			blockTypeStr = "management"
		case 2:
			blockTypeStr = "development"
		case 3:
			blockTypeStr = "test"
		case 4:
			blockTypeStr = "operations"
		default:
			blockTypeStr = "management"
		}
	}

	// use the corresponding block table
	blockTable := bm.getBlockTableName(blockTypeStr)
	if blockTable == "" {
		return 0, NewDatabaseError("GetBlockCount", fmt.Errorf("the current node type cannot query the full block count of this chain"))
	}

	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, blockTable)
	row := bm.db.QueryRow(query)

	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, WrapDatabaseError("GetBlockCount", fmt.Errorf("failed to get block count: %w", err))
	}

	return count, nil
}

// StoreBlockByNodeType stores a block according to node type
func (bm *BlockManager) StoreBlockByNodeType(nodeType string, block *types.Block, managerPublicKey string) error {
	// determine the chain type
	chainType := "management"
	if block.Header != nil {
		switch block.Header.ChainType {
		case 1:
			chainType = "management"
		case 2:
			chainType = "development"
		case 3:
			chainType = "test"
		case 4:
			chainType = "operations"
		default:
			chainType = "management"
		}
	}

	// DTO nodes must verify the block
	if nodeType != "management" {
		// determine the expected public key of the chain
		// the chain's public key should come from the actual node configuration
		// a sample key is used for now; a real implementation should read it from configuration or the database
		expectedPublicKey := ""
		switch chainType {
		case "management":
			expectedPublicKey = "management_node_public_key"
		case "development":
			expectedPublicKey = "development_node_public_key"
		case "test":
			expectedPublicKey = "test_node_public_key"
		case "operations":
			expectedPublicKey = "operations_node_public_key"
		}

		// verify the block
		if err := bm.VerifyBlock(block, expectedPublicKey, managerPublicKey); err != nil {
			return NewDatabaseError("StoreBlockByNodeType", fmt.Errorf("block verification failed: %w", err))
		}
	}

	// decide the storage mode based on node type and chain type
	switch nodeType {
	case "development":
		// development node: store full blocks of the development chain, headers of other chains
		if chainType == "development" {
			return bm.AddBlock(block)
		} else {
			return bm.AddBlockHeader(block)
		}
	case "test":
		// test node: store full blocks of the test chain, headers of other chains
		if chainType == "test" {
			return bm.AddBlock(block)
		} else {
			return bm.AddBlockHeader(block)
		}
	case "operations":
		// operations node: store full blocks of the operations chain, headers of other chains
		if chainType == "operations" {
			return bm.AddBlock(block)
		} else {
			return bm.AddBlockHeader(block)
		}
	default:
		// management node: store full blocks of all chains
		return bm.AddBlock(block)
	}
}

// VerifyBlock verifies a block (used by DTO nodes)
func (bm *BlockManager) VerifyBlock(block *types.Block, expectedPublicKey string, managerPublicKey string) error {
	if block == nil || block.Header == nil {
		return NewDatabaseError("VerifyBlock", fmt.Errorf("block or block header is nil"))
	}

	// determine the chain type
	chainType := "management"
	switch block.Header.ChainType {
	case 1:
		chainType = "management"
	case 2:
		chainType = "development"
	case 3:
		chainType = "test"
	case 4:
		chainType = "operations"
	default:
		chainType = "management"
	}

	// verify the prehash
	if err := bm.verifyPrehash(block, chainType, expectedPublicKey); err != nil {
		return err
	}

	// verify the aggregate signature
	if err := bm.verifyAggregatedSignature(block, managerPublicKey); err != nil {
		return err
	}

	return nil
}

// verifyPrehash verifies a block's prehash
func (bm *BlockManager) verifyPrehash(block *types.Block, chainType string, expectedPublicKey string) error {
	// check the block height
	if block.Header.BlockHeight == 1 {
		// the first block's prehash should be the identity node's public key
		if block.Header.PreviousHash != expectedPublicKey {
			return NewDatabaseError("verifyPrehash", fmt.Errorf("first block prehash verification failed: expected=%s, actual=%s", expectedPublicKey, block.Header.PreviousHash))
		}
	} else {
		// a non-first block's prehash should be the previous block's hash (including the aggregate signature)
		// query the previous block from the database
		prevBlock, err := bm.GetBlockByHeight(int64(block.Header.BlockHeight-1), chainType)
		if err != nil {
			return NewDatabaseError("verifyPrehash", fmt.Errorf("failed to get previous block: %w", err))
		}

		prevBlockObj, ok := prevBlock.(*types.Block)
		if !ok {
			return NewDatabaseError("verifyPrehash", fmt.Errorf("invalid previous block type"))
		}

		prevBlockHash := prevBlockObj.Header.CalculateHash()
		if block.Header.PreviousHash != prevBlockHash {
			return NewDatabaseError("verifyPrehash", fmt.Errorf("prehash verification failed: expected=%s, actual=%s", prevBlockHash, block.Header.PreviousHash))
		}
	}

	return nil
}

// verifyAggregatedSignature verifies a block's aggregate signature
func (bm *BlockManager) verifyAggregatedSignature(block *types.Block, managerPublicKey string) error {
	// check the aggregate signature is non-empty
	if block.Header.AggregatedSignature == "" {
		return NewDatabaseError("verifyAggregatedSignature", fmt.Errorf("aggregate signature is empty"))
	}

	// check the verifier public key is non-empty
	if managerPublicKey == "" {
		return NewDatabaseError("verifyAggregatedSignature", fmt.Errorf("verifier public key is empty"))
	}

	// compute the block hash as the verification message (excluding the aggregate signature)
	blockHash := block.Header.CalculateHash()

	// verify the signature with the BLS library
	isValid, err := crypto.VerifyManagementSignature(
		managerPublicKey,
		blockHash,
		block.Header.AggregatedSignature,
	)

	if err != nil {
		return NewDatabaseError("verifyAggregatedSignature", fmt.Errorf("signature verification failed: %w", err))
	}

	if !isValid {
		return NewDatabaseError("verifyAggregatedSignature", fmt.Errorf("aggregate signature verification failed"))
	}

	return nil
}

// updateBlockchainInfo update the blockchain info
func (bm *BlockManager) updateBlockchainInfo(blockType string, latestBlockHash string, latestBlockHeight int) error {
	infoTable := bm.getInfoTableName()

	// get the current latest block height first
	var currentHeight int
	query := fmt.Sprintf(`SELECT latest_block_height FROM %s WHERE chain_type = ?`, infoTable)
	err := bm.db.QueryRow(query, blockType).Scan(&currentHeight)
	if err != nil {
		return WrapDatabaseError("updateBlockchainInfo", fmt.Errorf("failed to get current latest block height: %w", err))
	}

	// check block height continuity
	// update the blockchain info only when the new block height is currentHeight+1
	if latestBlockHeight != currentHeight+1 && currentHeight != 0 {
		log.Printf("[blockchain info update] block heights not consecutive: current=%d, new=%d, skipping update", currentHeight, latestBlockHeight)
		return nil
	}

	// update the blockchain info
	updateQuery := fmt.Sprintf(`UPDATE %s SET latest_block_hash = ?, latest_block_height = ?, last_updated = CURRENT_TIMESTAMP 
		WHERE chain_type = ?`, infoTable)

	log.Printf("[blockchain info update] preparing to update blockchain info: table=%s, chainType=%s, latestBlockHash=%s, latestBlockHeight=%d, currentHeight=%d",
		infoTable, blockType, SafeSubstring(latestBlockHash, 16), latestBlockHeight, currentHeight)

	result, err := bm.db.Exec(updateQuery, latestBlockHash, latestBlockHeight, blockType)
	if err != nil {
		return WrapDatabaseError("updateBlockchainInfo", fmt.Errorf("failed to update blockchain info: %w", err))
	}

	rowsAffected, _ := result.RowsAffected()
	log.Printf("[blockchain info update] update succeeded: rowsAffected=%d", rowsAffected)

	return nil
}

// loadRecentBlocks loads recent blocks
func (bm *BlockManager) loadRecentBlocks() error {
	// get the latest block of each chain type
	chainTypes := []string{"management", "development", "test", "operations"}

	for _, chainType := range chainTypes {
		infoTable := bm.getInfoTableName()
		query := fmt.Sprintf(`SELECT latest_block_hash FROM %s WHERE chain_type = ?`, infoTable)
		row := bm.db.QueryRow(query, chainType)

		var latestBlockHash string
		if err := row.Scan(&latestBlockHash); err != nil {
			if err != sql.ErrNoRows {
				log.Printf("failed to get latest block hash of %s chain: %v", chainType, err)
			}
			continue
		}

		if latestBlockHash != "" {
			blockInterface, err := bm.GetBlock(latestBlockHash)
			if err != nil {
				log.Printf("failed to load latest block of %s chain: %v", chainType, err)
				continue
			}

			block, ok := blockInterface.(*types.Block)
			if !ok {
				log.Printf("block type conversion failed: %s", latestBlockHash)
				continue
			}

			// load the chain's most recent blocks
			startHeight := int64(block.Header.BlockHeight) - 9
			if startHeight < 0 {
				startHeight = 0
			}

			blocks, err := bm.GetBlocksByRange(startHeight, int64(block.Header.BlockHeight), chainType)
			if err != nil {
				log.Printf("failed to load %s chain blocks: %v", chainType, err)
				continue
			}

			log.Printf("loaded %s chain blocks: %d", chainType, len(blocks))
		}
	}

	return nil
}

// scanBlock scans a block from a single row
func (bm *BlockManager) scanBlock(row *sql.Row) (*types.Block, error) {
	var blockHash, chainType, previousHash, merkleRoot, aggregatedSignature string
	var blockHeight, chainTypeInt int
	var timestamp float64
	var transactionsJSON []byte

	err := row.Scan(
		&blockHash,
		&chainType,
		&previousHash,
		&blockHeight,
		&timestamp,
		&transactionsJSON,
		&merkleRoot,
		&chainTypeInt,
		&aggregatedSignature,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("block not found")
		}
		return nil, WrapDatabaseError("scanBlock", fmt.Errorf("failed to scan block: %w", err))
	}

	// parse transactions
	var transactions []*types.Transaction
	if transactionsJSON != nil {
		if err := json.Unmarshal(transactionsJSON, &transactions); err != nil {
			return nil, WrapDatabaseError("scanBlock", fmt.Errorf("failed to parse transactions: %w", err))
		}
	} else {
		transactions = []*types.Transaction{}
	}

	// create the block
	block := &types.Block{
		Header: &types.BlockHeader{
			BlockHeight:         blockHeight,
			PreviousHash:        previousHash,
			MerkleRoot:          merkleRoot,
			Timestamp:           timestamp,
			ChainType:           chainTypeInt,
			AggregatedSignature: aggregatedSignature,
		},
		Transactions: transactions,
	}

	return block, nil
}

// scanBlockRow scans a block from a result-set row
func (bm *BlockManager) scanBlockRow(rows *sql.Rows) (*types.Block, error) {
	var blockHash, chainType, previousHash, merkleRoot, aggregatedSignature string
	var blockHeight, chainTypeInt int
	var timestamp float64
	var transactionsJSON []byte

	err := rows.Scan(
		&blockHash,
		&chainType,
		&previousHash,
		&blockHeight,
		&timestamp,
		&transactionsJSON,
		&merkleRoot,
		&chainTypeInt,
		&aggregatedSignature,
	)

	if err != nil {
		return nil, WrapDatabaseError("scanBlockRow", fmt.Errorf("failed to scan block row: %w", err))
	}

	// parse transactions
	var transactions []*types.Transaction
	if transactionsJSON != nil {
		if err := json.Unmarshal(transactionsJSON, &transactions); err != nil {
			return nil, WrapDatabaseError("scanBlockRow", fmt.Errorf("failed to parse transactions: %w", err))
		}
	} else {
		transactions = []*types.Transaction{}
	}

	// create the block
	block := &types.Block{
		Header: &types.BlockHeader{
			BlockHeight:         blockHeight,
			PreviousHash:        previousHash,
			MerkleRoot:          merkleRoot,
			Timestamp:           timestamp,
			ChainType:           chainTypeInt,
			AggregatedSignature: aggregatedSignature,
		},
		Transactions: transactions,
	}

	return block, nil
}
