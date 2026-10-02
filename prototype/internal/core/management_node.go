package core

import (
	"C"
	"fmt"
	"io"
	"log"
	"os"
	"sfchain/internal/consensus"
	"sfchain/internal/database"
	"sfchain/internal/database/mysql"
	"sfchain/internal/network"
	"sfchain/pkg/config"
	"sfchain/pkg/crypto"
	"sfchain/pkg/types"
	"strings"
	"sync"
	"time"

	bls "github.com/herumi/bls-eth-go-binary/bls"
)

// ManagementNode is the management node with BLS aggregate signature support.
type ManagementNode struct {
	BaseNode
	networkManager   *network.NodeNetwork
	ConsensusManager *consensus.ParallelConsensusManager
	TxPoolDB         *TransactionPoolDB
	BlockManager     *PersistentBlockManager
	LogProcessor     *LogProcessor

	// node private key and public key objects
	PrivateKey   string
	PublicKeyObj *bls.PublicKey

	// configuration
	BlockIntervals        map[types.TransactionType]int // block interval per chain (seconds)
	TransactionThresholds map[types.TransactionType]int // transactions per block for each chain
	MinSignatures         int                           // minimum signatures required to complete consensus

	// BLS signature fields
	signatureCache        map[string]map[string]*bls.Sign      // blockHash -> nodeID -> BLS signature
	broadcastAt           map[string]time.Time                 // blockHash -> last broadcast time (rebroadcast fallback)
	publicKeyCache        map[string]map[string]*bls.PublicKey // blockHash -> nodeID -> public key
	blockCache            map[string]*types.Block              // blockHash -> block
	managerSignatureCache map[string]string                    // blockHash -> management node signature (serialized hex string)
	mu                    sync.RWMutex

	// per-chain lock preventing races between concurrent block generations on the same chain
	chainMu map[types.TransactionType]*sync.Mutex

	// local variable tracking the next block height of each chain
	chainNextHeight map[types.TransactionType]int

	// cache of the previous block hash (with aggregate signature), used for the next block's prehash
	chainLastBlockHash map[types.TransactionType]string

	// Aggregate signature cache: key is "chainType:height", value is the aggregate signature hex string
	// used when generating the next block (block n's aggregate is stored in block n+1's AggregatedSignature field)
	pendingAggregatedSignatures map[string]string

	// per-chain trigger channel (event-triggered mode)
	chainTriggers map[types.TransactionType]chan struct{}

	// BlockSize - maximum transactions per block
	BlockSize int

	// TpsPriority - whether TPS-priority mode is enabled
	TpsPriority bool

	// timerRunning - whether the timer is running (prevents duplicate starts)
	timerRunning map[types.TransactionType]bool

	// persistence queues
	persistQueue chan struct{} // transaction status persistence task queue (empty struct used only as a signal)

	// block persistence queue (asynchronous)
	blockPersistQueue   chan *blockPersistTask
	blockPersistCache   map[string]*blockPersistTask
	blockPersistCacheMu sync.RWMutex

	StopChan chan struct{} // stop signal channel
}

// blockPersistTask is a block persistence task
type blockPersistTask struct {
	block     *types.Block
	chainType types.TransactionType
	height    int
}

// qlog emits high-frequency block-production logs; silenced when SFCHAIN_QUIET=1 (9p log write amplification is a significant component of inter-block gaps)
var qlog = log.New(io.Discard, "", log.LstdFlags)

func init() {
	if os.Getenv("SFCHAIN_QUIET") != "1" {
		qlog.SetOutput(os.Stdout)
	}
}

func NewManagementNode(nodeID string, port int) (*ManagementNode, error) {
	baseNode := NewBaseNode(nodeID, types.NodeTypeManagement, port)

	node := &ManagementNode{
		BaseNode:                    *baseNode,
		networkManager:              network.NewNodeNetwork(nodeID, fmt.Sprintf("127.0.0.1:%d", port), ""),
		signatureCache:              make(map[string]map[string]*bls.Sign),
		broadcastAt:                 make(map[string]time.Time),
		publicKeyCache:              make(map[string]map[string]*bls.PublicKey),
		blockCache:                  make(map[string]*types.Block),
		managerSignatureCache:       make(map[string]string),
		BlockIntervals:              make(map[types.TransactionType]int),
		TransactionThresholds:       make(map[types.TransactionType]int),
		MinSignatures:               4,
		chainMu:                     make(map[types.TransactionType]*sync.Mutex),
		chainNextHeight:             make(map[types.TransactionType]int),
		chainLastBlockHash:          make(map[types.TransactionType]string),
		pendingAggregatedSignatures: make(map[string]string),
		chainTriggers:               make(map[types.TransactionType]chan struct{}),
		BlockSize:                   10,
		persistQueue:                make(chan struct{}, 100),
		blockPersistQueue:           make(chan *blockPersistTask, 100),
		blockPersistCache:           make(map[string]*blockPersistTask),
		StopChan:                    make(chan struct{}),
	}

	// initialize the next block height of each chain to 1
	for i := types.TransactionTypeManagement; i <= types.TransactionTypeOperations; i++ {
		node.chainNextHeight[i] = 1
	}

	// set defaults
	for i := types.TransactionTypeManagement; i <= types.TransactionTypeOperations; i++ {
		node.BlockIntervals[i] = 10        // default 10 seconds
		node.TransactionThresholds[i] = 10 // default 10 transactions per block
	}

	// initialize the consensus manager
	node.ConsensusManager = consensus.NewParallelConsensusManager(node)

	// LogProcessor and the transaction pool database are initialized in InitializeWithConfig
	// so the correct config-file parameters are used

	// the block manager is created in InitializeWithConfig rather than NewManagementNode
	// to ensure the genesis block is created with the correct public key

	return node, nil
}

// InitializeWithConfig initializes the management node from configuration
func (mn *ManagementNode) InitializeWithConfig(cfg *config.NodeConfig) error {
	// set basic node information
	mn.PublicKey = cfg.Node.PublicKey
	mn.PrivateKey = cfg.Node.PrivateKey

	// initialize the public key object (for BLS signature verification)
	publicKeyObj, err := crypto.DeserializePublicKey(cfg.Node.PublicKey)
	if err == nil {
		mn.PublicKeyObj = publicKeyObj
	}

	mn.networkManager = network.NewNodeNetwork(cfg.Node.NodeID, cfg.Node.Address, "nats://localhost:4222")

	// initialize network nodes
	typesNodes := mn.convertConfigNodesToTypesNodes(cfg.Network.Nodes)
	mn.InitializeNetworkNodes(typesNodes)

	// output all nodes' public key information
	//log.Printf("[Management init] public keys configured for node %s (type: %s):", cfg.Node.NodeID, cfg.Node.NodeType)
	for range cfg.Network.Nodes {
		// node info is used for network initialization
	}

	// initialize the user database (shared connection via the global manager)
	if GetGlobalUserDatabaseManager() == nil || !GetGlobalUserDatabaseManager().IsInitialized() {
		if err := InitGlobalUserDatabaseManager(&cfg.Database); err == nil {
			mn.UserDatabase = GetGlobalUserDatabaseManager().GetDB()
		}
	} else {
		mn.UserDatabase = GetGlobalUserDatabaseManager().GetDB()
	}

	// initialize the log database
	dbFactory := database.NewDBFactory(&cfg.Database)
	logDB, err := dbFactory.CreateLogDatabase()
	if err != nil {
		// create an empty LogProcessor to avoid nil-pointer panics
		mn.LogProcessor = &LogProcessor{
			db:       nil,
			node:     mn,
			stopChan: make(chan struct{}),
			processTypes: []types.TransactionType{
				types.TransactionTypeManagement,
				types.TransactionTypeDevelopment,
				types.TransactionTypeTest,
				types.TransactionTypeOperations,
			},
			txThreshold: 10,
			interval:    5 * time.Second,
		}
	} else {
		// initialize the log processor
		mn.LogProcessor = NewLogProcessor(logDB, mn)
	}

	// initialize the transaction pool database (shared connection via the global manager)
	if GetGlobalTxPoolManager() == nil || !GetGlobalTxPoolManager().IsInitialized() {
		if err := InitGlobalTxPoolManager(&cfg.Database); err != nil {
			// create an empty TransactionPoolDB to avoid nil-pointer panics
			mn.TxPoolDB = &TransactionPoolDB{
				db:                   nil,
				packagingPool:        make(map[types.TransactionType][]*types.Transaction),
				userPublicKeys:       make(map[string]string),
				pendingStatusUpdates: make(map[string]*types.Transaction),
			}
		} else {
			mn.TxPoolDB = &TransactionPoolDB{
				db:                   GetGlobalTxPoolManager().GetDB(),
				packagingPool:        make(map[types.TransactionType][]*types.Transaction),
				userPublicKeys:       make(map[string]string),
				pendingStatusUpdates: make(map[string]*types.Transaction),
			}
		}
	} else {
		mn.TxPoolDB = &TransactionPoolDB{
			db:                   GetGlobalTxPoolManager().GetDB(),
			packagingPool:        make(map[types.TransactionType][]*types.Transaction),
			userPublicKeys:       make(map[string]string),
			pendingStatusUpdates: make(map[string]*types.Transaction),
		}
	}

	// load user public keys into the cache
	if mn.TxPoolDB.db != nil && mn.UserDatabase != nil {
		if err := mn.TxPoolDB.LoadUserPublicKeysToCache(mn.UserDatabase); err != nil {
			//log.Printf("[init] failed to load user public key cache: %v", err)
		}
	}

	// RQ1 reads the source logs once at the beginning of the run and uses the
	// live endorsement-to-packaging path. Reloading endorsed rows from MySQL
	// during this mode can race with block consumption and duplicate a tx.
	var loadWg sync.WaitGroup
	if os.Getenv("SFCHAIN_RQ1") != "1" {
		loadWg.Add(1)
		go func() {
			defer loadWg.Done()
			if mn.TxPoolDB.db != nil {
				if err := mn.TxPoolDB.LoadEndorsedTransactionsToPool(); err != nil {
					//log.Printf("[init] failed to load endorsed transactions into the packaging pool: %v", err)
				}
			}
		}()
	}

	// initialize the persistent block manager
	// empty dataPath because MySQL is used
	blockManager, err := NewPersistentBlockManager(types.NodeTypeManagement, "", cfg.Node.PublicKey, &cfg.Database)
	if err == nil {
		mn.BlockManager = blockManager

		// load the latest block height of each chain from the database
		mn.loadChainHeightsFromDB()
	}
	loadWg.Wait()

	// read block intervals and transaction counts from configuration
	if len(cfg.Blockchain) > 0 {
		// try reading directly from the blockchain map
		for chainType, chainConfig := range cfg.Blockchain {
			txType := types.TransactionTypeFromString(chainType)
			if txType >= types.TransactionTypeManagement && txType <= types.TransactionTypeOperations {
				if chainConfig.BlockInterval > 0 {
					mn.BlockIntervals[txType] = chainConfig.BlockInterval
				}
				if chainConfig.TransactionThreshold > 0 {
					mn.TransactionThresholds[txType] = chainConfig.TransactionThreshold
				}
			}
		}
	}

	// ensure all chain types have configuration
	for i := types.TransactionTypeManagement; i <= types.TransactionTypeOperations; i++ {
		if mn.BlockIntervals[i] <= 0 {
			mn.BlockIntervals[i] = 10 // default 10 seconds
		}
		if mn.TransactionThresholds[i] <= 0 {
			mn.TransactionThresholds[i] = 10 // default 10 transactions per block
		}
	}

	// read consensus configuration
	if cfg.Consensus.MinSignatures > 0 {
		mn.MinSignatures = cfg.Consensus.MinSignatures
	}

	// read block generation configuration
	if cfg.BlockGeneration.BlockSize > 0 {
		mn.BlockSize = cfg.BlockGeneration.BlockSize
	}

	// read TPS-priority mode configuration
	mn.TpsPriority = cfg.BlockGeneration.TpsPriority

	// initialize the timer running flags
	mn.timerRunning = make(map[types.TransactionType]bool)
	for i := types.TransactionTypeManagement; i <= types.TransactionTypeOperations; i++ {
		mn.timerRunning[i] = false
	}

	// start the persistence worker goroutine
	go mn.persistenceWorker()
	// start the block persistence worker goroutine
	go mn.blockPersistenceWorker()

	return nil
}

// CreateTransaction creates a new transaction (management-node specific)
func (mn *ManagementNode) CreateTransaction(txType types.TransactionType, logData map[string]interface{}) (*types.Transaction, error) {
	// check mn is not nil
	if mn == nil {
		return nil, fmt.Errorf("ManagementNode is nil")
	}

	// ensure LogData contains the user_id field
	userID := ""
	if id, ok := logData["user_id"].(string); ok && id != "" {
		userID = id
	} else if id, ok := logData["UserID"].(string); ok && id != "" {
		userID = id
		logData["user_id"] = id
	} else {
		return nil, fmt.Errorf("log data is missing the user_id field")
	}
	// Canonical factory timestamp is integer milliseconds. Fractional DOUBLE
	// milliseconds can change their decimal rendering after MySQL JSON storage;
	// normalize before hashing, signing, or persisting the evidence.
	if ts, ok := logData["timestamp"].(float64); ok {
		logData["timestamp"] = int64(ts)
	}

	tx := &types.Transaction{
		TxType:           txType,
		UserID:           userID,
		LogData:          logData,
		Timestamp:        float64(time.Now().UnixNano()) / 1e6,
		CreatorPublicKey: mn.PublicKey,
		Endorsed:         false,
		Endorsements:     []types.Endorsement{}, // initialized as an empty slice; user signatures start empty
	}

	// compute the transaction ID
	tx.TXID = tx.CalculateTXID()

	// the management node signs the transaction
	// sign with the private key from the configuration file
	secretKey, err := crypto.DeserializeSecretKey(mn.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("invalid configured management key: %w", err)
	}
	signature := crypto.Sign(secretKey, []byte(tx.TXID))
	tx.CreatorSignature = crypto.SerializeSignature(signature)

	return tx, nil
}

// ReceiveEndorsedTransaction receives endorsed transactions pushed directly by the endorsement service into the in-memory packaging pool.
// In-memory endorsement mode: signature results bypass database status transitions (transactions.status stays pending,
// the existing async queue marks them blocked after sealing), so pipeline intermediate states incur zero persistence.
func (mn *ManagementNode) ReceiveEndorsedTransaction(tx *types.Transaction) {
	if mn == nil || tx == nil || mn.TxPoolDB == nil {
		return
	}
	mn.TxPoolDB.AddToPackagingPool(tx)
}

// ReceiveEndorsedTransactionsBatch receives a batch of pushed endorsed transactions (single lock, whole batch enqueued)
func (mn *ManagementNode) ReceiveEndorsedTransactionsBatch(txs []*types.Transaction) {
	if mn == nil || len(txs) == 0 || mn.TxPoolDB == nil {
		return
	}
	mn.TxPoolDB.AddToPackagingPoolBatch(txs)
}

// ProcessLogsToTransactions creates transactions from logs
func (mn *ManagementNode) ProcessLogsToTransactions() (int, error) {
	processed := 0

	// process the four log categories
	categories := []types.TransactionType{
		types.TransactionTypeManagement,
		types.TransactionTypeDevelopment,
		types.TransactionTypeTest,
		types.TransactionTypeOperations,
	}

	for _, txType := range categories {
		count, err := mn.processLogsByType(txType)
		if err != nil {
			continue
		}
		processed += count
	}

	return processed, nil
}

func (mn *ManagementNode) processLogsByType(txType types.TransactionType) (int, error) {
	// check the log database is initialized
	if mn.LogDatabase == nil {
		return 0, fmt.Errorf("log database not initialized")
	}

	// fetch unprocessed logs from the log database
	logsInterface, err := mn.LogDatabase.QueryLogs(&database.LogQuery{
		Category:  database.LogCategory(txType.String()),
		Processed: database.BoolPtr(false),
		Limit:     25,
		Offset:    0,
	})
	if err != nil {
		return 0, err
	}

	// convert to []*database.SoftwareFactoryLog
	var logs []*database.SoftwareFactoryLog
	for _, logInterface := range logsInterface {
		if log, ok := logInterface.(*database.SoftwareFactoryLog); ok {
			logs = append(logs, log)
		}
	}

	processed := 0
	for _, logEntry := range logs {
		// create the transaction
		tx, err := mn.CreateTransaction(txType, logEntry.ToTransactionLog())
		if err != nil {
			continue
		}

		// add the transaction to the pool (without endorsement state)
		if err := mn.TxPoolDB.AddTransaction(tx); err != nil {
			continue
		}

		// mark the log as processed
		if err := mn.LogDatabase.MarkAsProcessed(logEntry.ID, tx.TXID); err != nil {
		}

		processed++
	}

	return processed, nil
}

// GenerateBlocks generates blocks per transaction type (event-triggered mode)
func (mn *ManagementNode) GenerateBlocks() {
	// check mn is not nil
	if mn == nil {
		//log.Printf("ManagementNode is nil, exiting GenerateBlocks")
		return
	}

	// start a dedicated block-generation goroutine per transaction type
	for txType := types.TransactionTypeManagement; txType <= types.TransactionTypeOperations; txType++ {
		// create an unbuffered trigger channel
		mn.chainTriggers[txType] = make(chan struct{})

		// trigger the first block immediately after node startup
		go func(t types.TransactionType) {
			mn.chainTriggers[t] <- struct{}{}
		}(txType)

		go mn.generateBlocksForChain(txType)
	}

	// rebroadcast fallback for unfinished blocks: periodically re-broadcast blocks lacking signatures after sporadic HTTP message loss (idempotent)
	go mn.rebroadcastPending()

	// wait for the stop signal
	if mn.StopChan != nil {
		<-mn.StopChan
	}
}

// rebroadcastPending periodically rebroadcasts blocks that have not reached consensus (application-layer retransmission covering sporadic network loss)
func (mn *ManagementNode) rebroadcastPending() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			mn.mu.Lock()
			var rebroadcast []*types.Block
			now := time.Now()
			for hash, block := range mn.blockCache {
				key := fmt.Sprintf("%s:%d", types.TransactionType(block.Header.ChainType).String(), block.Header.BlockHeight)
				if _, done := mn.pendingAggregatedSignatures[key]; done {
					continue
				}
				if len(mn.signatureCache[hash]) >= 4 {
					continue
				}
				if t, ok := mn.broadcastAt[hash]; ok && now.Sub(t) > 1500*time.Millisecond {
					rebroadcast = append(rebroadcast, block)
					mn.broadcastAt[hash] = now
				}
			}
			mn.mu.Unlock()
			for _, block := range rebroadcast {
				log.Printf("[rebroadcast fallback] signatures incomplete, re-broadcasting: chain=%s, height=%d", types.TransactionType(block.Header.ChainType).String(), block.Header.BlockHeight)
				mn.mu.RLock()
				sig := mn.managerSignatureCache[block.Header.CalculateHash()]
				mn.mu.RUnlock()
				mn.broadcastBlock(block, sig)
			}
		case <-mn.StopChan:
			return
		}
	}
}

// generateBlocksForChain generates blocks for a specific chain type (event-triggered mode)
func (mn *ManagementNode) generateBlocksForChain(txType types.TransactionType) {
	trigger := mn.chainTriggers[txType]

	//log.Printf("starting block generation for %s chain (event-triggered mode)", txType.String())

	for {
		select {
		case <-trigger:
			log.Printf("[generateBlocksForChain] trigger received, txType=%s", txType.String())
			for mn.generateBlockWithRetry(txType) {
				// short gap between blocks: buffers DTO processing and signature returns, avoiding message storms
				time.Sleep(80 * time.Millisecond)
			}
		case <-mn.StopChan:
			log.Printf("[generateBlocksForChain] stop signal received, txType=%s", txType.String())
			return
		}
	}
}

func (mn *ManagementNode) generateBlockWithRetry(txType types.TransactionType) bool {
	// record the currently expected block height
	mn.mu.Lock()
	initialHeight := mn.chainNextHeight[txType]
	mn.mu.Unlock()

	log.Printf("[generateBlockWithRetry] start, txType=%s, initialHeight=%d", txType.String(), initialHeight)

	// try to generate a block
	mn.generateBlock(txType)

	// check whether the block height advanced (i.e., generation succeeded)
	mn.mu.Lock()
	currentHeight := mn.chainNextHeight[txType]
	mn.mu.Unlock()

	log.Printf("[generateBlockWithRetry] generateBlock returned, txType=%s, initialHeight=%d, currentHeight=%d",
		txType.String(), initialHeight, currentHeight)

	// if the height is unchanged, generation was skipped (missing aggregate signature or another reason)
	if currentHeight == initialHeight {
		log.Printf("[generateBlockWithRetry] block height unchanged, starting timer to wait, txType=%s, height=%d", txType.String(), currentHeight)
		mn.startTimerForChain(txType)
		return false
	}

	// block generated; check whether to trigger the next round
	txCount, err := mn.TxPoolDB.GetEndorsedTransactionsCount(txType)
	log.Printf("[generateBlockWithRetry] block generated, txType=%s, newHeight=%d, poolCount=%d, err=%v",
		txType.String(), currentHeight, txCount, err)

	// TPS-priority mode: start the timer when transactions are below BlockSize
	if mn.TpsPriority {
		if err == nil && txCount < mn.BlockSize {
			log.Printf("[generateBlockWithRetry] TPS priority, insufficient transactions, starting timer, txType=%s, poolCount=%d, BlockSize=%d",
				txType.String(), txCount, mn.BlockSize)
			mn.startTimerForChain(txType)
			return false
		}
	}

	// non-TPS-priority mode: start the timer when the pool is empty
	if err == nil && txCount == 0 {
		log.Printf("[generateBlockWithRetry] pool empty, starting timer, txType=%s", txType.String())
		mn.startTimerForChain(txType)
		return false
	}

	// pool still has transactions: generate the next block immediately (eliminates the inter-block trigger gap)
	log.Printf("[generateBlockWithRetry] pool still has transactions, continuing block generation, txType=%s, poolCount=%d", txType.String(), txCount)
	return true
}

// startTimerForChain starts the timer for a chain to wait for new transactions
func (mn *ManagementNode) startTimerForChain(txType types.TransactionType) {
	// check whether the timer is already running
	mn.mu.Lock()
	log.Printf("⏰ [startTimerForChain] called, txType=%s, timerRunning=%v", txType.String(), mn.timerRunning[txType])
	if mn.timerRunning[txType] {
		log.Printf(" [startTimerForChain] timer already running, txType=%s, skipping (will retry)", txType.String())
		mn.mu.Unlock()
		// timer already running; retry after a short wait
		// a short sleep yields the CPU, avoiding a busy loop
		time.Sleep(100 * time.Millisecond)
		return
	}
	mn.timerRunning[txType] = true
	mn.mu.Unlock()

	// no transactions to package; start the timer
	log.Printf("[block generation] pool empty/insufficient, starting timer to check for new transactions, txType=%s", txType.String())
	// adjust the timer interval and check logic based on TPS-priority mode
	tickerInterval := 5 * time.Second
	if mn.TpsPriority {
		tickerInterval = 1 * time.Second
	}
	// Configure block pacing with the environment variable:
	// SFCHAIN_PACK_TICK=500ms
	if v := os.Getenv("SFCHAIN_PACK_TICK"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			tickerInterval = d
		} else {
			log.Printf("failed to parse SFCHAIN_PACK_TICK (%s), using default %v", v, tickerInterval)
		}
	}
	log.Printf("[block generation] starting timer to check for new transactions, txType=%s, interval=%v, TpsPriority=%v", txType.String(), tickerInterval, mn.TpsPriority)
	go func(t types.TransactionType) {
		ticker := time.NewTicker(tickerInterval)
		defer func() {
			ticker.Stop()
			mn.mu.Lock()
			mn.timerRunning[t] = false
			mn.mu.Unlock()
			log.Printf("[block generation] timer stopped, txType=%s", t.String())
		}()

		log.Printf("[block generation] timer started, txType=%s", t.String())

		for {
			select {
			case <-ticker.C:
				txCount, err := mn.TxPoolDB.GetEndorsedTransactionsCount(t)
				if err == nil && txCount == 0 && os.Getenv("SFCHAIN_RQ1") != "1" && os.Getenv("SF_NO_POOL_RELOAD") != "1" {
					// when the packaging pool is empty, reload endorsed transactions from the database (the endorsement service is a separate process that only writes to the DB,
					// the in-memory pool is not populated automatically at runtime; the reload must happen before the block threshold check,
					// otherwise TPS-priority mode can never fill BlockSize).
					// in push mode (SF_NO_POOL_RELOAD=1) the DB holds no endorsed snapshot,
					// reloading would only pull back ghost transactions and must be skipped (same guard as generateBlock step 4)
					if reloadErr := mn.TxPoolDB.LoadEndorsedTransactionsToPool(); reloadErr == nil {
						txCount, err = mn.TxPoolDB.GetEndorsedTransactionsCount(t)
					}
				}
				log.Printf("[timer] ticker fired, txType=%s, txCount=%d, BlockSize=%d, TpsPriority=%v", t.String(), txCount, mn.BlockSize, mn.TpsPriority)
				if mn.TpsPriority {
					// TPS-priority mode: check whether there are enough transactions
					if err == nil && txCount >= mn.BlockSize {
						log.Printf("[timer] enough transactions, sending trigger, txType=%s, txCount=%d", t.String(), txCount)
						// non-blocking send prevents the ticker goroutine from blocking on a full channel
						select {
						case mn.chainTriggers[t] <- struct{}{}:
						default:
							log.Printf("[timer] trigger channel full, skipping this trigger, txType=%s", t.String())
						}
					} else if err != nil {
						log.Printf("[timer] failed to get transaction count, txType=%s, err=%v", t.String(), err)
					} else {
						log.Printf("[timer] insufficient transactions, not sending trigger, txType=%s, txCount=%d, BlockSize=%d", t.String(), txCount, mn.BlockSize)
					}
				} else {
					// non-TPS-priority mode: trigger directly
					log.Printf("[timer] non-TPS-priority mode, sending trigger, txType=%s", t.String())
					select {
					case mn.chainTriggers[t] <- struct{}{}:
					default:
						log.Printf("[timer] trigger channel full, skipping this trigger, txType=%s", t.String())
					}
				}
			case <-mn.StopChan:
				return
			}
		}
	}(txType)
}

func (mn *ManagementNode) generateBlock(txType types.TransactionType) {
	log.Printf("[block generation - step 0] starting to generate %s chain block", txType.String())

	if mn == nil {
		log.Printf("[block generation - step 0] ManagementNode is nil, exiting")
		return
	}

	if mn.TxPoolDB == nil {
		log.Printf("[block generation - step 0] TxPoolDB is nil, skipping")
		return
	}

	// get or create the per-chain lock
	mn.mu.Lock()
	if mn.chainMu[txType] == nil {
		mn.chainMu[txType] = &sync.Mutex{}
	}
	chainLock := mn.chainMu[txType]
	mn.mu.Unlock()

	// use the per-chain lock to prevent races between concurrent block generations on the same chain
	chainLock.Lock()
	defer chainLock.Unlock()

	log.Printf("[block generation - step 1] get next block height, txType=%s", txType.String())
	nextHeight := mn.getNextBlockHeight(txType)
	log.Printf("[block generation - step 1] nextHeight=%d, txType=%s", nextHeight, txType.String())

	if nextHeight > 1 {
		log.Printf("[block generation - step 2] not the first block, checking previous block, txType=%s, nextHeight=%d", txType.String(), nextHeight)

		// query the previous block via getBlockByHeight, preferring the in-memory cache
		prevBlock := mn.getBlockByHeight(nextHeight-1, txType)
		log.Printf("[block generation - step 2] prevBlock=%v, txType=%s", prevBlock != nil, txType.String())

		if prevBlock == nil {
			log.Printf("[block generation - step 2] prevBlock is nil, exiting, txType=%s, nextHeight=%d", txType.String(), nextHeight)
			return
		}

		// verify the previous block height is correct
		if prevBlock.Header.BlockHeight != nextHeight-1 {
			log.Printf("[block generation - step 2] prevBlock height mismatch: expected=%d, actual=%d, exiting",
				nextHeight-1, prevBlock.Header.BlockHeight)
			return
		}

		// by design, block n's AggregatedSignature stores all nodes' aggregate signature over block n-1
		// when generating block n, block n-1's aggregate signature must already exist
		// check the cache for block n-1's aggregate signature
		key := fmt.Sprintf("%s:%d", txType.String(), nextHeight-1)
		mn.mu.RLock()
		_, exists := mn.pendingAggregatedSignatures[key]
		mn.mu.RUnlock()
		log.Printf("[block generation - step 3] checking aggregate signature, key=%s, exists=%v, txType=%s", key, exists, txType.String())

		if !exists {
			log.Printf("[block generation - step 3] aggregate signature missing, skipping: key=%s, txType=%s", key, txType.String())
			return
		}
		log.Printf("[block generation - step 3] aggregate signature present, key=%s, txType=%s", key, txType.String())
	} else {
		log.Printf("[block generation - step 2] this is the first block, txType=%s", txType.String())
	}

	// print current transaction pool state

	txCount := mn.BlockSize
	if txCount <= 0 {
		txCount = 10
	}

	log.Printf("[block generation - step 4] fetching transactions from packaging pool, txType=%s, txCount=%d", txType.String(), txCount)
	txs, err := mn.TxPoolDB.GetEndorsedTransactions(txType, txCount)
	log.Printf("[block generation - step 4] transactions fetched, txType=%s, len(txs)=%d, err=%v", txType.String(), len(txs), err)

	// The endorsement service is a separate process that only writes status to the database (the in-process AddToPackagingPool notification path is not wired).
	// When the packaging pool is empty, reload endorsed transactions from the database (reusing the startup load logic) so blocks keep being produced at runtime.
	// In push mode (SF_NO_POOL_RELOAD=1) the endorsement service pushes the in-memory packaging pool directly; this fallback is disabled:
	// the DB snapshot lacks endorsement data, and reloading would cause duplicate packaging and lost endorsements.
	if err == nil && len(txs) == 0 &&
		os.Getenv("SFCHAIN_RQ1") != "1" &&
		os.Getenv("SF_NO_POOL_RELOAD") != "1" {
		if reloadErr := mn.TxPoolDB.LoadEndorsedTransactionsToPool(); reloadErr == nil {
			txs, err = mn.TxPoolDB.GetEndorsedTransactions(txType, txCount)
			log.Printf("[block generation - step 4] transactions fetched after pool reload, txType=%s, len(txs)=%d", txType.String(), len(txs))
		}
	}

	if err != nil {
		log.Printf("[block generation - step 4] failed to fetch endorsed transactions: %v, exiting", err)
		return
	}
	if nextHeight > 1 && len(txs) == 0 {
		log.Printf("[block generation - step 4] transaction pool empty, nextHeight=%d, len(txs)=%d, exiting", nextHeight, len(txs))
		return
	}

	// TPS-priority mode: check whether the transaction count suffices
	log.Printf("[block generation - step 5] TPS check, txType=%s, TpsPriority=%v, nextHeight=%d, len(txs)=%d, BlockSize=%d",
		txType.String(), mn.TpsPriority, nextHeight, len(txs), mn.BlockSize)
	if mn.TpsPriority && nextHeight > 1 && len(txs) < mn.BlockSize {
		log.Printf("[block generation - step 5] TPS-priority mode, insufficient transactions: len(txs)=%d, BlockSize=%d, skipping",
			len(txs), mn.BlockSize)
		return
	}
	log.Printf("[block generation - step 5] TPS check passed")

	transactionSlice := make([]types.Transaction, 0)
	verifiedCount := 0
	log.Printf("[block generation - step 6] starting transaction signature verification, txType=%s, len(txs)=%d", txType.String(), len(txs))
	for i := 0; i < len(txs); i++ {
		tx := txs[i]

		if !mn.verifyTransactionUserSignature(tx) {
			log.Printf("[block generation - step 6] transaction %s failed user signature verification, skipping", SafeSubstring(tx.TXID, 16))
			continue
		}

		transactionSlice = append(transactionSlice, *tx)
		verifiedCount++
	}
	log.Printf("[block generation - step 6] signature verification done, txType=%s, verifiedCount=%d/%d", txType.String(), verifiedCount, len(txs))

	if verifiedCount == 0 && nextHeight > 1 {
		log.Printf("[block generation - step 6] no valid endorsed transactions, skipping, txType=%s", txType.String())
		return
	}

	log.Printf("[block generation - step 6] %d transactions passed user signature verification", verifiedCount)

	// get and verify the prehash
	log.Printf("[block generation - step 7] getting prehash, txType=%s", txType.String())
	prehash := mn.getPreviousBlockHash(txType)
	if prehash == "" {
		log.Printf("[block generation - step 7] prehash is empty, cannot create a new block, txType=%s, nextHeight=%d", txType.String(), nextHeight)
		return
	}
	log.Printf("[block generation - step 7] prehash obtained, txType=%s, prehash=%s", txType.String(), SafeSubstring(prehash, 16))

	log.Printf("[block generation - step 8] creating new block, txType=%s, nextHeight=%d, txCount=%d", txType.String(), nextHeight, len(transactionSlice))
	block := types.NewBlock(
		nextHeight,
		prehash,
		int(txType),
		transactionSlice,
	)

	block.Header.MerkleRoot = block.CalculateMerkleRoot()
	log.Printf("[block generation - step 8] block created successfully, txType=%s, height=%d, merkleRoot=%s",
		txType.String(), block.Header.BlockHeight, SafeSubstring(block.Header.MerkleRoot, 16))

	// by design, block n's AggregatedSignature stores all nodes' aggregate signature over block n-1
	// fetch block n-1's aggregate signature from the cache
	if nextHeight > 1 {
		key := fmt.Sprintf("%s:%d", txType.String(), nextHeight-1)
		mn.mu.RLock()
		aggSig, exists := mn.pendingAggregatedSignatures[key]
		mn.mu.RUnlock()
		if exists {
			block.Header.AggregatedSignature = aggSig
			log.Printf("[block generation - step 8] setting aggregate signature, txType=%s, height=%d, key=%s", txType.String(), nextHeight, key)
		} else {
			log.Printf("[block generation - step 8] aggregate signature for block n-1 missing: height=%d, key=%s, exiting", nextHeight, key)
			return
		}
	} else {
		// block 1's AggregatedSignature is empty
		block.Header.AggregatedSignature = ""
		log.Printf("[block generation - step 8] block 1, AggregatedSignature is empty")
	}

	blockHash := block.Header.CalculateHash()
	log.Printf("[block generation - step 9] block hash computed, txType=%s, height=%d, hash=%s",
		txType.String(), block.Header.BlockHeight, SafeSubstring(blockHash, 16))

	// sign the block header (excluding the aggregate signature)
	log.Printf("[block generation - step 10] management node signing, txType=%s", txType.String())
	managerSignatureHex, err := crypto.SignBlockHeader(mn.PrivateKey, blockHash)
	if err != nil {
		log.Printf("[block generation - step 10] signing failed: %v, exiting", err)
		return
	}

	managerSignature, err := crypto.DeserializeSignature(managerSignatureHex)
	if err != nil {
		log.Printf("[block generation - step 10] signature deserialization failed: %v, exiting", err)
		return
	}
	log.Printf("[block generation - step 10] block signed, txType=%s, hash=%s, signature=%s",
		txType.String(), SafeSubstring(blockHash, 16), SafeSubstring(managerSignatureHex, 32))

	// cache the block and signature information
	blockCache := block
	managerSignatureCache := managerSignatureHex
	signatureCache := make(map[string]*bls.Sign)
	publicKeyCache := make(map[string]*bls.PublicKey)

	// add the management node's own signature to the cache
	signatureCache[mn.NodeID] = managerSignature
	publicKeyCache[mn.NodeID] = mn.PublicKeyObj

	// update chainNextHeight immediately to prevent duplicate-height blocks
	log.Printf("[block generation - step 11] updating chainNextHeight, txType=%s, oldHeight=%d, newHeight=%d",
		txType.String(), nextHeight-1, nextHeight)
	mn.incrementChainHeight(txType)

	// save cache entries immediately (before persistence, so lookups hit)
	log.Printf("[block generation - step 12] saving cache, txType=%s, blockHash=%s", txType.String(), SafeSubstring(blockHash, 16))
	mn.mu.Lock()
	mn.blockCache[blockHash] = blockCache
	mn.managerSignatureCache[blockHash] = managerSignatureCache
	mn.signatureCache[blockHash] = signatureCache
	mn.publicKeyCache[blockHash] = publicKeyCache
	mn.mu.Unlock()

	// enqueue the block persistence task (executed asynchronously)
	if mn.BlockManager != nil {
		log.Printf("[block generation - step 13] enqueued block persistence task, txType=%s, height=%d", txType.String(), block.Header.BlockHeight)
		mn.enqueueBlockPersistTask(block, txType)
	} else {
		log.Printf("[block generation - step 13] BlockManager is nil, skipping persistence, txType=%s", txType.String())
	}

	// mark transactions as blocked (asynchronous batch database update)
	txIDs := make([]string, len(txs))
	now := float64(time.Now().UnixNano()) / 1e6 // millisecond timestamp
	for i, tx := range txs {
		txIDs[i] = tx.TXID
		tx.UpdatedAt = now // update UpdatedAt in the cache
	}
	if len(txIDs) > 0 {
		log.Printf("[block generation - step 14] marking transactions as blocked, txType=%s, txCount=%d", txType.String(), len(txIDs))
		// Ordering is critical: write blocked to the database synchronously first, then remove from the in-memory packaging pool.
		// the packaging pool reloads status='endorsed' transactions from the database only when "empty" - if memory is drained first,
		// (pool momentarily empty), the reload SELECT fetches the same batch back before this UPDATE commits,
		// Reject duplicate sealing of the same transaction ID.
		// while the write is in flight the pool is non-empty, so the reload overwrite guard (overwrite only when empty) does not fire; after the commit the reload reads no
		// endorsed rows, closing the loop with no duplication window.
		if err := mn.TxPoolDB.UpdateTransactionsStatusBatch(txIDs, "blocked", time.Now()); err != nil {
			log.Printf("[block generation - step 14] status write-back failed: %v (sealed transactions removed from pool; async queue will retry)", err)
		}
		// Already sealed: never repackage after a transient SQL deadlock.
		mn.TxPoolDB.MarkTransactionsAsBlocked(txType, txIDs)
		// enqueue the persistence task (asynchronous fallback)
		mn.enqueuePersistTask(txType)
	}

	// Broadcast the block
	log.Printf("[block generation - step 15] broadcasting block, txType=%s, height=%d, hash=%s",
		txType.String(), block.Header.BlockHeight, SafeSubstring(blockHash, 16))
	mn.broadcastBlock(block, managerSignatureHex)
	log.Printf("[block generation - step 15] block broadcast done, txType=%s, height=%d", txType.String(), block.Header.BlockHeight)

	log.Printf("[%s chain block %d generated]", txType.String(), block.Header.BlockHeight)
}

func (mn *ManagementNode) broadcastBlock(block *types.Block, managerSignatureHex string) {
	// record the broadcast time (rebroadcast fallback): the block path actually uses this function (carrying the previous block's aggregate signature)
	mn.mu.Lock()
	mn.broadcastAt[block.Header.CalculateHash()] = time.Now()
	mn.mu.Unlock()
	//log.Printf("[block broadcast] start broadcasting block: hash=%s, height=%d, chainType=%d, txCount=%d",

	if mn.networkManager == nil {
		//log.Printf("[block broadcast] networkManager is nil, cannot send block")
		return
	}

	// check NetworkNodes is non-empty
	if len(mn.NetworkNodes) == 0 {
		//log.Printf("[block broadcast] NetworkNodes is empty, cannot send block")
		return
	}

	//log.Printf("[block broadcast] network node count: %d", len(mn.NetworkNodes))
	for range mn.NetworkNodes {
		// node iteration for logging if needed
	}

	var previousBlockHeight int64
	var previousAggregatedSig string

	if block.Header.BlockHeight > 1 {
		// query the previous block via getBlockByHeight, preferring the in-memory cache
		prevBlock := mn.getBlockByHeight(block.Header.BlockHeight-1, types.TransactionType(block.Header.ChainType))
		if prevBlock != nil && prevBlock.Header.BlockHeight == block.Header.BlockHeight-1 {
			// use the block height as the previous block identifier to avoid hash ambiguity
			previousBlockHeight = int64(prevBlock.Header.BlockHeight)
			// A sealed header is immutable; the aggregate for n lives in n+1.
			previousAggregatedSig = block.Header.AggregatedSignature
		} else {
			//log.Printf("[block broadcast] previous block missing or height mismatch")
		}
	}

	var wg sync.WaitGroup
	var sentCount int64
	var countMu sync.Mutex

	for _, nodeInfo := range mn.NetworkNodes {
		if nodeInfo.NodeID == mn.NodeID {
			//log.Printf("[block broadcast] skipping self node: %s", nodeInfo.NodeID)
			continue
		}

		//log.Printf("[block broadcast] sending to node: id=%s, type=%s, address=%s", nodeInfo.NodeID, nodeInfo.NodeType, nodeInfo.Address)

		wg.Add(1)
		go func(node *types.NodeInfo) {
			defer wg.Done()

			chainType := types.TransactionType(block.Header.ChainType)
			isFullBlock := mn.shouldSendFullBlock(node.NodeType, chainType)

			if err := mn.networkManager.SendBlockWithPreviousSig(block, isFullBlock, node, managerSignatureHex, previousBlockHeight, previousAggregatedSig); err != nil {
				//log.Printf("[block broadcast] failed to send to node %s: %v", node.NodeID, err)
			} else {
				blockType := "block header"
				if isFullBlock {
					blockType = "full block"
				}
				_ = blockType // blockType is used for logging
				//log.Printf("[block broadcast] sent %s to node %s successfully", blockType, node.NodeID)

				countMu.Lock()
				sentCount++
				countMu.Unlock()
			}
		}(nodeInfo)
	}

	wg.Wait()

	//log.Printf("[block broadcast] block broadcast done: hash=%s, sent to %d nodes, total nodes: %d",
}

func (mn *ManagementNode) shouldSendFullBlock(receiverType types.NodeType, chainType types.TransactionType) bool {
	// management-chain blocks send only headers to DTO nodes, not full blocks
	if chainType == types.TransactionTypeManagement {
		return false
	}

	// other chains: the node with the matching identity receives the full block; others receive headers
	return receiverType.String() == chainType.String()
}

// ReceiveSignature receives BLS signatures from other nodes
func (mn *ManagementNode) ReceiveSignature(chainType types.TransactionType, blockHash, nodeID, signatureHex, publicKeyHex string) {
	log.Printf("[signature receive] management node received signature: chainType=%s, blockHash=%s, nodeID=%s", chainType.String(), SafeSubstring(blockHash, 16), nodeID)
	// Sender-supplied public keys and arbitrary identities must never satisfy
	// unanimity. Authenticate against the configured roster, then verify.
	expected, ok := mn.NetworkNodes[nodeID]
	if !ok || expected == nil || nodeID == mn.NodeID || expected.PublicKey != publicKeyHex {
		return
	}

	// deserialize the BLS signature and public key
	signature, err := crypto.DeserializeSignature(signatureHex)
	if err != nil {
		//log.Printf("[signature receive] signature deserialization failed: %v", err)
		return
	}

	publicKey, err := crypto.DeserializePublicKey(publicKeyHex)
	if err != nil {
		//log.Printf("[signature receive] public key deserialization failed: %v", err)
		return
	}
	if !crypto.VerifyAggregatedSignature(publicKey, []byte(blockHash), signature) {
		return
	}

	mn.mu.Lock()
	if block, exists := mn.blockCache[blockHash]; !exists ||
		block.Header.ChainType != int(chainType) {
		mn.mu.Unlock()
		return
	}
	if _, duplicate := mn.signatureCache[blockHash][nodeID]; duplicate {
		mn.mu.Unlock()
		return
	}

	// check the cache exists; if not, the block has not been generated yet or the cache was cleaned
	if _, exists := mn.signatureCache[blockHash]; !exists {
		mn.mu.Unlock()
		//log.Printf("[signature receive] block cache missing, retry after 100ms: blockHash=%s", SafeSubstring(blockHash, 16))

		// retry after waiting
		time.Sleep(100 * time.Millisecond)

		mn.mu.Lock()
		if _, exists := mn.signatureCache[blockHash]; !exists {
			mn.mu.Unlock()
			log.Printf("[signature receive] block cache still missing after retry, dropping signature: blockHash=%s, from=%s", SafeSubstring(blockHash, 16), nodeID)
			return
		}
	}

	// store the BLS signature and public key
	mn.signatureCache[blockHash][nodeID] = signature
	mn.publicKeyCache[blockHash][nodeID] = publicKey
	currentCount := len(mn.signatureCache[blockHash])
	log.Printf("[signature receive] signature cached: blockHash=%s, nodeID=%s, signatureCount=%d", SafeSubstring(blockHash, 16), nodeID, currentCount)

	// check whether signatures from all DTO nodes are collected (including the management node itself, 4 total)
	// DTO nodes: development, test, operations (3) + the management node itself = 4
	required := len(mn.NetworkNodes)
	hasEnough := currentCount >= required

	mn.mu.Unlock()

	//log.Printf("signature collection: current %d, required %d", currentCount, required)

	if hasEnough {
		mn.aggregateAndBroadcastSignatures(blockHash, chainType)
	}
}

// ReceiveBlockConfirmation receives DTO nodes' block-processing confirmation (no-op; a signature is the confirmation)
func (mn *ManagementNode) ReceiveBlockConfirmation(chainType types.TransactionType, blockHash string, nodeID string, height int64) {
}

func (mn *ManagementNode) hasEnoughSignatures(blockHash string) bool {
	mn.mu.RLock()
	defer mn.mu.RUnlock()

	// all DTO node signatures must be collected (including the management node itself, 4 total)
	// DTO nodes: development, test, operations (3) + the management node itself = 4
	required := 4
	current := len(mn.signatureCache[blockHash])
	//log.Printf("signature collection: current %d, required %d", current, required)
	return current >= required
}

func (mn *ManagementNode) aggregateAndBroadcastSignatures(blockHash string, chainType types.TransactionType) {
	mn.mu.RLock()
	signatures := make(map[string]*bls.Sign)
	for id, sig := range mn.signatureCache[blockHash] {
		signatures[id] = sig
	}
	block := mn.blockCache[blockHash]
	mn.mu.RUnlock()
	if block == nil || len(signatures) != len(mn.NetworkNodes) {
		return
	}
	for id := range mn.NetworkNodes {
		if signatures[id] == nil {
			return
		}
	}

	//log.Printf("[aggregate signature] aggregating signatures: hash=%s, signatureCount=%d", SafeSubstring(blockHash, 16), len(signatures))

	// Aggregate the BLS signatures
	aggregatedSig, err := mn.aggregateBLSSignatures(signatures)
	if err != nil {
		//log.Printf("aggregate signature failed: %v", err)
		return
	}

	// serialize the aggregate signature
	aggSigHex := crypto.SerializeSignature(aggregatedSig)

	// by design, block n's aggregate signature is stored in block n+1's AggregatedSignature field
	// cache the aggregate signature for use when generating the next block
	key := fmt.Sprintf("%s:%d", chainType.String(), block.Header.BlockHeight)
	mn.mu.Lock()
	if _, already := mn.pendingAggregatedSignatures[key]; already {
		mn.mu.Unlock()
		return
	}
	mn.chainLastBlockHash[chainType] = blockHash
	mn.pendingAggregatedSignatures[key] = aggSigHex
	mn.mu.Unlock()
	// consensus completion time (all node signatures collected and aggregated), used by performance analysis scripts to correlate per block
	log.Printf("consensus completed, height=%d, chain=%s, sigs=%d, hash=%s, final_ns=%d",
		block.Header.BlockHeight, chainType.String(), len(signatures),
		SafeSubstring(blockHash, 16), time.Now().UnixNano())

	// generateBlock installs this aggregate in n+1 BEFORE hashing/signing n+1.
	// Never rewrite a sealed header (including through a cache lookup).

	// update the cached previous block hash
	// The predecessor digest was published atomically with its aggregate above.

	// note: chainNextHeight is already updated during block persistence; do not update it here
	// note: do not delete blockCache here; cleanupBlockCache manages cache cleanup

	// clean up the signature cache
	mn.mu.Lock()
	delete(mn.signatureCache, blockHash)
	delete(mn.publicKeyCache, blockHash)
	mn.mu.Unlock()

	// trigger generation of the next block
	if trigger, exists := mn.chainTriggers[chainType]; exists {
		select {
		case trigger <- struct{}{}:
		default:
			// receiver busy (continuous block production); drop the signal, the block loop continues on its own
		}
		//log.Printf("[block trigger] triggered %s chain block generation at height %d", chainType.String(), block.Header.BlockHeight+1)
	}
}

func (mn *ManagementNode) aggregateBLSSignatures(signatures map[string]*bls.Sign) (*bls.Sign, error) {
	if len(signatures) == 0 {
		return nil, fmt.Errorf("no signatures to aggregate")
	}

	// collect all signatures
	sigList := make([]*bls.Sign, 0, len(signatures))
	for _, sig := range signatures {
		sigList = append(sigList, sig)
	}

	// Aggregate the signatures
	aggregatedSig := crypto.AggregateSignatures(sigList)
	return aggregatedSig, nil
}

func (mn *ManagementNode) aggregatePublicKeys(publicKeys map[string]*bls.PublicKey) (*bls.PublicKey, error) {
	if len(publicKeys) == 0 {
		return nil, fmt.Errorf("no public keys to aggregate")
	}

	// collect all public keys
	pubKeyList := make([]*bls.PublicKey, 0, len(publicKeys))
	for _, pubKey := range publicKeys {
		pubKeyList = append(pubKeyList, pubKey)
	}

	// aggregate the public keys
	aggregatedPubKey := crypto.AggregatePublicKeys(pubKeyList)
	return aggregatedPubKey, nil
}

// Helper methods

// getBlockByHeight fetches a block from the in-memory cache only
// memory-first design: a cache miss means the entry was cleaned (persisted successfully and no longer needed); the database is never queried
// Headers are immutable after sealing; pending aggregates are installed only
// by generateBlock when constructing the successor.
func (mn *ManagementNode) getBlockByHeight(height int, chainType types.TransactionType) *types.Block {
	mn.mu.RLock()
	defer mn.mu.RUnlock()

	for _, block := range mn.blockCache {
		if block.Header.BlockHeight == height && types.TransactionType(block.Header.ChainType) == chainType {
			return block
		}
	}
	return nil
}

func (mn *ManagementNode) getNextBlockHeight(txType types.TransactionType) int {
	mn.mu.RLock()
	defer mn.mu.RUnlock()

	height, exists := mn.chainNextHeight[txType]
	if !exists {
		return 1
	}
	return height
}

// incrementChainHeight advances the next block height of the given chain
func (mn *ManagementNode) incrementChainHeight(txType types.TransactionType) {
	mn.mu.Lock()
	defer mn.mu.Unlock()

	mn.chainNextHeight[txType]++
}

// loadChainHeightsFromDB loads the latest block height of each chain from the database
func (mn *ManagementNode) loadChainHeightsFromDB() {
	if mn.BlockManager == nil {
		return
	}

	mn.mu.Lock()
	defer mn.mu.Unlock()

	// iterate all chain types
	for chainType := types.TransactionTypeManagement; chainType <= types.TransactionTypeOperations; chainType++ {
		// get the chain's latest block
		latestBlock := mn.BlockManager.GetLastBlock(int(chainType))
		if latestBlock != nil {
			// set the next block height to the latest height + 1
			mn.chainNextHeight[chainType] = latestBlock.Header.BlockHeight + 1
			// cache the previous block hash
			mn.chainLastBlockHash[chainType] = latestBlock.Header.CalculateHash()
		}
	}
}

// updateCachedLastBlockHash updates the cached previous block hash (the hash includes the aggregate signature)
func (mn *ManagementNode) updateCachedLastBlockHash(txType types.TransactionType, blockHash string) {
	mn.mu.Lock()
	defer mn.mu.Unlock()

	mn.chainLastBlockHash[txType] = blockHash
}

func (mn *ManagementNode) getPreviousBlockHash(txType types.TransactionType) string {
	// get the next block height
	nextHeight := mn.getNextBlockHeight(txType)

	// if this is the first block (height 1), return the chain's node public key
	if nextHeight == 1 {
		switch txType {
		case types.TransactionTypeManagement:
			return mn.PublicKey
		case types.TransactionTypeDevelopment:
			// get the development node's public key from the network nodes
			for _, node := range mn.NetworkNodes {
				if node.NodeType == types.NodeTypeDevelopment {
					return node.PublicKey
				}
			}
		case types.TransactionTypeTest:
			// get the test node's public key from the network nodes
			for _, node := range mn.NetworkNodes {
				if node.NodeType == types.NodeTypeTest {
					return node.PublicKey
				}
			}
		case types.TransactionTypeOperations:
			// get the operations node's public key from the network nodes
			for _, node := range mn.NetworkNodes {
				if node.NodeType == types.NodeTypeOperations {
					return node.PublicKey
				}
			}
		}
		// if no matching node is found, return an empty string
		return ""
	}

	// not the first block: prefer the cached previous block hash
	mn.mu.RLock()
	cachedHash, exists := mn.chainLastBlockHash[txType]
	mn.mu.RUnlock()

	if exists && cachedHash != "" {
		return cachedHash
	}

	// cache miss; read from the database
	if mn.BlockManager == nil {
		return ""
	}

	// query the previous block via getBlockByHeight, preferring the in-memory cache
	lastBlock := mn.getBlockByHeight(nextHeight-1, txType)
	if lastBlock == nil {
		return ""
	}

	// verify the previous block height is correct
	if lastBlock.Header.BlockHeight != nextHeight-1 {
		return ""
	}

	// return the previous block hash (including all fields)
	blockHash := lastBlock.Header.CalculateHash()
	return blockHash
}

func (mn *ManagementNode) verifyTransactionUserSignature(tx *types.Transaction) bool {
	if tx == nil {
		return false
	}
	fail := func(reason string) bool {
		if os.Getenv("SFCHAIN_RQ1_DEBUG") == "1" && tx != nil {
			log.Printf("RQ1_VERIFY_FAIL tx=%s reason=%s user=%s endorsements=%d",
				SafeSubstring(tx.TXID, 16), reason, tx.UserID, len(tx.Endorsements))
		}
		return false
	}
	if tx.TXID == "" {
		//log.Printf("[signature verification] transaction TXID is empty")
		return fail("empty_txid")
	}
	if tx.CalculateTXID() != tx.TXID {
		return fail("content_hash")
	}
	if user, ok := tx.LogData["user_id"].(string); ok && user != tx.UserID {
		return fail("log_actor_binding")
	}
	if category, ok := tx.LogData["category"].(string); ok && category != tx.TxType.String() {
		return fail("log_chain_binding")
	}

	if len(tx.Endorsements) != 1 {
		//log.Printf("[signature verification] transaction %s user signature is empty", SafeSubstring(tx.TXID, 16))
		return fail("endorsement_count")
	}

	// prefer the cached user public key
	var publicKeyHex string

	if mn.TxPoolDB != nil {
		publicKeyHex, _ = mn.TxPoolDB.GetUserPublicKey(tx.UserID)
	}

	// not cached; fetch from the database
	if publicKeyHex == "" {
		if mn.UserDatabase == nil {
			//log.Printf("[signature verification] UserDatabase not initialized")
			return fail("no_user_db")
		}

		userIface, err := mn.UserDatabase.GetUser(tx.UserID)
		if err != nil {
			//log.Printf("[signature verification] failed to get user %s: %v", tx.UserID, err)
			return fail("user_lookup")
		}

		user, ok := userIface.(*mysql.User)
		if !ok {
			//log.Printf("[signature verification] malformed user data: %s", tx.UserID)
			return fail("user_type")
		}

		publicKeyHex = user.PublicKey
		if publicKeyHex == "" {
			//log.Printf("[signature verification] user %s public key missing", tx.UserID)
			return fail("empty_registered_key")
		}
	}

	endorsement := tx.Endorsements[0]
	if endorsement.UserID != tx.UserID || endorsement.PublicKey == "" ||
		!strings.EqualFold(endorsement.PublicKey, publicKeyHex) {
		return fail(fmt.Sprintf("identity_binding endorsement_user=%s pub_equal=%v",
			endorsement.UserID, strings.EqualFold(endorsement.PublicKey, publicKeyHex)))
	}

	// RQ1 uses one responsible-user signature over the canonical source-log
	// representation. The non-RQ1 path signs the TXID.
	if os.Getenv("SFCHAIN_RQ1") == "1" {
		if !crypto.ECDSAVerify(publicKeyHex, []byte(rq1CanonicalLog(tx.LogData)), endorsement.Signature) {
			return fail("canonical_signature")
		}
		return true
	}
	if !crypto.ECDSAVerify(publicKeyHex, []byte(tx.TXID), endorsement.Signature) {
		return fail("txid_signature")
	}
	return true
}

// GetNodeID implements ConsensusNodeInterface
func (mn *ManagementNode) GetNodeID() string {
	return mn.NodeID
}

// GetNodeType implements ConsensusNodeInterface
func (mn *ManagementNode) GetNodeType() types.NodeType {
	return types.NodeTypeManagement
}

// BroadcastBlock implements ConsensusNodeInterface
func (mn *ManagementNode) BroadcastBlock(block *types.Block) error {
	blockHash := block.Header.CalculateHash()
	managerSignatureHex := mn.managerSignatureCache[blockHash]
	mn.mu.Lock()
	mn.broadcastAt[blockHash] = time.Now()
	mn.mu.Unlock()

	// broadcast to all nodes via the existing SendBlock method
	for _, nodeInfo := range mn.NetworkNodes {
		if nodeInfo.NodeID == mn.NodeID {
			continue
		}
		isFullBlock := mn.shouldSendFullBlock(nodeInfo.NodeType, types.TransactionType(block.Header.ChainType))
		if err := mn.networkManager.SendBlock(block, isFullBlock, nodeInfo, managerSignatureHex); err != nil {
			//log.Printf("failed to send block to node %s: %v", nodeInfo.NodeID, err)
		}
	}
	return nil
}

// GetRegisteredNodes implements ConsensusNodeInterface
func (mn *ManagementNode) GetRegisteredNodes() map[string]*types.NodeInfo {
	return mn.NetworkNodes
}

// UpdateAggregatedSignature implements ConsensusNodeInterface
func (mn *ManagementNode) UpdateAggregatedSignature(chainType types.TransactionType, blockHeight int64, signature string) {
}

// ProcessOtherChainHeader implements ConsensusNodeInterface
func (mn *ManagementNode) ProcessOtherChainHeader(header *types.BlockHeader, chainType types.TransactionType) {
}

// GetLastBlock implements ConsensusNodeInterface - returns the latest block of the given chain type
func (mn *ManagementNode) GetLastBlock(txType types.TransactionType) *types.Block {
	// simplified implementation: return nil or mock data
	// a real implementation should read from blockchain storage
	return nil
}

// SignBlock implements ConsensusNodeInterface - signs a block
func (mn *ManagementNode) SignBlock(block *types.Block) (string, error) {
	blockHash := block.Header.CalculateHash()
	signatureHex, err := crypto.SignBlockHeader(mn.PrivateKey, blockHash)
	if err != nil {
		return "", err
	}
	return signatureHex, nil
}

// GetNetworkManager returns the network manager
func (mn *ManagementNode) GetNetworkManager() *network.NodeNetwork {
	return mn.networkManager
}

// GetManagerSignature returns the management node signature
func (mn *ManagementNode) GetManagerSignature(blockHash string) string {
	mn.mu.RLock()
	defer mn.mu.RUnlock()
	return mn.managerSignatureCache[blockHash]
}

// AggregateBLSSignatures implements ConsensusNodeInterface - aggregates BLS signatures
func (mn *ManagementNode) AggregateBLSSignatures(signatures map[string]string) (string, error) {
	if len(signatures) == 0 {
		return "", fmt.Errorf("no signatures to aggregate")
	}

	// actual implementation: aggregate signatures using the BLS library
	sigList := make([]*bls.Sign, 0, len(signatures))
	for _, sigHex := range signatures {
		sig, err := crypto.DeserializeSignature(sigHex)
		if err != nil {
			continue
		}
		sigList = append(sigList, sig)
	}

	if len(sigList) == 0 {
		return "", fmt.Errorf("no valid signatures to aggregate")
	}

	// Aggregate the signatures
	aggregatedSig := crypto.AggregateSignatures(sigList)
	aggregatedSigHex := crypto.SerializeSignature(aggregatedSig)
	return aggregatedSigHex, nil
}

// convertConfigNodesToTypesNodes converts config.NodeInfo to types.NodeInfo
func (mn *ManagementNode) convertConfigNodesToTypesNodes(configNodes []config.NodeInfo) []types.NodeInfo {
	var typesNodes []types.NodeInfo
	for _, configNode := range configNodes {
		typesNodes = append(typesNodes, types.NodeInfo{
			NodeID:    configNode.NodeID,
			NodeType:  types.NodeTypeFromString(configNode.NodeType),
			Port:      configNode.Port,
			Address:   configNode.Address,
			PublicKey: configNode.PublicKey,
		})
	}
	return typesNodes
}

// enqueuePersistTask enqueues a persistence task
func (mn *ManagementNode) enqueuePersistTask(txType types.TransactionType) {
	select {
	case mn.persistQueue <- struct{}{}:
		// task enqueued
	default:
		// queue full; warn
	}
}

// persistenceWorker is the asynchronous persistence worker goroutine
// takes tasks from the queue and batch-updates transaction status to the database
func (mn *ManagementNode) persistenceWorker() {
	for {
		select {
		case <-mn.persistQueue:
			mn.persistTask()
		case <-mn.StopChan:
			return
		}
	}
}

// persistTask runs the persistence task: batch-update transaction status to the database
func (mn *ManagementNode) persistTask() {
	// get pending transaction updates from TxPoolDB
	txIDs, updatedAts, err := mn.TxPoolDB.GetPendingStatusUpdates()
	if err != nil || len(txIDs) == 0 {
		return
	}

	// group by each updatedAt and batch-update each group separately
	grouped := make(map[time.Time][]string)
	for _, txID := range txIDs {
		updatedAt := updatedAts[txID]
		txUpdatedAt := time.Unix(0, int64(updatedAt*1e6))
		if txUpdatedAt.IsZero() {
			txUpdatedAt = time.Now()
		}
		grouped[txUpdatedAt] = append(grouped[txUpdatedAt], txID)
	}

	// batch-update each group separately
	for updatedAt, groupTxIDs := range grouped {
		for {
			if err := mn.TxPoolDB.UpdateTransactionsStatusBatch(groupTxIDs, "blocked", updatedAt); err == nil {
				break
			} else {
				log.Printf("[tx pool] status write-back retry: %v", err)
			}
			select {
			case <-mn.StopChan:
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// blockPersistenceWorker is the asynchronous block persistence worker goroutine
// takes tasks from the queue and persists blocks to the database
func (mn *ManagementNode) blockPersistenceWorker() {
	for {
		select {
		case task := <-mn.blockPersistQueue:
			mn.persistBlockTask(task)
		case <-mn.StopChan:
			return
		}
	}
}

// persistBlockTask runs a block persistence task
func (mn *ManagementNode) persistBlockTask(task *blockPersistTask) {
	if task == nil {
		return
	}

	// build the cacheKey for deduplication
	cacheKey := fmt.Sprintf("%s:%d", task.chainType.String(), task.height)

	// check whether already persisted (prevent duplicate persistence)
	mn.blockPersistCacheMu.RLock()
	_, exists := mn.blockPersistCache[cacheKey]
	mn.blockPersistCacheMu.RUnlock()

	if exists {
		return
	}

	// perform persistence
	if mn.BlockManager != nil {
		for {
			if mn.BlockManager.AddBlock(task.block) {
				log.Printf("%s chain block %d persisted", task.chainType.String(), task.height)

				// update the cache
				mn.blockPersistCacheMu.Lock()
				mn.blockPersistCache[cacheKey] = task
				mn.blockPersistCacheMu.Unlock()

				// clean up expired block cache entries (keep the last block for prehash verification)
				mn.cleanupBlockCache(task.chainType, task.height)
				return
			}
			select {
			case <-mn.StopChan:
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// enqueueBlockPersistTask enqueues a block persistence task
func (mn *ManagementNode) enqueueBlockPersistTask(block *types.Block, chainType types.TransactionType) {
	task := &blockPersistTask{
		block:     block,
		chainType: chainType,
		height:    block.Header.BlockHeight,
	}
	// RQ2 control: same protocol and workload, but custody I/O is on the
	// block-processing path. Default remains asynchronous.
	if os.Getenv("SFCHAIN_SYNC_BLOCK_PERSIST") == "1" {
		mn.persistBlockTask(task)
		return
	}

	select {
	case mn.blockPersistQueue <- task:
	case <-mn.StopChan:
		log.Printf("shutdown before custody enqueue: chain=%s height=%d", chainType.String(), block.Header.BlockHeight)
	}
}

// cleanupBlockCache cleans up expired block cache entries
// rules: 1. only clean persisted blocks; 2. keep the last block of each chain
func (mn *ManagementNode) cleanupBlockCache(chainType types.TransactionType, currentHeight int) {
	mn.mu.Lock()
	defer mn.mu.Unlock()

	// minimum height to keep for the chain (at least currentHeight-1, needed by the next block)
	minHeightToKeep := currentHeight - 1
	if minHeightToKeep < 1 {
		minHeightToKeep = 1
	}

	// collect blocks to delete
	var toDelete []string
	for blockHash, block := range mn.blockCache {
		if types.TransactionType(block.Header.ChainType) != chainType {
			continue
		}

		// skip the chain's latest block (used for prehash verification)
		if block.Header.BlockHeight == currentHeight {
			continue
		}

		// skip blocks older than the minimum height
		if block.Header.BlockHeight < minHeightToKeep {
			toDelete = append(toDelete, blockHash)
		}
	}

	// delete expired blocks
	for _, hash := range toDelete {
		delete(mn.blockCache, hash)
	}
}
