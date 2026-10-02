package core

import (
	"fmt"
	"log"
	"os"
	"sfchain/internal/database"
	"sfchain/internal/database/mysql"
	"sfchain/pkg/types"
	"strconv"
	"time"
)

// LogProcessor is the log processor - fetches logs directly from the log database built by log_service and converts them into transactions
type LogProcessor struct {
	db           database.LogDatabaseInterface // database connected to the log_service log database
	node         *ManagementNode
	stopChan     chan struct{}
	processTypes []types.TransactionType
	txThreshold  int
	interval     time.Duration
	pacing       time.Duration // gap between the creation of two consecutive transactions
	rq1          bool
	rq1RunID     string
	rq1Total     int
}

// NewLogProcessor creates a log processor that connects directly to the log database built by log_service
//
// Polling and pacing can be configured with environment variables:
//
//	SFCHAIN_LOG_POLL_INTERVAL  log poll interval   default 5s
//	SFCHAIN_LOG_THRESHOLD      rows per category per round   default 10
//	SFCHAIN_LOG_PACING         gap between transactions   default 100ms (0 disables)
func NewLogProcessor(db database.LogDatabaseInterface, node *ManagementNode) *LogProcessor {
	interval := 5 * time.Second
	if v := os.Getenv("SFCHAIN_LOG_POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			interval = d
		} else {
			log.Printf("failed to parse SFCHAIN_LOG_POLL_INTERVAL (%s), using default 5s", v)
		}
	}
	threshold := 10
	if v, err := strconv.Atoi(os.Getenv("SFCHAIN_LOG_THRESHOLD")); err == nil && v > 0 {
		threshold = v
	}
	pacing := 100 * time.Millisecond
	if v := os.Getenv("SFCHAIN_LOG_PACING"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			pacing = d
		}
	}
	rq1 := os.Getenv("SFCHAIN_RQ1") == "1"
	rq1RunID := os.Getenv("SFCHAIN_RQ1_RUN_ID")
	if rq1RunID == "" {
		rq1RunID = "rq1"
	}
	rq1Total := 20000
	if v, err := strconv.Atoi(os.Getenv("SFCHAIN_RQ1_TOTAL")); err == nil && v > 0 {
		rq1Total = v
	}
	return &LogProcessor{
		db:       db, // this database instance should be the same one used by log_service
		node:     node,
		stopChan: make(chan struct{}),
		processTypes: []types.TransactionType{
			types.TransactionTypeManagement,
			types.TransactionTypeDevelopment,
			types.TransactionTypeTest,
			types.TransactionTypeOperations,
		},
		txThreshold: threshold, // process at most 10 logs per round
		interval:    interval,  // check the database for new logs every 5 seconds
		pacing:      pacing,
		rq1:         rq1,
		rq1RunID:    rq1RunID,
		rq1Total:    rq1Total,
	}
}

// Start starts the log processor
func (lp *LogProcessor) Start() {
	log.Println("starting log processor...")
	if lp.rq1 {
		go func() {
			lp.processRQ1Once()
			// The RQ1 driver adds one successor trigger per role after the
			// initial snapshot drains. Continue polling to consume those rows.
			lp.processingLoop()
		}()
		return
	}
	go lp.processingLoop()
}

// processRQ1Once is the current comparison ingestion path. It takes one
// deterministic source-log snapshot and then uses the normal SFChain
// endorsement and block pipeline.
func (lp *LogProcessor) processRQ1Once() {
	if lp == nil || lp.db == nil || lp.node == nil {
		log.Printf("RQ1 skipped: log processor is not initialized")
		return
	}

	log.Printf("RQ1_START run_id=%s expected=%d", lp.rq1RunID, lp.rq1Total)
	rawLogs, err := lp.db.GetPendingLogsOrdered(lp.rq1Total)
	if err != nil {
		log.Printf("RQ1_READ_ERROR run_id=%s error=%v", lp.rq1RunID, err)
		return
	}
	if len(rawLogs) != lp.rq1Total {
		log.Printf("RQ1_READ_COUNT run_id=%s expected=%d actual=%d",
			lp.rq1RunID, lp.rq1Total, len(rawLogs))
	}

	txs := make([]interface{}, 0, len(rawLogs))
	refs := make([][2]string, 0, len(rawLogs))
	for i, raw := range rawLogs {
		sfLog, ok := raw.(*mysql.SoftwareFactoryLog)
		if !ok {
			log.Printf("RQ1_SKIP seq=%d unsupported_log_type=%T", i+1, raw)
			continue
		}
		// Keep the three RQ1 workloads on the same ten source fields. The
		// database stores fractional milliseconds, while the FISCO contract
		// uses uint64 milliseconds; normalize that field identically here.
		logData := map[string]interface{}{
			"id":        sfLog.ID,
			"category":  string(sfLog.Category),
			"timestamp": float64(uint64(sfLog.Timestamp)),
			"level":     sfLog.Level,
			"message":   sfLog.Message,
			"user_id":   sfLog.UserID,
			"module":    sfLog.Module,
			"project":   sfLog.Project,
			"operation": sfLog.Operation,
			"status":    sfLog.Status,
		}
		txType := types.TransactionTypeFromString(string(sfLog.Category))
		tx, err := lp.node.CreateTransaction(txType, logData)
		if err != nil {
			log.Printf("RQ1_CREATE_ERROR seq=%d error=%v", i+1, err)
			continue
		}
		tx.RQ1RunID = lp.rq1RunID
		txs = append(txs, tx)
		refs = append(refs, [2]string{sfLog.ID, tx.TXID})
	}

	if len(txs) == 0 {
		log.Printf("RQ1_EMPTY run_id=%s", lp.rq1RunID)
		return
	}
	if err := lp.node.TxPoolDB.AddTransactionsBatch(txs); err != nil {
		log.Printf("RQ1_POOL_ERROR run_id=%s count=%d error=%v",
			lp.rq1RunID, len(txs), err)
		return
	}
	if err := lp.db.MarkAsProcessedBatch(refs); err != nil {
		log.Printf("RQ1_SOURCE_UPDATE_ERROR run_id=%s error=%v", lp.rq1RunID, err)
		return
	}
	log.Printf("RQ1_READ_DONE run_id=%s count=%d", lp.rq1RunID, len(txs))
}

// Stop stops the log processor
func (lp *LogProcessor) Stop() {
	close(lp.stopChan)
	log.Println("log processor stopped")
}

// processingLoop is the processing loop
func (lp *LogProcessor) processingLoop() {
	// check lp is not nil
	if lp == nil {
		log.Printf("LogProcessor is nil, exiting processing loop")
		return
	}

	// check stopChan is not nil
	if lp.stopChan == nil {
		log.Printf("LogProcessor stopChan is nil, exiting processing loop")
		return
	}

	// set defaults
	interval := lp.interval
	if interval == 0 {
		interval = 5 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("LogProcessor processing loop started with interval: %v", interval)

	for {
		select {
		case <-ticker.C:
			log.Printf("LogProcessor ticker fired, calling processPendingLogs")
			lp.processPendingLogs()
		case <-lp.stopChan:
			log.Printf("LogProcessor stopChan received, exiting processing loop")
			return
		}
	}
}

// processPendingLogs processes pending logs - fetches unprocessed logs directly from the log_service log database
func (lp *LogProcessor) processPendingLogs() {
	// check lp is not nil
	if lp == nil {
		log.Printf("LogProcessor is nil, exiting processPendingLogs")
		return
	}

	// check lp.db is not nil
	if lp.db == nil {
		log.Printf("LogProcessor db is nil, skipping processPendingLogs")
		return
	}

	// check lp.processTypes is non-nil and non-empty
	if lp.processTypes == nil || len(lp.processTypes) == 0 {
		log.Printf("LogProcessor processTypes is nil or empty, skipping processPendingLogs")
		return
	}

	for _, txType := range lp.processTypes {
		// print database path information for debugging
		// log.Printf("log processor is accessing database path: %s", lp.db.GetDBPath())

		// get database statistics for debugging
		stats, err := lp.db.GetStats()
		if err != nil {
			// log.Printf("failed to get database statistics: %v", err)
		} else {
			if statsObj, ok := stats.(map[string]interface{}); ok {
				// totalCount := statsObj["TotalCount"]
				// pendingCount := statsObj["PendingCount"]
				// processedCount := statsObj["ProcessedCount"]
				// log.Printf("database stats - total logs: %v, pending: %v, processed: %v",
				// 	totalCount, pendingCount, processedCount)

				if categoryStats, ok := statsObj["CategoryStats"].(map[string]interface{}); ok {
					for _, _ = range categoryStats {
						// log.Printf("  category %s: %v", category, count)
					}
				}
			} else if statsObj, ok := stats.(*database.LogStats); ok {
				// log.Printf("database stats - total logs: %d, pending: %d, processed: %d",
				// 	statsObj.TotalCount, statsObj.PendingCount, statsObj.ProcessedCount)

				if statsObj.CategoryStats != nil {
					for _, _ = range statsObj.CategoryStats {
						// log.Printf("  category %s: %d", category, count)
					}
				}
			} else if statsObj, ok := stats.(*mysql.LogStats); ok {
				// log.Printf("database stats - total logs: %d, pending: %d, processed: %d",
				// 	statsObj.TotalCount, statsObj.PendingCount, statsObj.ProcessedCount)

				if statsObj.CategoryStats != nil {
					for _, _ = range statsObj.CategoryStats {
						// log.Printf("  category %s: %d", category, count)
					}
				}
			} else {
				log.Printf("malformed database statistics: %v", stats)
			}
		}

		// add counts of unprocessed logs for specific types
		// var category database.LogCategory
		// switch txType {
		// case types.TransactionTypeManagement:
		// 	category = database.CategoryManagement
		// case types.TransactionTypeDevelopment:
		// 	category = database.CategoryDevelopment
		// case types.TransactionTypeTest:
		// 	category = database.CategoryTest
		// case types.TransactionTypeOperations:
		// 	category = database.CategoryOperations
		// }

		// extra debugging: inspect all logs of a category and their Processed state
		// allLogsInCategory, err := lp.getAllLogsForCategory(category)
		// if err != nil {
		// 	// log.Printf("failed to get all logs of type %s: %v", txType.String(), err)
		// } else {
		// 	// log.Printf("  all logs of type %s - unprocessed: %d, processed: %d", txType.String(),
		// 	// 	lp.countUnprocessedLogs(allLogsInCategory), lp.countProcessedLogs(allLogsInCategory))
		// }

		// fetch unprocessed logs directly from the log_service log database
		logs, err := lp.db.BuildTransactionLogs(txType, lp.txThreshold)
		if err != nil {
			// log.Printf("failed to fetch %s transaction logs from the log_service log database: %v", txType.String(), err)
			continue
		}

		if len(logs) == 0 {
			// log.Printf("no pending %s logs in the log_service log database", txType.String())
			continue
		}

		// build transactions in batch (per-tx BLS signing; construction is fast)
		txs := make([]interface{}, 0, len(logs))
		refs := make([][2]string, 0, len(logs))
		for _, logData := range logs {
			tx, err := lp.node.CreateTransaction(txType, logData)
			if err != nil {
				// log.Printf("failed to create transaction from log: %v", err)
				continue
			}
			txs = append(txs, tx)
			if id, ok := logData["id"].(string); ok && id != "" {
				refs = append(refs, [2]string{id, tx.TXID})
			}

			// small delay to avoid concurrency issues (default 100ms, overridable via SFCHAIN_LOG_PACING, 0 disables)
			if lp.pacing > 0 {
				time.Sleep(lp.pacing)
			}
		}
		if len(txs) == 0 {
			continue
		}

		// write transactions to the pool in batch (single multi-row INSERT)
		if err := lp.node.TxPoolDB.AddTransactionsBatch(txs); err != nil {
			log.Printf("failed to batch-add to transaction pool (%d txs): %v", len(txs), err)
			continue
		}

		// mark logs as processed in batch (single UPDATE ... CASE)
		if err := lp.db.MarkAsProcessedBatch(refs); err != nil {
			log.Printf("failed to batch-mark log processed state (%d rows): %v", len(refs), err)
		}
		log.Printf("[batch collection] %s: %d logs converted to transactions", txType.String(), len(txs))
	}
}

// getAllLogsForCategory fetches all logs of the given category
func (lp *LogProcessor) getAllLogsForCategory(category database.LogCategory) ([]*database.SoftwareFactoryLog, error) {
	logsInterface, err := lp.db.QueryLogs(&database.LogQuery{
		Category: category, // no Processed filter; fetch all
	})
	if err != nil {
		return nil, err
	}

	// convert to []*database.SoftwareFactoryLog
	var logs []*database.SoftwareFactoryLog
	for _, logInterface := range logsInterface {
		if log, ok := logInterface.(*database.SoftwareFactoryLog); ok {
			logs = append(logs, log)
		}
	}

	return logs, nil
}

// countUnprocessedLogs counts unprocessed logs
func (lp *LogProcessor) countUnprocessedLogs(logs []*database.SoftwareFactoryLog) int {
	count := 0
	for _, log := range logs {
		if !log.Processed {
			count++
		}
	}
	return count
}

// countProcessedLogs counts processed logs
func (lp *LogProcessor) countProcessedLogs(logs []*database.SoftwareFactoryLog) int {
	count := 0
	for _, log := range logs {
		if log.Processed {
			count++
		}
	}
	return count
}

// getUnprocessedCountForCategory returns the unprocessed log count for a category

// createTransactionFromLog creates a transaction from a log
func (lp *LogProcessor) createTransactionFromLog(txType types.TransactionType, logData map[string]interface{}) error {

	// 1. create the transaction
	tx, err := lp.node.CreateTransaction(txType, logData)
	if err != nil {
		return err
	}

	// 2. extract the log ID
	logID := ""
	if id, ok := logData["id"].(string); ok {
		logID = id
	}

	// 3. put the unendorsed transaction into the transaction pool database
	if err := lp.node.TxPoolDB.AddTransaction(tx); err != nil {
		return fmt.Errorf("failed to add to transaction pool: %v", err)
	}

	// 4. mark the log as processed
	if logID != "" {
		if err := lp.db.MarkAsProcessed(logID, tx.TXID); err != nil {
			// log.Printf("failed to mark log as processed: %v", err)
		}
	}

	// log.Printf("transaction added to pool: %s (type: %s)", tx.TXID[:16], txType.String())
	return nil
}

// SetTxThreshold sets the transaction threshold
func (lp *LogProcessor) SetTxThreshold(threshold int) {
	lp.txThreshold = threshold
}

// SetInterval sets the processing interval
func (lp *LogProcessor) SetInterval(interval time.Duration) {
	lp.interval = interval
}

// SetProcessTypes sets the transaction types to process
func (lp *LogProcessor) SetProcessTypes(types []types.TransactionType) {
	lp.processTypes = types
}
