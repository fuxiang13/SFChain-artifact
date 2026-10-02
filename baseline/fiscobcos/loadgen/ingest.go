package main

// ingest consumes staged factory logs, constructs responsible-user evidence,
// and submits signed transactions to the checked FISCO contract.
// RQ1 latency is receipt_ms - submitted_ms. Evidence preparation precedes
// submitted_ms; throughput uses the first submission and last receipt.

import (
	"context"
	"crypto/ecdsa"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FISCO-BCOS/go-sdk/v3/abi"
	"github.com/FISCO-BCOS/go-sdk/v3/client"
	"github.com/FISCO-BCOS/go-sdk/v3/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	_ "github.com/go-sql-driver/mysql"
)

var ingestCategories = []string{"management", "development", "test", "operations"}

type ingestConfig struct {
	poll            time.Duration // ① polling interval (SFChain LogProcessor interval)
	threshold       int           // ① max rows consumed per category per round (txThreshold)
	pacing          time.Duration // ① interval between consecutive builds (the 100ms spacing per tx)
	endorseInterval time.Duration // ② endorsement-stage interval (endorsement_service -interval); 0 = no endorsement stage
	endorsePersist  bool          // ② persist signatures (fisco.endorsed_txs); the submit stage reads them back -- aligns with SFChain endorsement persistence
	endorseEvidence bool          // ① build evidence isomorphic to SFChain Endorsement (reads sfchain.users, ECDSA-signs the log content hash) and stores it on-chain with the tx
	endorseCheck    bool          // ① build endorsement evidence and call recordLogChecked (contract enforces ecrecover verification; semantic-level admission check)
	idle            time.Duration // how long to wait before exiting when nothing is pending/in flight/queued
	duration        time.Duration // hard run duration cap, 0 means unlimited
	outDir          string

	mysqlHost     string
	mysqlPort     int
	mysqlUser     string
	mysqlPassword string
	mysqlDB       string
}

type ingestRow struct {
	LogID       string
	Category    string
	LogTsMs     float64
	ClaimedMs   int64
	SubmittedMs int64
	ReceiptMs   int64
	LatencyMs   float64 // RQ1 post-admission latency: receipt - submitted
	ConsensusMs float64 // consensus segment: receipt - submitted
	Status      int
	BlockNum    int
	ErrMsg      string
	Recovered   bool
}

type ingestFlight struct {
	row    ingestRow
	txHash []byte
}

// preparedLog log built in stage ① awaiting stage ② endorsement submission
// (mirrors the SFChain two-stage pipeline: ① enqueue tx (unsigned) ->
// ② endorsement service signs concurrently)
//   - queue/persist path: rec carries the raw log fields; evidence building and
//     SDK signing both happen in stage ② (matching the SFChain
//     endorsement_service responsibilities and -max-workers concurrency)
//   - immediate-submit path (interval=0 and not persist): txData/txHash built
//     in stage ①, signed and submitted right away
type preparedLog struct {
	row     ingestRow
	rec     LogRecord
	txData  []byte
	txHash  []byte
	txBytes []byte // fully signed transaction
}

func runIngest(ctx context.Context, cl *client.Client, parsed abi.ABI, addr common.Address, cfg ingestConfig) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Local&charset=utf8mb4",
		cfg.mysqlUser, cfg.mysqlPassword, cfg.mysqlHost, cfg.mysqlPort, cfg.mysqlDB)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to connect to MySQL: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(16)
	if err := db.Ping(); err != nil {
		log.Fatalf("MySQL unreachable (%s:%d/%s): %v", cfg.mysqlHost, cfg.mysqlPort, cfg.mysqlDB, err)
	}
	// table existence check
	var probe int
	if err := db.QueryRow("SELECT 1 FROM software_factory_logs LIMIT 1").Scan(&probe); err != nil {
		if err == sql.ErrNoRows {
			log.Println("note: software_factory_logs table is empty (waiting for loadgen injection)")
		} else {
			log.Fatalf("software_factory_logs table unavailable: %v", err)
		}
	}

	// persist mode: endorsement persistence table (mirrors SFChain transactions
	// state flow: ① INSERT log_json (pending, unendorsed) -> ② endorsement
	// stage signs then UPDATE tx_hex (endorsed) -> send)
	if cfg.endorsePersist {
		if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS fisco"); err != nil {
			log.Fatalf("failed to create database: %v", err)
		}
		if _, err := db.Exec("DROP TABLE IF EXISTS fisco.endorsed_txs"); err != nil {
			log.Fatalf("failed to drop table: %v", err)
		}
		if _, err := db.Exec(`CREATE TABLE fisco.endorsed_txs (
			log_id VARCHAR(64) PRIMARY KEY, log_ts_ms DOUBLE, claimed_ms BIGINT,
			log_json TEXT NOT NULL, tx_hex TEXT, tx_hash VARCHAR(64), sent TINYINT DEFAULT 0)`); err != nil {
			log.Fatalf("failed to create table: %v", err)
		}
	}

	// evidence mode: load the user key set shared with SFChain
	if cfg.endorseEvidence || cfg.endorseCheck {
		if err := loadEndorseUsers(db); err != nil {
			log.Fatalf("failed to load endorsement users: %v", err)
		}
	}

	fmt.Printf("start ingest: poll=%s threshold=%d pacing=%s endorse=%s contract=%s\n",
		cfg.poll, cfg.threshold, cfg.pacing, cfg.endorseInterval, addr.Hex())
	fmt.Printf("MySQL: %s:%d/%s\n", cfg.mysqlHost, cfg.mysqlPort, cfg.mysqlDB)

	// periodic blockLimit refresh (follows chain height with a +500 margin)
	var blockLimit atomic.Int64
	refresh := func() {
		if h, err := cl.GetBlockNumber(ctx); err == nil {
			blockLimit.Store(h + 500)
		}
	}
	refresh()
	stopAll := make(chan struct{})
	defer close(stopAll)
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				refresh()
			case <-stopAll:
				return
			}
		}
	}()

	// results, in-flight map, and endorsement queue
	var mu sync.Mutex
	rows := make([]ingestRow, 0, 1024)
	flight := make(map[string]ingestFlight) // logID -> in flight (submitted, awaiting receipt)
	endorseQueue := make([]preparedLog, 0)  // built, awaiting endorsement submission
	var inflight atomic.Int64
	var queued atomic.Int64
	var totalConsumed atomic.Int64
	var lastActivityNs atomic.Int64
	lastActivityNs.Store(time.Now().UnixNano())

	record := func(r ingestRow) {
		mu.Lock()
		if _, exists := flight[r.LogID]; !exists {
			mu.Unlock()
			return // a recovery query and a late callback must not double count
		}
		rows = append(rows, r)
		delete(flight, r.LogID)
		mu.Unlock()
		inflight.Add(-1)
		lastActivityNs.Store(time.Now().UnixNano())
	}

	// ② endorsement submission: sign + submit + receipt callback (the queued
	// counter is managed by the caller)
	var sendTx func(p preparedLog) // declared first for forward reference in submit
	submit := func(p preparedLog) {
		if len(p.txBytes) > 0 {
			// persist mode: already signed and stored in ①; send directly here
			sendTx(p)
			return
		}
		sig, err := cl.CreateEncodedSignature(p.txHash)
		if err != nil {
			log.Printf("signing failed log=%s: %v (returned to pending)", p.row.LogID, err)
			unclaimLog(db, p.row.LogID)
			totalConsumed.Add(-1)
			return
		}
		tx, err := cl.CreateEncodedTransaction(p.txData, p.txHash, sig, 0, "")
		if err != nil {
			log.Printf("failed to assemble transaction log=%s: %v (returned to pending)", p.row.LogID, err)
			unclaimLog(db, p.row.LogID)
			totalConsumed.Add(-1)
			return
		}
		p.txBytes = tx
		sendTx(p)
	}

	// sendTx sends one (signed) transaction and registers the receipt callback
	sendTx = func(p preparedLog) {
		tx := p.txBytes
		nowMs := time.Now().UnixMilli()
		row := p.row
		row.SubmittedMs = nowMs

		mu.Lock()
		flight[row.LogID] = ingestFlight{row: row, txHash: p.txHash}
		mu.Unlock()
		inflight.Add(1)

		lg := row // row snapshot used inside the closure
		err = cl.AsyncSendEncodedTransaction(ctx, tx, false, func(r *types.Receipt, e error) {
			nowMs := time.Now().UnixMilli()
			mu.Lock()
			f, ok := flight[lg.LogID]
			row := f.row
			mu.Unlock()
			if !ok {
				row = lg
			}
			row.ReceiptMs = nowMs
			row.LatencyMs = float64(nowMs) - float64(row.SubmittedMs)
			row.ConsensusMs = float64(nowMs) - float64(row.SubmittedMs)
			if e != nil {
				row.ErrMsg = "receipt: " + e.Error()
			} else if r != nil {
				row.Status = r.Status
				row.BlockNum = r.BlockNumber
				if r.Status != 0 {
					row.ErrMsg = statusMessage(r.Status)
				}
			} else {
				row.ErrMsg = "nil receipt"
			}
			record(row)
		})
		if err != nil {
			// submit failed: unclaim so a later round can retry
			unclaimLog(db, row.LogID)
			mu.Lock()
			delete(flight, row.LogID)
			mu.Unlock()
			inflight.Add(-1)
			totalConsumed.Add(-1)
			log.Printf("submit failed log=%s: %v (unclaimed)", row.LogID, err)
		}
	}

	// ② endorsement stage (mirrors SFChain endorsement_service: batch processing
	// on a timer, 4 concurrent signing workers)
	if cfg.endorseInterval > 0 {
		go func() {
			t := time.NewTicker(cfg.endorseInterval)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					if cfg.endorsePersist {
						// persist mode, three steps (mirrors SFChain persisted endorsements):
						//   a) read unendorsed rows (tx_hex IS NULL) -> 4 workers build
						//      evidence + sign concurrently
						//   b) batch UPDATE tx_hex to persist (= SFChain
						//      UpdateTransactionsEndorsementsBatch)
						//   c) read back all ready-to-send (sent=0 and signed) -> mark
						//      sent -> submit
						rowsOut, err := db.Query("SELECT log_id, log_ts_ms, claimed_ms, log_json FROM fisco.endorsed_txs WHERE tx_hex IS NULL")
						if err != nil {
							log.Printf("endorsement stage: DB read failed: %v", err)
							continue
						}
						var batch []preparedLog
						for rowsOut.Next() {
							var logID, logJSON string
							var logTsMs float64
							var claimedMs int64
							if err := rowsOut.Scan(&logID, &logTsMs, &claimedMs, &logJSON); err == nil {
								var rec LogRecord
								if json.Unmarshal([]byte(logJSON), &rec) == nil {
									batch = append(batch, preparedLog{row: ingestRow{
										LogID: logID, Category: rec.Category, LogTsMs: logTsMs, ClaimedMs: claimedMs,
										SubmittedMs: -1, ReceiptMs: -1, LatencyMs: -1, ConsensusMs: -1,
									}, rec: rec})
								}
							}
						}
						rowsOut.Close()
						signed := endorseBatch(ctx, cl, parsed, addr, cfg, blockLimit.Load(), batch)
						for _, p := range signed.failed {
							log.Printf("endorsement failed log=%s: %v (retry next round)", p.row.LogID, p.row.ErrMsg)
						}
						if len(signed.ok) > 0 {
							batchUpdateTxHex(db, signed.ok)
							log.Printf("endorsement stage: signed and persisted %d rows", len(signed.ok))
						}
						// c) read back ready-to-send (freshly signed plus leftovers),
						// mark sent=1, then submit
						ready, err := db.Query("SELECT log_id, log_ts_ms, claimed_ms, tx_hex, tx_hash FROM fisco.endorsed_txs WHERE sent = 0 AND tx_hex IS NOT NULL")
						if err != nil {
							log.Printf("endorsement stage: ready-to-send read failed: %v", err)
							continue
						}
						type pendTx struct {
							p    preparedLog
							hexs string
						}
						var toSend []pendTx
						for ready.Next() {
							var logID, txHex, txHash string
							var logTsMs float64
							var claimedMs int64
							if err := ready.Scan(&logID, &logTsMs, &claimedMs, &txHex, &txHash); err == nil {
								hashBytes, decodeErr := hex.DecodeString(txHash)
								if decodeErr != nil || len(hashBytes) != 32 {
									log.Fatalf("invalid persisted tx hash for %s", logID)
								}
								toSend = append(toSend, pendTx{p: preparedLog{row: ingestRow{
									LogID: logID, LogTsMs: logTsMs, ClaimedMs: claimedMs,
									SubmittedMs: -1, ReceiptMs: -1, LatencyMs: -1, ConsensusMs: -1,
								}, txHash: hashBytes}, hexs: txHex})
							}
						}
						ready.Close()
						if len(toSend) == 0 {
							continue
						}
						ids := make([]string, 0, len(toSend))
						for _, pt := range toSend {
							ids = append(ids, pt.p.row.LogID)
						}
						markSentBatch(db, ids)
						for _, pt := range toSend {
							raw, err := hex.DecodeString(pt.hexs)
							if err != nil {
								log.Printf("endorsement stage: decode failed log=%s: %v", pt.p.row.LogID, err)
								continue
							}
							pt.p.txBytes = raw
							sendTx(pt.p)
						}
						if len(toSend) > 0 {
							log.Printf("endorsement stage (persisted): read back and submitted %d rows", len(toSend))
						}
						continue
					}
					// in-memory queue mode (mirrors SFChain direct push): 4 workers
					// build evidence + sign concurrently, then submit immediately
					mu.Lock()
					batch := endorseQueue
					endorseQueue = make([]preparedLog, 0)
					mu.Unlock()
					if len(batch) == 0 {
						continue
					}
					signed := endorseBatch(ctx, cl, parsed, addr, cfg, blockLimit.Load(), batch)
					for _, p := range signed.ok {
						sendTx(p)
						queued.Add(-1)
					}
					for _, p := range signed.failed {
						log.Printf("endorsement failed log=%s: %v (returned to pending)", p.row.LogID, p.row.ErrMsg)
						unclaimLog(db, p.row.LogID)
						totalConsumed.Add(-1)
						queued.Add(-1)
					}
					log.Printf("endorsement stage: submitted %d rows", len(signed.ok))
				case <-stopAll:
					return
				}
			}
		}()
	}

	// ① poll and build (mirrors LogProcessor: categories in order, at most
	// threshold rows each)
	start := time.Now()
	lastConsumed := int64(0)
	stop := false
	for !stop {
		tickStart := time.Now()

		for _, category := range ingestCategories {
			category := category // Go1.21 closure capture
			logs, err := queryPendingLogs(db, category, cfg.threshold)
			if err != nil {
				log.Printf("failed to query pending logs: %v", err)
				continue
			}
			if len(logs) == 0 {
				continue
			}

			// build the batch first (mirrors SFChain LogProcessor: build and
			// enqueue txs unsigned)
			type built struct {
				lg     pendingLog
				rec    LogRecord
				txData []byte
				txHash []byte
			}
			immediate := cfg.endorseInterval == 0 && !cfg.endorsePersist
			builtLogs := make([]built, 0, len(logs))
			txIDs := make(map[string]string, len(logs))
			for _, lg := range logs {
				rec := LogRecord{
					Id:        lg.id,
					Category:  category,
					Timestamp: uint64(lg.timestamp),
					Level:     lg.level,
					Message:   lg.message,
					UserId:    lg.userId,
					Module:    lg.module,
					Project:   lg.project,
					Operation: lg.operation,
					Status:    lg.status,
				}
				var txData, txHash []byte
				if immediate {
					// immediate-submit path (no endorsement stage): evidence
					// building and Pack both complete in stage ①
					var input []byte
					var err error
					if cfg.endorseCheck {
						ev, signer, sig, err2 := buildCheckedEndorsement(rec)
						if err2 != nil {
							log.Printf("failed to build endorsement evidence log=%s: %v", lg.id, err2)
							continue
						}
						rec.Endorsement = ev
						messageHash := rq1EndorsementHash(rec)
						input, err = parsed.Pack("recordLogChecked", rec, messageHash, []common.Address{signer}, [][]byte{sig})
					} else {
						if cfg.endorseEvidence {
							ev, err2 := buildEndorsement(rec)
							if err2 != nil {
								log.Printf("failed to build endorsement evidence log=%s: %v", lg.id, err2)
								continue
							}
							rec.Endorsement = ev
						}
						input, err = parsed.Pack("recordLog", rec)
					}
					if err != nil {
						log.Printf("failed to pack transaction log=%s: %v", lg.id, err)
						continue
					}
					txData, txHash, err = cl.CreateEncodedTransactionDataV1(&addr, input, blockLimit.Load(), "")
					if err != nil {
						log.Printf("failed to build transaction log=%s: %v", lg.id, err)
						continue
					}
					txIDs[lg.id] = fmt.Sprintf("%x", txHash)
				} else {
					// endorsement-stage path: the tx hash only exists once stage ②
					// signs; tx_id is temporarily the log_id
					txIDs[lg.id] = lg.id
				}
				builtLogs = append(builtLogs, built{lg: lg, rec: rec, txData: txData, txHash: txHash})
			}
			if len(builtLogs) == 0 {
				continue
			}

			// batch claim (same semantics as SFChain's mark-on-enqueue; a single
			// SQL avoids per-row network RTT)
			ids := make([]string, 0, len(builtLogs))
			for _, b := range builtLogs {
				ids = append(ids, b.lg.id)
			}
			claimed, err := claimBatch(db, ids, txIDs)
			if err != nil {
				log.Printf("batch claim failed: %v", err)
				continue
			}

			claimMs := time.Now().UnixMilli()
			var toPersist []preparedLog
			for _, b := range builtLogs {
				if !claimed[b.lg.id] {
					continue
				}
				lg := b.lg
				totalConsumed.Add(1)
				p := preparedLog{
					row: ingestRow{
						LogID: lg.id, Category: category, LogTsMs: lg.timestamp,
						ClaimedMs: claimMs, SubmittedMs: -1, ReceiptMs: -1,
						LatencyMs: -1, ConsensusMs: -1,
					},
					rec:    b.rec,
					txData: b.txData,
					txHash: b.txHash,
				}

				switch {
				case cfg.endorsePersist:
					// persist mode: store only the "awaiting endorsement" snapshot
					// (mirrors SFChain ①INSERT pending tx)
					toPersist = append(toPersist, p)
				case cfg.endorseInterval > 0:
					// in-memory queue: wait for stage ② concurrent signing
					// (mirrors SFChain's direct-push endorsement queue)
					queued.Add(1)
					mu.Lock()
					endorseQueue = append(endorseQueue, p)
					mu.Unlock()
				default:
					// no endorsement stage: sign and submit immediately
					// (submitted = claim time)
					p.row.SubmittedMs = claimMs
					submit(p)
				}

				if cfg.pacing > 0 {
					time.Sleep(cfg.pacing)
				}
			}

			// persist mode: batch-store "awaiting endorsement" snapshots
			// (multi-row INSERT; ON DUPLICATE handles re-consumption after sendTx
			// fails and returns the row: reset to unendorsed and unsent)
			for i := 0; i < len(toPersist); i += 250 {
				j := i + 250
				if j > len(toPersist) {
					j = len(toPersist)
				}
				var sb strings.Builder
				sb.WriteString("INSERT INTO fisco.endorsed_txs (log_id, log_ts_ms, claimed_ms, log_json) VALUES ")
				args := make([]interface{}, 0, (j-i)*4)
				for k, p := range toPersist[i:j] {
					if k > 0 {
						sb.WriteString(",")
					}
					sb.WriteString("(?,?,?,?)")
					jb, _ := json.Marshal(p.rec)
					args = append(args, p.row.LogID, p.row.LogTsMs, p.row.ClaimedMs, string(jb))
				}
				sb.WriteString(" ON DUPLICATE KEY UPDATE log_json=VALUES(log_json), tx_hex=NULL, sent=0")
				if _, err := db.Exec(sb.String(), args...); err != nil {
					log.Printf("failed to persist endorsement snapshots (one retry): %v", err)
					time.Sleep(100 * time.Millisecond)
					if _, err2 := db.Exec(sb.String(), args...); err2 != nil {
						log.Printf("endorsement snapshot persist retry failed, falling back to row-by-row: %v", err2)
						for _, p := range toPersist[i:j] {
							jb, _ := json.Marshal(p.rec)
							if _, e := db.Exec("INSERT INTO fisco.endorsed_txs (log_id, log_ts_ms, claimed_ms, log_json) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE log_json=VALUES(log_json), tx_hex=NULL, sent=0",
								p.row.LogID, p.row.LogTsMs, p.row.ClaimedMs, string(jb)); e != nil {
								log.Printf("failed to persist endorsement snapshot row log=%s: %v", p.row.LogID, e)
							}
						}
					}
				}
			}
		}

		// per-tick summary (process-local counters, accurate across batches)
		pending := countPending(db)
		persistUnsent := 0
		if cfg.endorsePersist {
			persistUnsent = countUnsent(db)
		}
		tickConsumed := totalConsumed.Load() - lastConsumed
		lastConsumed = totalConsumed.Load()
		fmt.Printf("[%s] consumed this tick %d | pending %d | awaiting endorsement %d | in flight %d | persisted unsent %d | total %d\n",
			time.Now().Format("15:04:05"), tickConsumed, pending, queued.Load(), inflight.Load(), persistUnsent, totalConsumed.Load())

		// exit conditions: nothing pending/queued/in-flight/unsent for the idle
		// duration, or duration exceeded; in-flight txs stalled for a long time
		// (the SDK occasionally drops callbacks) -> break out of the main loop
		// and fall back to hash-based recovery
		if cfg.duration > 0 && time.Since(start) > cfg.duration {
			stop = true
			break
		}
		if pending == 0 && queued.Load() == 0 && inflight.Load() > 0 {
			last := time.Unix(0, lastActivityNs.Load())
			if time.Since(last) > 15*time.Second {
				fmt.Printf("%d in-flight txs stalled for over 15s, switching to recovery\n", inflight.Load())
				stop = true
				break
			}
		}
		if pending == 0 && queued.Load() == 0 && inflight.Load() == 0 && persistUnsent == 0 {
			idleDeadline := tickStart.Add(cfg.poll + cfg.idle)
			for time.Now().Before(idleDeadline) {
				time.Sleep(500 * time.Millisecond)
				pu := 0
				if cfg.endorsePersist {
					pu = countUnsent(db)
				}
				if countPending(db) > 0 || queued.Load() > 0 || inflight.Load() > 0 || pu > 0 {
					break
				}
			}
			pu := 0
			if cfg.endorsePersist {
				pu = countUnsent(db)
			}
			if countPending(db) == 0 && queued.Load() == 0 && inflight.Load() == 0 && pu == 0 {
				fmt.Println("all logs consumed, exiting")
				stop = true
			}
		}

		if wait := cfg.poll - time.Since(tickStart); wait > 0 {
			time.Sleep(wait)
		}
	}

	// in-flight fallback: recover receipts by hash
	deadline := time.Now().Add(15 * time.Second)
	for inflight.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
	}
	if inflight.Load() > 0 {
		mu.Lock()
		stragglers := make([]ingestFlight, 0)
		for _, f := range flight {
			stragglers = append(stragglers, f)
		}
		mu.Unlock()
		for _, f := range stragglers {
			qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			r, err := cl.GetTransactionReceipt(qCtx, common.BytesToHash(f.txHash), false)
			cancel()
			row := f.row
			nowMs := time.Now().UnixMilli()
			row.ReceiptMs = nowMs
			row.LatencyMs = float64(nowMs) - float64(row.SubmittedMs)
			row.ConsensusMs = float64(nowMs) - float64(row.SubmittedMs)
			if err == nil && r != nil && r.Status == 0 {
				row.Status = r.Status
				row.BlockNum = r.BlockNumber
				row.ErrMsg = ""
				row.Recovered = true // latency includes the actual recovery time
			} else {
				row.ErrMsg = "receipt not returned: " + fmt.Sprint(err)
			}
			record(row)
		}
	}

	ingestSummarize(rows, cfg, start)
}

// ---------- MySQL helpers ----------

type pendingLog struct {
	id        string
	timestamp float64
	level     string
	message   string
	userId    string
	module    string
	project   string
	operation string
	status    string
}

func queryPendingLogs(db *sql.DB, category string, limit int) ([]pendingLog, error) {
	q := "SELECT id, `timestamp`, level, message, user_id, module, project, operation, status " +
		"FROM software_factory_logs WHERE processed = 0 AND category = ? " +
		"ORDER BY `timestamp` ASC LIMIT ?"
	rows, err := db.Query(q, category, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []pendingLog
	for rows.Next() {
		var lg pendingLog
		var ts float64
		var level, message, userId, module, project, operation, status sql.NullString
		if err := rows.Scan(&lg.id, &ts, &level, &message, &userId, &module, &project, &operation, &status); err != nil {
			return nil, err
		}
		lg.timestamp = ts
		lg.level, lg.message, lg.userId = level.String, message.String, userId.String
		lg.module, lg.project, lg.operation, lg.status = module.String, project.String, operation.String, status.String
		out = append(out, lg)
	}
	return out, rows.Err()
}

func claimLog(db *sql.DB, id, txID string) (bool, error) {
	res, err := db.Exec("UPDATE software_factory_logs SET processed = 1, tx_id = ? WHERE id = ? AND processed = 0", txID, id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// claimBatch claims a whole round of logs in one SQL (per-row UPDATEs' network
// RTT becomes the injection-phase bottleneck: ingest container ->
// host-gateway -> MySQL is ~10ms per row, i.e. ~20s for 2000 rows).
// Returns the set of successfully claimed ids; same semantics as per-row
// claiming (processed=0 -> 1 plus writing tx_id)
func claimBatch(db *sql.DB, ids []string, txIDs map[string]string) (map[string]bool, error) {
	if len(ids) == 0 {
		return map[string]bool{}, nil
	}
	var sb strings.Builder
	sb.WriteString("UPDATE software_factory_logs SET processed = 1, tx_id = CASE id ")
	args := make([]interface{}, 0, len(ids)*2+1)
	for _, id := range ids {
		sb.WriteString("WHEN ? THEN ? ")
		args = append(args, id, txIDs[id])
	}
	sb.WriteString("END WHERE id IN (")
	for i, id := range ids {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("?")
		args = append(args, id)
	}
	sb.WriteString(") AND processed = 0")
	res, err := db.Exec(sb.String(), args...)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if int(n) == len(ids) {
		out := make(map[string]bool, len(ids))
		for _, id := range ids {
			out[id] = true
		}
		return out, nil
	}
	// partial claim (rare: a concurrent consumer exists) -- re-query which ids
	// are actually still pending and skip them
	out := make(map[string]bool, len(ids))
	for _, id := range ids {
		out[id] = true
	}
	qArgs := make([]interface{}, 0, len(ids))
	qMarks := make([]string, 0, len(ids))
	for _, id := range ids {
		qMarks = append(qMarks, "?")
		qArgs = append(qArgs, id)
	}
	rows, err := db.Query("SELECT id FROM software_factory_logs WHERE id IN ("+
		strings.Join(qMarks, ",")+") AND processed = 0", qArgs...)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil {
				delete(out, id)
			}
		}
	}
	return out, nil
}

func unclaimLog(db *sql.DB, id string) {
	db.Exec("UPDATE software_factory_logs SET processed = 0, tx_id = NULL WHERE id = ?", id)
}

func countPending(db *sql.DB) int {
	var n sql.NullInt64
	if err := db.QueryRow("SELECT COUNT(*) FROM software_factory_logs WHERE processed = 0").Scan(&n); err != nil {
		return -1
	}
	return int(n.Int64)
}

// countUnsent persist mode: number of endorsed txs persisted but not yet sent
// (the exit condition must include this; otherwise when ① persists faster
// than ② reads back, the main loop exits early with a backlog unsent)
func countUnsent(db *sql.DB) int {
	var n sql.NullInt64
	if err := db.QueryRow("SELECT COUNT(*) FROM fisco.endorsed_txs WHERE sent = 0").Scan(&n); err != nil {
		return -1
	}
	return int(n.Int64)
}

// ---------- ② endorsement stage: concurrent evidence building + SDK signing (mirrors SFChain endorsement_service) ----------

const endorseWorkers = 4 // aligned with SFChain endorsement_service -max-workers 4

type endorseResult struct {
	ok     []preparedLog // signed (txData/txHash/txBytes all set)
	failed []preparedLog // failures (reason recorded in row.ErrMsg)
}

// endorseBatch concurrently processes a batch of "awaiting endorsement" logs:
// build evidence (if enabled) -> Pack -> RLP -> SDK sign -> assemble the full
// transaction. Structurally identical to SFChain endorsement_service's 4
// concurrent signing workers (evidence object, secp256k1 ECDSA signing
// algorithm, and concurrency all match).
func endorseBatch(ctx context.Context, cl *client.Client, parsed abi.ABI, addr common.Address, cfg ingestConfig, blockLimit int64, batch []preparedLog) endorseResult {
	res := endorseResult{}
	if len(batch) == 0 {
		return res
	}
	workers := endorseWorkers
	if workers > len(batch) {
		workers = len(batch)
	}
	jobs := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				p := batch[i]
				var input []byte
				var err error
				if cfg.endorseCheck {
					ev, signer, sig, err2 := buildCheckedEndorsement(p.rec)
					if err2 != nil {
						mu.Lock()
						p.row.ErrMsg = err2.Error()
						res.failed = append(res.failed, p)
						mu.Unlock()
						continue
					}
					p.rec.Endorsement = ev
					messageHash := rq1EndorsementHash(p.rec)
					input, err = parsed.Pack("recordLogChecked", p.rec, messageHash, []common.Address{signer}, [][]byte{sig})
				} else {
					if cfg.endorseEvidence {
						ev, err2 := buildEndorsement(p.rec)
						if err2 != nil {
							mu.Lock()
							p.row.ErrMsg = err2.Error()
							res.failed = append(res.failed, p)
							mu.Unlock()
							continue
						}
						p.rec.Endorsement = ev
					}
					input, err = parsed.Pack("recordLog", p.rec)
				}
				if err != nil {
					mu.Lock()
					p.row.ErrMsg = "pack: " + err.Error()
					res.failed = append(res.failed, p)
					mu.Unlock()
					continue
				}
				txData, txHash, err := cl.CreateEncodedTransactionDataV1(&addr, input, blockLimit, "")
				if err != nil {
					mu.Lock()
					p.row.ErrMsg = "txData: " + err.Error()
					res.failed = append(res.failed, p)
					mu.Unlock()
					continue
				}
				sig, err := cl.CreateEncodedSignature(txHash)
				if err != nil {
					mu.Lock()
					p.row.ErrMsg = "sign: " + err.Error()
					res.failed = append(res.failed, p)
					mu.Unlock()
					continue
				}
				tx, err := cl.CreateEncodedTransaction(txData, txHash, sig, 0, "")
				if err != nil {
					mu.Lock()
					p.row.ErrMsg = "assemble: " + err.Error()
					res.failed = append(res.failed, p)
					mu.Unlock()
					continue
				}
				p.txData, p.txHash, p.txBytes = txData, txHash, tx
				mu.Lock()
				res.ok = append(res.ok, p)
				mu.Unlock()
			}
		}()
	}
	for i := range batch {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return res
}

// batchUpdateTxHex persists signing results in batch (mirrors SFChain
// UpdateTransactionsEndorsementsBatch's UPDATE CASE WHEN form; sharded
// 500 per batch)
func batchUpdateTxHex(db *sql.DB, signed []preparedLog) {
	for i := 0; i < len(signed); i += 500 {
		j := i + 500
		if j > len(signed) {
			j = len(signed)
		}
		var sb strings.Builder
		sb.WriteString("UPDATE fisco.endorsed_txs SET tx_hex = CASE log_id ")
		args := make([]interface{}, 0, (j-i)*2+1)
		for _, p := range signed[i:j] {
			sb.WriteString("WHEN ? THEN ? ")
			args = append(args, p.row.LogID, hex.EncodeToString(p.txBytes))
		}
		sb.WriteString("END, tx_hash = CASE log_id ")
		for _, p := range signed[i:j] {
			sb.WriteString("WHEN ? THEN ? ")
			args = append(args, p.row.LogID, hex.EncodeToString(p.txHash))
		}
		sb.WriteString("END WHERE log_id IN (")
		for k, p := range signed[i:j] {
			if k > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("?")
			args = append(args, p.row.LogID)
		}
		sb.WriteString(")")
		if _, err := db.Exec(sb.String(), args...); err != nil {
			// concurrent with ①'s INSERT this can hit an InnoDB deadlock (1213):
			// a retry suffices; the failed shard keeps tx_hex IS NULL and is
			// re-signed by the next endorsement round
			log.Printf("failed to persist endorsement results in batch (retry in 200ms): %v", err)
			time.Sleep(200 * time.Millisecond)
			if _, err2 := db.Exec(sb.String(), args...); err2 != nil {
				log.Printf("endorsement batch persist retry failed (re-signed next round): %v", err2)
			}
		}
	}
}

// markSentBatch marks rows as sent in batch (IN shards)
func markSentBatch(db *sql.DB, ids []string) {
	for i := 0; i < len(ids); i += 500 {
		j := i + 500
		if j > len(ids) {
			j = len(ids)
		}
		ph := strings.Repeat("?,", j-i)
		args := make([]interface{}, 0, j-i)
		for _, id := range ids[i:j] {
			args = append(args, id)
		}
		db.Exec("UPDATE fisco.endorsed_txs SET sent = 1 WHERE log_id IN ("+strings.TrimRight(ph, ",")+")", args...)
	}
}

// ---------- endorsement evidence (isomorphic to SFChain Endorsement) ----------

// endorseUser endorsement user (from sfchain.users, sharing the same identity
// system as the SFChain endorsement service)
type endorseUser struct {
	userID    string
	publicKey string // hex (compressed public key)
	role      string
	key       *ecdsa.PrivateKey
}

var endorseUsers map[string]*endorseUser

// loadEndorseUsers loads all user keys at startup (evidence mode)
func loadEndorseUsers(db *sql.DB) error {
	rows, err := db.Query("SELECT user_id, public_key, private_key, role FROM sfchain.users")
	if err != nil {
		return err
	}
	defer rows.Close()
	endorseUsers = make(map[string]*endorseUser)
	for rows.Next() {
		var uid, pub, priv, role string
		if err := rows.Scan(&uid, &pub, &priv, &role); err != nil {
			continue
		}
		key, err := crypto.HexToECDSA(priv)
		if err != nil {
			log.Printf("failed to parse private key for user %s: %v", uid, err)
			continue
		}
		endorseUsers[uid] = &endorseUser{userID: uid, publicKey: pub, role: role, key: key}
	}
	log.Printf("endorsement evidence mode: loaded %d user keys", len(endorseUsers))
	return nil
}

// buildEndorsement the responsible user signs the log content hash, building
// an evidence object isomorphic to SFChain Endorsement
// (user_id/public_key/signature/timestamp/role), stored on-chain with the tx
func buildEndorsement(rec LogRecord) (string, error) {
	evidence, _, _, err := buildCheckedEndorsement(rec)
	return evidence, err
}

// buildCheckedEndorsement returns the serialized evidence plus the exact
// signer/address/signature tuple passed to the FISCO contract verifier.
func buildCheckedEndorsement(rec LogRecord) (string, common.Address, []byte, error) {
	u := endorseUsers[rec.UserId]
	if u == nil {
		return "", common.Address{}, nil, fmt.Errorf("log owner %q not found in sfchain.users", rec.UserId)
	}
	digest := rq1EndorsementHash(rec).Bytes()
	sig, err := crypto.Sign(digest, u.key)
	if err != nil {
		return "", common.Address{}, nil, err
	}
	ev, err := json.Marshal(map[string]interface{}{
		"user_id":    rec.UserId,
		"public_key": u.publicKey,
		"signature":  "0x" + hex.EncodeToString(sig),
		"timestamp":  float64(time.Now().UnixNano()) / 1e6,
		"role":       u.role,
	})
	if err != nil {
		return "", common.Address{}, nil, err
	}
	return string(ev), crypto.PubkeyToAddress(u.key.PublicKey), sig, nil
}

// ---------- summary ----------

func ingestSummarize(rows []ingestRow, cfg ingestConfig, start time.Time) {
	ts := time.Now().Format("20060102_150405")
	csvPath := filepath.Join(cfg.outDir, fmt.Sprintf("fisco_ingest_%s.csv", ts))
	f, err := os.Create(csvPath)
	if err == nil {
		defer f.Close()
		f.WriteString("log_id,category,log_ts_ms,claimed_ms,submitted_ms,receipt_ms,latency_ms,consensus_ms,status,block_number,error,recovered\n")
		for _, r := range rows {
			errMsg := strings.ReplaceAll(r.ErrMsg, "\"", "'")
			fmt.Fprintf(f, "%s,%s,%.3f,%d,%d,%d,%.3f,%.3f,%d,%d,%s,%t\n",
				r.LogID, r.Category, r.LogTsMs, r.ClaimedMs, r.SubmittedMs, r.ReceiptMs,
				r.LatencyMs, r.ConsensusMs, r.Status, r.BlockNum, errMsg, r.Recovered)
		}
	}

	okLat, okCons := make([]float64, 0, len(rows)), make([]float64, 0, len(rows))
	failed := 0
	var minSubmitted, maxReceipt float64
	for _, r := range rows {
		if r.ErrMsg == "" && r.Status == 0 {
			okLat = append(okLat, r.LatencyMs)
			if r.SubmittedMs > 0 {
				okCons = append(okCons, r.ConsensusMs)
			}
			if minSubmitted == 0 || float64(r.SubmittedMs) < minSubmitted {
				minSubmitted = float64(r.SubmittedMs)
			}
			if max := float64(r.ReceiptMs); max > maxReceipt {
				maxReceipt = max
			}
		} else {
			failed++
		}
	}

	type ingestSummary struct {
		Chain              string             `json:"chain"`
		Mode               string             `json:"mode"`
		PollIntervalSec    float64            `json:"poll_interval_sec"`
		Threshold          int                `json:"threshold_per_category"`
		PacingMs           int64              `json:"pacing_ms"`
		EndorseIntervalSec float64            `json:"endorse_interval_sec"`
		EndorseEvidence    bool               `json:"endorse_evidence"`
		EndorseCheck       bool               `json:"endorse_check"`
		Confirmed          int                `json:"confirmed"`
		Failed             int                `json:"failed"`
		ConfirmWindowSec   float64            `json:"confirm_window_sec"`
		ThroughputTPS      float64            `json:"throughput_tps"`
		LatencyMs          map[string]float64 `json:"latency_ms"`           // RQ1: receipt - submitted
		ConsensusLatencyMs map[string]float64 `json:"consensus_latency_ms"` // consensus segment: receipt - submitted
		RunDurationSec     float64            `json:"run_duration_sec"`
	}
	s := ingestSummary{
		Chain: "fisco-bcos", Mode: "ingest(sfchain-compatible)",
		PollIntervalSec: cfg.poll.Seconds(), Threshold: cfg.threshold,
		PacingMs: cfg.pacing.Milliseconds(), EndorseIntervalSec: cfg.endorseInterval.Seconds(),
		EndorseEvidence: cfg.endorseEvidence, EndorseCheck: cfg.endorseCheck,
		Confirmed: len(okLat), Failed: failed,
		RunDurationSec: round(time.Since(start).Seconds()),
	}
	if len(okLat) > 0 && maxReceipt > minSubmitted {
		window := (maxReceipt - minSubmitted) / 1000
		s.ConfirmWindowSec = round(window)
		s.ThroughputTPS = round(float64(len(okLat)) / window)
		s.LatencyMs = latStats(okLat)
	}
	if len(okCons) > 0 {
		s.ConsensusLatencyMs = latStats(okCons)
	}
	jsonPath := filepath.Join(cfg.outDir, fmt.Sprintf("fisco_ingest_%s.summary.json", ts))
	if b, err := json.MarshalIndent(s, "", "  "); err == nil {
		os.WriteFile(jsonPath, b, 0o644)
	}

	fmt.Printf("\n========== FISCO BCOS ingest results (submission-to-receipt) ==========\n")
	fmt.Printf("consumed: %d rows, failed: %d rows, ran %.1f s\n", s.Confirmed, s.Failed, s.RunDurationSec)
	if s.ThroughputTPS > 0 {
		fmt.Printf("throughput: %.1f rows/s (first submission -> last receipt, window %.1f s)\n", s.ThroughputTPS, s.ConfirmWindowSec)
	}
	if s.LatencyMs != nil {
		fmt.Printf("post-admission latency(ms): avg=%.1f p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n",
			s.LatencyMs["avg"], s.LatencyMs["p50"], s.LatencyMs["p90"], s.LatencyMs["p95"], s.LatencyMs["p99"], s.LatencyMs["max"])
	}
	if s.ConsensusLatencyMs != nil {
		fmt.Printf("consensus-segment latency(ms): avg=%.1f p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n",
			s.ConsensusLatencyMs["avg"], s.ConsensusLatencyMs["p50"], s.ConsensusLatencyMs["p90"],
			s.ConsensusLatencyMs["p95"], s.ConsensusLatencyMs["p99"], s.ConsensusLatencyMs["max"])
	}
	fmt.Printf("detail CSV: %s\nsummary JSON: %s\n", csvPath, jsonPath)
}
