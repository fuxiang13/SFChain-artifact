package core

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"reflect"
	"sfchain/internal/database"
	"sfchain/internal/database/mysql"
	"sfchain/pkg/crypto"
	"sfchain/pkg/types"
	"strconv"
	"sync"
	"time"
)

// EndorsementService is the endorsement service
type EndorsementService struct {
	txPoolDB      database.TransactionPoolDatabaseInterface
	userDB        database.UserDatabaseInterface
	Interval      time.Duration
	MaxWorkers    int    // concurrent endorsement workers (aligned with 4-org parallel endorsement on the Fabric side)
	DirectPushURL string // when non-empty, enables in-memory endorsement mode: signature results are not persisted but POSTed directly to the management node's in-memory packaging pool
	StopChan      chan struct{}
	WG            sync.WaitGroup
	mu            sync.RWMutex
	userCache     map[string]*EndorserUser
}

// EndorserUser is endorsement user information
type EndorserUser struct {
	UserID     string
	PublicKey  string
	PrivateKey string
	Role       string
}

// NewEndorsementService creates an endorsement service
func NewEndorsementService(txPoolDB database.TransactionPoolDatabaseInterface, userDB database.UserDatabaseInterface) *EndorsementService {
	return &EndorsementService{
		txPoolDB:   txPoolDB,
		userDB:     userDB,
		Interval:   10 * time.Second, // default interval 10 seconds
		MaxWorkers: 1,
		StopChan:   make(chan struct{}),
		userCache:  make(map[string]*EndorserUser),
	}
}

// Start starts the endorsement service
func (es *EndorsementService) Start() {
	es.mu.Lock()
	defer es.mu.Unlock()

	if es.txPoolDB == nil {
		log.Println("warning: transaction pool database not initialized; the endorsement service will start automatically once the database is available")
		return
	}

	log.Printf("endorsement service started, processing interval: %v", es.Interval)

	es.WG.Add(1)
	go es.processLoop()
}

// Stop stops the endorsement service
func (es *EndorsementService) Stop() {
	es.mu.Lock()
	defer es.mu.Unlock()

	if es.StopChan != nil {
		close(es.StopChan)
	}

	es.WG.Wait()
	log.Println("endorsement service stopped")
}

// SetInterval sets the processing interval
func (es *EndorsementService) SetInterval(interval time.Duration) {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.Interval = interval
}

// processLoop is the processing loop
func (es *EndorsementService) processLoop() {
	defer es.WG.Done()

	ticker := time.NewTicker(es.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			es.processEndorsements()
		case <-es.StopChan:
			return
		}
	}
}

// processEndorsements processes endorsements
func (es *EndorsementService) processEndorsements() {
	if es.txPoolDB == nil || es.userDB == nil {
		return
	}

	// fetch unendorsed transactions
	unendorsedTxs, err := es.txPoolDB.GetUnendorsedTransactions()
	if err != nil {
		log.Printf("failed to fetch unendorsed transactions: %v", err)
		return
	}

	if len(unendorsedTxs) == 0 {
		return
	}

	log.Printf("starting to process %d unendorsed transactions", len(unendorsedTxs))

	workers := es.MaxWorkers
	if workers < 1 {
		workers = 1
	}
	start := time.Now()
	okCount, failCount := 0, 0
	var cntMu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)

	// sign concurrently (pure in-memory), then write back to the database in batch:
	// only 2 SQL statements per batch (endorsements CASE batch + status IN batch), eliminating per-transaction UPDATE write amplification
	signed := make([]*types.Transaction, 0, len(unendorsedTxs))
	var smu sync.Mutex

	for _, txInterface := range unendorsedTxs {
		tx, ok := txInterface.(*types.Transaction)
		if !ok {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(tx *types.Transaction) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := es.signTransaction(tx); err != nil {
				log.Printf("failed to endorse transaction %s: %v", tx.TXID[:16], err)
				cntMu.Lock()
				failCount++
				cntMu.Unlock()
				return
			}
			smu.Lock()
			signed = append(signed, tx)
			smu.Unlock()
			cntMu.Lock()
			okCount++
			cntMu.Unlock()
		}(tx)
	}
	wg.Wait()

	// persist endorsements and status in batch (single UPDATE ... CASE covering data/user_signature/status);
	// in-memory endorsement mode (DirectPushURL non-empty): signature results are not persisted but POSTed in batch to the management node's in-memory packaging pool,
	// after success only a lightweight status mark is made (single IN UPDATE, no data JSON rewrite) to prevent re-reading next round
	if n := len(signed); n > 0 {
		if es.DirectPushURL != "" {
			if err := es.pushDirect(signed); err != nil {
				log.Printf("failed to push endorsed transactions (%d txs): %v", n, err)
				failCount += n
				okCount -= n
			} else {
				txIDs := make([]string, 0, n)
				for _, tx := range signed {
					txIDs = append(txIDs, tx.TXID)
				}
				if err := es.txPoolDB.UpdateTransactionsStatusBatch(txIDs, "endorsed", time.Now()); err != nil {
					log.Printf("failed to mark status after push (%d txs): %v", n, err)
				}
			}
		} else {
			txs := make([]interface{}, 0, n)
			for _, it := range signed {
				txs = append(txs, it)
			}
			if err := es.txPoolDB.UpdateTransactionsEndorsementsBatch(txs); err != nil {
				log.Printf("failed to write back endorsements in batch (%d txs): %v", n, err)
				failCount += n
				okCount -= n
			}
		}
	}
	log.Printf("endorsement batch done: %d ok, %d failed, elapsed %v (workers=%d, direct=%v)", okCount, failCount, time.Since(start).Round(time.Millisecond), workers, es.DirectPushURL != "")
}

// pushDirect implements in-memory endorsement mode: POST signed transactions in small batches to the management node /api/tx/endorsed
func (es *EndorsementService) pushDirect(txs []*types.Transaction) error {
	const batch = 200
	client := &http.Client{Timeout: 30 * time.Second}
	for i := 0; i < len(txs); i += batch {
		j := i + batch
		if j > len(txs) {
			j = len(txs)
		}
		body, err := json.Marshal(txs[i:j])
		if err != nil {
			return err
		}
		pStart := time.Now()
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		gz.Write(body)
		gz.Close()
		req, err := http.NewRequest("POST", es.DirectPushURL, bytes.NewReader(buf.Bytes()))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")
		resp, err := client.Do(req)
		log.Printf("push batch (%d txs, %dB->%dB): post=%v err=%v", j-i, len(body), buf.Len(), time.Since(pStart).Round(time.Millisecond), err)
		if err != nil {
			return err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("direct push http %d", resp.StatusCode)
		}
	}
	return nil
}

// signTransaction generates an endorsement signature for a single transaction (pure in-memory, no database write; persistence is done by the batch write-back)
func (es *EndorsementService) signTransaction(tx *types.Transaction) error {
	if os.Getenv("SFCHAIN_RQ1") == "1" {
		return es.signRQ1SingleEndorsement(tx)
	}

	// get the transaction operator ID
	operatorID, err := es.getOperatorID(tx)
	if err != nil {
		return err
	}

	// get the operator's user information
	user, err := es.getUserByID(operatorID)
	if err != nil {
		return fmt.Errorf("failed to get user information: %w", err)
	}

	// create the endorsement
	endorsement := types.Endorsement{
		UserID:    user.UserID,
		PublicKey: user.PublicKey,
		Signature: es.generateSignature(tx, user.PrivateKey),
		Timestamp: float64(time.Now().UnixNano()) / 1e6,
		Role:      user.Role,
	}

	// add the endorsement to the transaction
	tx.Endorsements = append(tx.Endorsements, endorsement)

	// mark the transaction as endorsed
	tx.Endorsed = true
	return nil
}

// signRQ1SingleEndorsement is the transaction-level endorsement used by the
// current RQ1 protocol. The responsible user signs the canonical source-log
// representation once; the four SFChain role nodes attest each block at the
// consensus layer.
func (es *EndorsementService) signRQ1SingleEndorsement(tx *types.Transaction) error {
	operatorID, err := es.getOperatorID(tx)
	if err != nil {
		return err
	}
	user, err := es.getUserByID(operatorID)
	if err != nil {
		return fmt.Errorf("failed to get RQ1 responsible user: %w", err)
	}
	signature := es.generateRQ1Signature(tx, user.PrivateKey)
	if signature == "" {
		return fmt.Errorf("RQ1 responsible user signature failed: %s", operatorID)
	}
	tx.Endorsements = []types.Endorsement{{
		UserID:    user.UserID,
		PublicKey: user.PublicKey,
		Signature: signature,
		Timestamp: float64(time.Now().UnixNano()) / 1e6,
		Role:      user.Role,
	}}
	tx.Endorsed = true
	return nil
}

// endorseTransaction adds an endorsement to a single transaction (per-tx persistence, kept for the low-throughput path)
func (es *EndorsementService) endorseTransaction(tx *types.Transaction) error {
	if err := es.signTransaction(tx); err != nil {
		return err
	}

	// update the transaction in the pool
	if err := es.txPoolDB.UpdateTransactionEndorsements(tx.TXID, interface{}(tx.Endorsements)); err != nil {
		return fmt.Errorf("failed to update transaction endorsement: %w", err)
	}

	// update the transaction status to endorsed
	if err := es.txPoolDB.UpdateTransactionStatus(tx.TXID, "endorsed"); err != nil {
		log.Printf("failed to update transaction status: %v", err)
	}

	return nil
}

// getOperatorID extracts the operator ID from a transaction
func (es *EndorsementService) getOperatorID(tx *types.Transaction) (string, error) {
	// prefer the transaction's UserID field
	if tx.UserID != "" {
		return tx.UserID, nil
	}

	// try to get it from LogData
	if userID, ok := tx.LogData["UserID"].(string); ok && userID != "" {
		return userID, nil
	}

	if userID, ok := tx.LogData["user_id"].(string); ok && userID != "" {
		return userID, nil
	}

	if userID, ok := tx.LogData["operator"].(string); ok && userID != "" {
		return userID, nil
	}

	if userID, ok := tx.LogData["operator_id"].(string); ok && userID != "" {
		return userID, nil
	}

	return "", fmt.Errorf("cannot get operator ID from transaction")
}

// getUserByID fetches user information by user ID (cached, avoiding a database query per endorsement)
func (es *EndorsementService) getUserByID(userID string) (*EndorserUser, error) {
	es.mu.RLock()
	if u, ok := es.userCache[userID]; ok {
		es.mu.RUnlock()
		return u, nil
	}
	es.mu.RUnlock()

	u, err := es.loadUser(userID)
	if err != nil {
		return nil, err
	}
	es.mu.Lock()
	es.userCache[userID] = u
	es.mu.Unlock()
	return u, nil
}

// loadUser loads and converts user information from the user database
func (es *EndorsementService) loadUser(userID string) (*EndorserUser, error) {
	// use the GetUser method of UserDatabaseInterface
	user, err := es.userDB.GetUser(userID)
	if err != nil {
		return nil, err
	}

	// try to convert to the database.User type
	if u, ok := user.(*database.User); ok {
		return &EndorserUser{
			UserID:     u.UserID,
			PublicKey:  u.PublicKey,
			PrivateKey: u.PrivateKey,
			Role:       u.Role,
		}, nil
	}

	// try to convert to the mysql.User type
	if u, ok := user.(*mysql.User); ok {
		return &EndorserUser{
			UserID:     u.UserID,
			PublicKey:  u.PublicKey,
			PrivateKey: u.PrivateKey,
			Role:       u.Role,
		}, nil
	}

	// try to extract user information via reflection
	if endorserUser, err := es.extractUserInfo(user); err == nil {
		return endorserUser, nil
	}

	return nil, fmt.Errorf("malformed user information")
}

// extractUserInfo extracts an EndorserUser from user information via reflection
func (es *EndorsementService) extractUserInfo(user interface{}) (*EndorserUser, error) {
	// use reflection to get the fields of the user information
	v := reflect.ValueOf(user)

	// if it is a pointer, get the pointed-to value
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// check whether it is a struct
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("user information is not a struct type")
	}

	// extract fields
	userID := ""
	publicKey := ""
	privateKey := ""
	role := ""

	// try to get field values
	if userIDField := v.FieldByName("UserID"); userIDField.IsValid() && userIDField.Kind() == reflect.String {
		userID = userIDField.String()
	}

	if publicKeyField := v.FieldByName("PublicKey"); publicKeyField.IsValid() && publicKeyField.Kind() == reflect.String {
		publicKey = publicKeyField.String()
	}

	if privateKeyField := v.FieldByName("PrivateKey"); privateKeyField.IsValid() && privateKeyField.Kind() == reflect.String {
		privateKey = privateKeyField.String()
	}

	if roleField := v.FieldByName("Role"); roleField.IsValid() && roleField.Kind() == reflect.String {
		role = roleField.String()
	}

	// check that all required fields exist
	if userID == "" || publicKey == "" || privateKey == "" || role == "" {
		return nil, fmt.Errorf("user information is missing required fields")
	}

	return &EndorserUser{
		UserID:     userID,
		PublicKey:  publicKey,
		PrivateKey: privateKey,
		Role:       role,
	}, nil
}

// getMapKeys returns all keys of a map
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// generateSignature generates a signature (ECDSA secp256k1, for transaction-level endorsement)
func (es *EndorsementService) generateSignature(tx *types.Transaction, privateKey string) string {
	return crypto.ECDSASign(privateKey, []byte(tx.TXID))
}

// generateRQ1Signature uses a canonical, user-visible log representation.
// The same field order is implemented by the FISCO RQ1 load generator.
func (es *EndorsementService) generateRQ1Signature(tx *types.Transaction, privateKey string) string {
	content := rq1CanonicalLog(tx.LogData)
	return crypto.ECDSASign(privateKey, []byte(content))
}

func rq1CanonicalLog(logData map[string]interface{}) string {
	get := func(k string) string {
		if v, ok := logData[k]; ok {
			if k == "timestamp" {
				switch n := v.(type) {
				case float64:
					return strconv.FormatUint(uint64(n), 10)
				case int64:
					return strconv.FormatInt(n, 10)
				case int:
					return strconv.Itoa(n)
				}
			}
			return fmt.Sprint(v)
		}
		return ""
	}
	return get("id") + "|" + get("category") + "|" + get("timestamp") + "|" +
		get("level") + "|" + get("message") + "|" + get("user_id") + "|" +
		get("module") + "|" + get("project") + "|" + get("operation") + "|" +
		get("status")
}
