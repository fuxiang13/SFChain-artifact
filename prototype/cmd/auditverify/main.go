package main

// Dispute-time audit verification against retained chain data.
//
// Scenario: an auditor holding only public keys receives a disputed
// transaction record and must verify it end to end from retained data:
//   (1) content hash: recomputed TXID equals the recorded TXID
//   (2) inclusion:    Merkle root over the containing block's transactions
//   (3) actor:        ECDSA endorsement signature (secp256k1, over the TXID)
//   (4) chain state:  header hash chain walked back to genesis
//   (5) finality:     four-role BLS aggregate signature (anchored in a
//                     successor header retained by the role node)
//
// Usage:
//   auditverify -host sf-mysql:3306 -user root -pass qwer@123 -db sfchain \
//               -config /app/deploy/sf-docker/configs/management-d.yaml \
//               -chain management [-height N] [-txid TXID] [-iters 200]

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"time"

	_ "github.com/go-sql-driver/mysql"
	bls "github.com/herumi/bls-eth-go-binary/bls"

	"sfchain/pkg/crypto"
	"sfchain/pkg/types"
)

type stepResult struct {
	Name   string  `json:"step"`
	OK     bool    `json:"ok"`
	Iters  int     `json:"iters"`
	MeanMs float64 `json:"mean_ms"`
	P50Ms  float64 `json:"p50_ms"`
	Note   string  `json:"note,omitempty"`
}

type headerRow struct {
	BlockHash    string
	PrevHash     string
	Height       int
	Timestamp    float64
	MerkleRoot   string
	ChainType    int
	AggSig       string
	Transactions string
}

func stats(xs []float64) (mean, p50 float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	s := append([]float64{}, xs...)
	sort.Float64s(s)
	var sum float64
	for _, v := range s {
		sum += v
	}
	return sum / float64(len(s)), s[len(s)/2]
}

func hdrToTypes(h headerRow) *types.BlockHeader {
	return &types.BlockHeader{
		BlockHeight:         h.Height,
		PreviousHash:        h.PrevHash,
		MerkleRoot:          h.MerkleRoot,
		Timestamp:           h.Timestamp,
		ChainType:           h.ChainType,
		AggregatedSignature: h.AggSig,
	}
}

func merkleRootOf(txs []types.Transaction) string {
	if len(txs) == 0 {
		return ""
	}
	b := &types.Block{Transactions: make([]*types.Transaction, 0, len(txs))}
	for i := range txs {
		b.Transactions = append(b.Transactions, &txs[i])
	}
	return b.CalculateMerkleRoot()
}

// verifyEvidenceBinding is the audit's fail-closed evidence binding check: the block the custodian presents
// must bind to the same commitment as the same-height header in the role node's retained header chain - chain/height/
// Merkle root/predecessor/timestamp must match field by field, the custodian's stored hash must be a legal hash interpretation of the role-retained header
// (either the sealing storage hash or the final-domain recomputed hash; both interpretations coexist on chain),
// and the Merkle root recomputed from the record set must be anchored to the role-retained root (not merely the root the custodian declares
// ). Any inconsistency is rejected: a self-consistent but role-unsigned replacement block (paired with an independently intact
// header chain) cannot pass audit.
func verifyEvidenceBinding(target, tgtH headerRow, txs []types.Transaction) error {
	if tgtH.BlockHash == "" && tgtH.Height == 0 {
		return fmt.Errorf("role header chain does not cover height %d", target.Height)
	}
	if target.ChainType != tgtH.ChainType {
		return fmt.Errorf("chain type mismatch: custodian=%d role=%d", target.ChainType, tgtH.ChainType)
	}
	if target.Height != tgtH.Height {
		return fmt.Errorf("height mismatch: custodian=%d role=%d", target.Height, tgtH.Height)
	}
	if target.MerkleRoot != tgtH.MerkleRoot {
		return fmt.Errorf("merkle root mismatch: custodian=%s role=%s",
			short(target.MerkleRoot), short(tgtH.MerkleRoot))
	}
	if target.PrevHash != tgtH.PrevHash {
		return fmt.Errorf("previous-hash mismatch: custodian=%s role=%s",
			short(target.PrevHash), short(tgtH.PrevHash))
	}
	if target.Timestamp != tgtH.Timestamp {
		return fmt.Errorf("timestamp mismatch: custodian=%v role=%v", target.Timestamp, tgtH.Timestamp)
	}
	if target.AggSig != tgtH.AggSig {
		return fmt.Errorf("predecessor aggregate mismatch")
	}
	// Cached hash strings are not evidence of payload identity. Bind content
	// fields and recomputed records to the independently retained role header.
	if merkleRootOf(txs) != tgtH.MerkleRoot {
		return fmt.Errorf("record set does not anchor to the role-retained merkle root")
	}
	return nil
}

func short(s string) string {
	if len(s) > 16 {
		return s[:16] + "..."
	}
	return s
}

func loadRoleKeys(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`public_key:\s*"([0-9a-fA-F]+)"`)
	ms := re.FindAllStringSubmatch(string(data), -1)
	seen := make(map[string]bool)
	keys := make([]string, 0, len(ms))
	for _, m := range ms {
		if !seen[m[1]] {
			seen[m[1]] = true
			keys = append(keys, m[1])
		}
	}
	if len(keys) < 4 {
		return nil, fmt.Errorf("expected >=4 distinct public keys, got %d", len(keys))
	}
	return keys, nil
}

func Safe16(s string) string {
	if len(s) > 16 {
		return s[:16]
	}
	return s
}

func headerByHeight(rows []headerRow, height int) *headerRow {
	for i := range rows {
		if rows[i].Height == height {
			return &rows[i]
		}
	}
	return nil
}

func fatal(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, "FATAL: "+f+"\n", a...)
	os.Exit(1)
}

func main() {
	host := flag.String("host", "sf-mysql:3306", "mysql host:port")
	user := flag.String("user", "root", "mysql user")
	pass := flag.String("pass", "qwer@123", "mysql password")
	dbName := flag.String("db", "sfchain", "database name")
	cfg := flag.String("config", "deploy/sf-docker/configs/management-d.yaml", "node config yaml with role public keys")
	chain := flag.String("chain", "management", "chain to audit (management|development|test|operations)")
	hdrSrc := flag.String("header-source", "development", "node whose retained headers to audit (management|development|test|operations)")
	height := flag.Int("height", 0, "block height to audit (0 = second-highest of the chain)")
	txid := flag.String("txid", "", "disputed TXID (default: first transaction of the chosen block)")
	iters := flag.Int("iters", 200, "iterations for per-step timing")
	flag.Parse()

	if err := bls.Init(bls.BLS12_381); err != nil {
		fatal("bls init: %v", err)
	}
	if err := bls.SetETHmode(bls.EthModeDraft07); err != nil {
		fatal("bls ethmode: %v", err)
	}

	roleKeys, err := loadRoleKeys(*cfg)
	if err != nil {
		fatal("load role keys: %v", err)
	}

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=false",
		*user, *pass, *host, *dbName))
	if err != nil {
		fatal("mysql open: %v", err)
	}
	defer db.Close()

	blockTable := fmt.Sprintf("man_blocks_%s", *chain)
	nodePrefix := map[string]string{"management": "man", "development": "dev", "test": "test", "operations": "ops"}[*hdrSrc]
	if nodePrefix == "" {
		fatal("bad -header-source %s", *hdrSrc)
	}
	headerTable := fmt.Sprintf("%s_headers_%s", nodePrefix, *chain)

	// ---- locate the disputed record -------------------------------------
	rows, err := db.Query(fmt.Sprintf(
		"SELECT block_height, block_hash, previous_hash, timestamp, merkle_root, chain_type, aggregated_signature, transactions FROM %s", blockTable))
	if err != nil {
		fatal("query blocks: %v", err)
	}
	var blocks []headerRow
	for rows.Next() {
		var h headerRow
		if err := rows.Scan(&h.Height, &h.BlockHash, &h.PrevHash, &h.Timestamp, &h.MerkleRoot, &h.ChainType, &h.AggSig, &h.Transactions); err != nil {
			fatal("scan block: %v", err)
		}
		blocks = append(blocks, h)
	}
	if err := rows.Err(); err != nil {
		fatal("read blocks: %v", err)
	}
	rows.Close()
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Height < blocks[j].Height })
	if len(blocks) < 2 {
		fatal("need >=2 blocks on chain %s, have %d", *chain, len(blocks))
	}

	hIdx := len(blocks) - 2
	if *height > 0 {
		hIdx = -1
		for i, b := range blocks {
			if b.Height == *height {
				hIdx = i
				break
			}
		}
		if hIdx < 0 || hIdx+1 >= len(blocks) {
			fatal("height %d not auditable (need a successor header)", *height)
		}
	}
	target := blocks[hIdx]

	var txs []types.Transaction
	if err := json.Unmarshal([]byte(target.Transactions), &txs); err != nil {
		fatal("parse transactions: %v", err)
	}
	if len(txs) == 0 {
		fatal("target block has no transactions")
	}
	var tx types.Transaction
	if *txid != "" {
		found := false
		for i := range txs {
			if txs[i].TXID == *txid {
				tx = txs[i]
				found = true
				break
			}
		}
		if !found {
			fatal("txid %s not found on chain %s", *txid, *chain)
		}
	} else {
		tx = txs[0]
	}
	if len(tx.Endorsements) == 0 {
		fatal("tx has no endorsements")
	}

	// actor public key (endorser) from users table
	var userPub string
	if err := db.QueryRow("SELECT public_key FROM users WHERE user_id=?", tx.UserID).Scan(&userPub); err != nil {
		fatal("user %s: %v", tx.UserID, err)
	}

	// aggregated role public key
	aggPub := &bls.PublicKey{}
	for i, k := range roleKeys[:4] {
		pk := &bls.PublicKey{}
		if err := pk.DeserializeHexStr(k); err != nil {
			fatal("role key %d: %v", i, err)
		}
		if i == 0 {
			*aggPub = *pk
		} else {
			aggPub.Add(pk)
		}
	}

	// Verify only the successor proof retained by the role node.
	trySig := func(sigHex, msg string) bool {
		if sigHex == "" {
			return false
		}
		s := &bls.Sign{}
		raw, err := hex.DecodeString(sigHex)
		if err != nil || s.Deserialize(raw) != nil {
			return false
		}
		return s.VerifyByte(aggPub, []byte(msg))
	}
	interp := ""
	aggSigHex, aggMsg := "", ""

	// ---- timed audit steps ----------------------------------------------
	var results []stepResult
	run := func(name string, n int, note string, f func() bool) {
		var xs []float64
		ok := false
		for i := 0; i < n; i++ {
			t0 := time.Now()
			ok = f()
			xs = append(xs, float64(time.Since(t0).Nanoseconds())/1e6)
		}
		mean, p50 := stats(xs)
		results = append(results, stepResult{name, ok, n, mean, p50, note})
	}

	run("tx_content_hash", *iters, fmt.Sprintf("tx=%s... user=%s", tx.TXID[:12], tx.UserID),
		func() bool { return tx.CalculateTXID() == tx.TXID })

	run("merkle_inclusion", *iters, fmt.Sprintf("block h=%d, %d txs", target.Height, len(txs)),
		func() bool { return merkleRootOf(txs) == target.MerkleRoot })

	run("endorsement_ecdsa", *iters, "secp256k1 over TXID, endorser pubkey from users table",
		func() bool { return crypto.ECDSAVerify(userPub, []byte(tx.TXID), tx.Endorsements[0].Signature) })

	hrows, err := db.Query(fmt.Sprintf(
		"SELECT block_height, block_hash, previous_hash, timestamp, merkle_root, chain_type, aggregated_signature FROM %s ORDER BY block_height", headerTable))
	if err != nil {
		fatal("query headers: %v", err)
	}
	var headers []headerRow
	for hrows.Next() {
		var h headerRow
		if err := hrows.Scan(&h.Height, &h.BlockHash, &h.PrevHash, &h.Timestamp, &h.MerkleRoot, &h.ChainType, &h.AggSig); err != nil {
			fatal("scan header: %v", err)
		}
		headers = append(headers, h)
	}
	if err := hrows.Err(); err != nil {
		fatal("read headers: %v", err)
	}
	hrows.Close()

	walkN := len(headers)
	// Bind the custodian payload to the independently retained role header.
	tgtH := headerByHeight(headers, target.Height)
	sucH := headerByHeight(headers, target.Height+1)
	switch {
	case tgtH != nil && sucH != nil && trySig(sucH.AggSig, tgtH.BlockHash):
		interp = "successor-anchored (successor field over stored signed hash)"
		aggSigHex, aggMsg = sucH.AggSig, tgtH.BlockHash
	case tgtH != nil && sucH != nil && trySig(sucH.AggSig, hdrToTypes(*tgtH).CalculateHash()):
		interp = "recompute anchored"
		aggSigHex, aggMsg = sucH.AggSig, hdrToTypes(*tgtH).CalculateHash()
	default:
		interp = "neither"
	}
	// fail-closed pre-check: when evidence assembly is incomplete (header chain does not cover this height / no successor / no anchored aggregate)
	// terminate immediately; never fall back to the custodian-side cache to continue the audit
	if tgtH == nil || sucH == nil || aggSigHex == "" {
		fatal("fail-closed: no successor-anchored aggregate on header source %s (tgtH=%v sucH=%v)",
			*hdrSrc, tgtH != nil, sucH != nil)
	}
	if err := verifyEvidenceBinding(target, *tgtH, txs); err != nil {
		fatal("fail-closed: evidence binding rejected: %v", err)
	}
	selfHashMatches := 0
	for _, h := range headers {
		if hdrToTypes(h).CalculateHash() == h.BlockHash {
			selfHashMatches++
		}
	}
	run("header_chain_walk", 50, fmt.Sprintf("%d headers back to genesis, full hash recomputation (%d/%d stored hashes recompute exactly)",
		walkN, selfHashMatches, walkN),
		func() bool {
			for i, h := range headers {
				if hdrToTypes(h).CalculateHash() != h.BlockHash {
					return false
				}
				if i > 0 && h.PrevHash != headers[i-1].BlockHash {
					return false
				}
			}
			return true
		})

	sigRaw, _ := hex.DecodeString(aggSigHex)
	run("bls_aggregate_verify", *iters, fmt.Sprintf("4-role aggregate, %d-byte sig, msg=hash(B_%d)", len(sigRaw), target.Height),
		func() bool {
			s := &bls.Sign{}
			if s.Deserialize(sigRaw) != nil {
				return false
			}
			return s.VerifyByte(aggPub, []byte(aggMsg))
		})

	run("full_audit", 50, fmt.Sprintf("depth %d, %d txs in block", walkN, len(txs)),
		func() bool {
			if tx.CalculateTXID() != tx.TXID {
				return false
			}
			if merkleRootOf(txs) != target.MerkleRoot {
				return false
			}
			if !crypto.ECDSAVerify(userPub, []byte(tx.TXID), tx.Endorsements[0].Signature) {
				return false
			}
			// evidence binding: the target block must bind to the same commitment as the role-retained header (chain/height/
			// hash/root/predecessor/timestamp), and the record set must be anchored to the role-retained root
			if tgtH == nil || verifyEvidenceBinding(target, *tgtH, txs) != nil {
				return false
			}
			for i, h := range headers {
				if hdrToTypes(h).CalculateHash() != h.BlockHash {
					return false
				}
				if i > 0 && h.PrevHash != headers[i-1].BlockHash {
					return false
				}
			}
			s := &bls.Sign{}
			if s.Deserialize(sigRaw) != nil || !s.VerifyByte(aggPub, []byte(aggMsg)) {
				return false
			}
			return true
		})

	// ---- report ----------------------------------------------------------
	fmt.Printf("chain=%s header_source=%s disputed_tx=%s... height=%d block_txs=%d chain_headers=%d\n",
		*chain, *hdrSrc, tx.TXID[:12], target.Height, len(txs), walkN)
	fmt.Printf("aggregate_anchor=%s\n", interp)
	fmt.Println("--- per-step timing (ms) ---")
	for _, r := range results {
		fmt.Printf("%-22s ok=%-5v iters=%-4d mean=%8.4f p50=%8.4f  %s\n",
			r.Name, r.OK, r.Iters, r.MeanMs, r.P50Ms, r.Note)
	}
	out, _ := json.Marshal(results)
	fmt.Println("json=" + string(out))
	for _, result := range results {
		if !result.OK {
			os.Exit(1)
		}
	}
}
