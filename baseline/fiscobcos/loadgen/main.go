package main

// fisco-loadgen deploys the RQ1 contract and ingests staged factory logs.
// Measurement: submitted_ms to successful receipt observation.

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/FISCO-BCOS/go-sdk/v3/abi"
	"github.com/FISCO-BCOS/go-sdk/v3/client"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
)

// envOr lets environment variables override built-in defaults (command-line
// flags in turn override environment variables), so docker-compose can inject
// defaults such as in-container node addresses
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDurationOr(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func main() {
	action := flag.String("action", "ingest", "action: deploy | ingest | check")

	host := flag.String("host", envOr("FISCO_HOST", "127.0.0.1"), "node RPC address")
	port := flag.Int("port", envIntOr("FISCO_PORT", 20200), "node RPC port")
	groupID := flag.String("group", envOr("FISCO_GROUP", "group0"), "group ID")
	certsDir := flag.String("certs", envOr("FISCO_CERTS", "./conf"), "SDK certificate directory (with ca.crt/sdk.crt/sdk.key)")
	skHex := flag.String("key", "145e247e170ba3afd6ae97e88f00dbc976c2345d511b0f6713355d19d8b80b58",
		"signing account private key hex (default is the go-sdk example test account; no change needed for benchmarks)")
	addressHex := flag.String("address", envOr("FISCO_ADDRESS", ""), "contract address (for ingest/check; if empty, read from results/contract_address)")
	outDir := flag.String("out", envOr("FISCO_OUT", "./results"), "result output directory")

	// ingest mode parameters (defaults = SFChain LogProcessor built-ins, keeping the same cadence)
	poll := flag.Duration("poll", 5*time.Second, "ingest: polling interval (SFChain LogProcessor interval=5s)")
	threshold := flag.Int("threshold", 10, "ingest: max rows consumed per category per round (SFChain txThreshold=10)")
	pacing := flag.Duration("pacing", 100*time.Millisecond, "ingest: interval between consecutive submissions (SFChain creates one tx every 100ms)")
	endorseInterval := flag.Duration("endorse-interval", envDurationOr("FISCO_ENDORSE_INTERVAL", 0), "ingest: endorsement-stage interval (mirrors SFChain endorsement_service -interval), 0 = no separate endorsement stage")
	endorsePersist := flag.Bool("endorse-persist", envIntOr("FISCO_ENDORSE_PERSIST", 0) != 0, "ingest: persist signatures to MySQL (fisco.endorsed_txs) and read them back for submission (aligns with SFChain endorsement persistence); requires -endorse-interval")
	endorseEvidence := flag.Bool("endorse-evidence", envIntOr("FISCO_ENDORSE_EVIDENCE", 0) != 0, "ingest: build SFChain-isomorphic endorsement evidence (reads sfchain.users, ECDSA-signs the log content hash) and stores it on-chain with the tx (requires the contract endorsement field)")
	endorseCheck := flag.Bool("endorse-check", envIntOr("FISCO_ENDORSE_CHECK", 0) != 0, "ingest: build endorsement evidence and call recordLogChecked (contract enforces per-signature ecrecover verification before storing; semantic-level admission check)")
	idleExit := flag.Duration("idle", 30*time.Second, "ingest: how long to wait before exiting when nothing is pending or in flight")
	maxDuration := flag.Duration("duration", 0, "ingest: hard run duration cap, 0 = unlimited")
	mysqlHost := flag.String("mysql-host", envOr("FISCO_MYSQL_HOST", "127.0.0.1"), "ingest: MySQL host")
	mysqlPort := flag.Int("mysql-port", envIntOr("FISCO_MYSQL_PORT", 3306), "ingest: MySQL port")
	mysqlUser := flag.String("mysql-user", envOr("FISCO_MYSQL_USER", "root"), "ingest: MySQL user")
	mysqlPass := flag.String("mysql-password", envOr("FISCO_MYSQL_PASSWORD", "qwer@123"), "ingest: MySQL password")
	mysqlDB := flag.String("mysql-db", envOr("FISCO_MYSQL_DB", "sfchain"), "ingest: MySQL database")
	flag.Parse()

	parsed, err := abi.JSON(strings.NewReader(SoftwareFactoryLogsABI))
	if err != nil {
		log.Fatalf("failed to parse contract ABI: %v", err)
	}

	privateKey, err := hex.DecodeString(strings.TrimPrefix(*skHex, "0x"))
	if err != nil || len(privateKey) == 0 {
		log.Fatalf("failed to decode private key hex: %v", err)
	}

	config := &client.Config{
		IsSMCrypto:  false,
		GroupID:     *groupID,
		DisableSsl:  false, // node enables RPC SSL by default; connect with the sdk certificates
		PrivateKey:  privateKey,
		Host:        *host,
		Port:        *port,
		TLSCaFile:   filepath.Join(*certsDir, "ca.crt"),
		TLSKeyFile:  filepath.Join(*certsDir, "sdk.key"),
		TLSCertFile: filepath.Join(*certsDir, "sdk.crt"),
	}

	ctx := context.Background()
	cl, err := client.DialContext(ctx, config)
	if err != nil {
		log.Fatalf("failed to connect to node %s:%d: %v", *host, *port, err)
	}
	defer cl.Close()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("failed to create output directory: %v", err)
	}

	switch *action {
	case "deploy":
		runDeploy(ctx, cl, parsed, *outDir)
	case "ingest":
		addr := resolveAddress(*addressHex, *outDir)
		if addr == "" {
			log.Fatalf("contract address not specified: run -action deploy first, or pass -address 0x..")
		}
		runIngest(ctx, cl, parsed, common.HexToAddress(addr), ingestConfig{
			poll: *poll, threshold: *threshold, pacing: *pacing,
			endorseInterval: *endorseInterval,
			endorsePersist:  *endorsePersist,
			endorseEvidence: *endorseEvidence,
			endorseCheck:    *endorseCheck,
			idle:            *idleExit, duration: *maxDuration, outDir: *outDir,
			mysqlHost: *mysqlHost, mysqlPort: *mysqlPort,
			mysqlUser: *mysqlUser, mysqlPassword: *mysqlPass, mysqlDB: *mysqlDB,
		})
	case "check":
		addr := resolveAddress(*addressHex, *outDir)
		if addr == "" {
			log.Fatalf("contract address not specified: run -action deploy first, or pass -address 0x..")
		}
		runCheck(ctx, cl, parsed, common.HexToAddress(addr))
	default:
		log.Fatalf("unknown action: %s (valid: deploy/ingest/check)", *action)
	}
}

// ---------- deploy ----------

func runDeploy(ctx context.Context, cl *client.Client, parsed abi.ABI, outDir string) {
	height, err := cl.GetBlockNumber(ctx)
	if err != nil {
		log.Fatalf("GetBlockNumber failed: %v", err)
	}
	input := common.FromHex(SoftwareFactoryLogsBin)
	txData, txHash, err := cl.CreateEncodedTransactionDataV1(nil, input, height+500, SoftwareFactoryLogsABI)
	if err != nil {
		log.Fatalf("failed to build deploy transaction: %v", err)
	}
	sig, err := cl.CreateEncodedSignature(txHash)
	if err != nil {
		log.Fatalf("signing failed: %v", err)
	}
	tx, err := cl.CreateEncodedTransaction(txData, txHash, sig, 0, "")
	if err != nil {
		log.Fatalf("failed to assemble transaction: %v", err)
	}
	log.Printf("deploy transaction hash: %x", txHash)
	receipt, err := cl.SendEncodedTransaction(ctx, tx, true)
	if err != nil {
		log.Fatalf("failed to send deploy transaction: %v", err)
	}
	if receipt.Status != 0 {
		log.Fatalf("deployment failed status=%d message=%s", receipt.Status, receipt.Message)
	}
	addr := receipt.ContractAddress
	path := filepath.Join(outDir, "contract_address")
	if err := os.WriteFile(path, []byte(addr), 0o644); err != nil {
		log.Fatalf("failed to write contract address: %v", err)
	}
	fmt.Printf("contract deployed\n  address: %s\n  block: %d\n  saved: %s\n", addr, receipt.BlockNumber, path)
}

func latStats(v []float64) map[string]float64 {
	sort.Float64s(v)
	pct := func(p float64) float64 {
		idx := p / 100 * float64(len(v)-1)
		return v[int(idx)]
	}
	var sum float64
	for _, x := range v {
		sum += x
	}
	return map[string]float64{
		"min": round(v[0]), "avg": round(sum / float64(len(v))),
		"p50": round(pct(50)), "p90": round(pct(90)), "p95": round(pct(95)),
		"p99": round(pct(99)), "max": round(v[len(v)-1]),
	}
}

func round(x float64) float64 { return float64(int(x*1000+0.5)) / 1000 }

// ---------- check ----------

func runCheck(ctx context.Context, cl *client.Client, parsed abi.ABI, addr common.Address) {
	data, err := parsed.Pack("getRecordCount")
	if err != nil {
		log.Fatalf("Pack getRecordCount failed: %v", err)
	}
	out, err := cl.CallContract(ctx, ethereum.CallMsg{To: &addr, Data: data})
	if err != nil {
		log.Fatalf("CallContract failed: %v", err)
	}
	// getRecordCount returns a single uint256 (anonymous output; go-sdk's
	// name-based Unpack does not support it, so decode directly per the ABI
	// encoding rules: a 32-byte big-endian integer
	if len(out) != 32 {
		log.Fatalf("unexpected return data length: %d bytes (expected 32)", len(out))
	}
	count := new(big.Int).SetBytes(out)
	fmt.Printf("contract %s total records: %v\n", addr.Hex(), count)
}

func resolveAddress(arg, outDir string) string {
	if arg != "" {
		return arg
	}
	b, err := os.ReadFile(filepath.Join(outDir, "contract_address"))
	if err == nil {
		return strings.TrimSpace(string(b))
	}
	return ""
}

func statusMessage(status int) string {
	if status == 0 {
		return ""
	}
	return fmt.Sprintf("receipt status=%d", status)
}
