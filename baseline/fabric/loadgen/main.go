package main

// Fabric load generator: submits N PutLog transactions through the gateway of
// peer0.org1 with bounded concurrency and records submit->commit latency per
// transaction plus batch throughput. Uses the paper's worker-submit-to-commit boundary.

import (
	"crypto/ecdsa"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/hyperledger/fabric-gateway/pkg/client"
	"github.com/hyperledger/fabric-gateway/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type result struct {
	ThroughputTPS float64 `json:"throughput_tps"`
	Count         int     `json:"count"`
	Fail          int     `json:"fail"`
	P50Ms         float64 `json:"p50_ms"`
	P90Ms         float64 `json:"p90_ms"`
	P95Ms         float64 `json:"p95_ms"`
	P99Ms         float64 `json:"p99_ms"`
	AvgMs         float64 `json:"avg_ms"`
}

// job is one workload record: the log id, the on-chain payload, and its category.
type job struct {
	id, payload, category string
}

// loadLogsFromMySQL reads the staged factory logs (the same software_factory_logs
// table the other platforms consume) and renders each record as the on-chain payload.
func loadLogsFromMySQL() []job {
	dsn := envOr("MYSQL_DSN", "root:qwer@123@tcp(host.docker.internal:13306)/sfchain?parseTime=true")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		panic(fmt.Sprintf("mysql ping failed: %v", err))
	}
	rows, err := db.Query(`SELECT id, category, timestamp, level, message, user_id, module, project, operation, status
		FROM software_factory_logs ORDER BY timestamp ASC, id ASC`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	var out []job
	for rows.Next() {
		var id, category, level, message, userID, module, project, operation, status string
		var ts float64
		var userIDN, moduleN, projectN, operationN, statusN sql.NullString
		if err := rows.Scan(&id, &category, &ts, &level, &message, &userIDN, &moduleN, &projectN, &operationN, &statusN); err != nil {
			panic(err)
		}
		userID, module, project, operation, status = userIDN.String, moduleN.String, projectN.String, operationN.String, statusN.String
		payload, _ := json.Marshal(map[string]interface{}{
			"log_id": id, "category": category, "timestamp": ts, "level": level,
			"message": message, "user_id": userID, "module": module,
			"project": project, "operation": operation, "status": status,
		})
		out = append(out, job{id: id, payload: string(payload), category: category})
	}
	return out
}

func readFirst(dir string) []byte {
	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			b, err := os.ReadFile(filepath.Join(dir, e.Name()))
			if err != nil {
				panic(err)
			}
			return b
		}
	}
	panic("no file in " + dir)
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)) * p / 100.0)
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func atoiOr(s string, d int) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return d
		}
		n = n*10 + int(c-'0')
	}
	if n == 0 {
		return d
	}
	return n
}

// orgRoute maps a log category to (org, chaincode name) so that each
// category is submitted through its responsible org's gateway to a
// chaincode whose endorsement policy requires exactly that org's peer.
var orgRoute = map[string][2]string{
	"management":  {"org1", "sflogs-m"},
	"development": {"org2", "sflogs-d"},
	"test":        {"org3", "sflogs-t"},
	"operations":  {"org4", "sflogs-o"},
}

// newOrgContract creates a gateway contract for the given org (org1..org4),
// using that org's User1 credentials and peer0 TLS cert.
func newOrgContract(org string, channel, ccName string) (*client.Contract, func()) {
	cryptoBase := "/etc/hyperledger/net/organizations/peerOrganizations/" + org + ".example.com"
	mspID := strings.ToUpper(org[:1]) + org[1:] + "MSP" // "org1" → "Org1MSP"
	peerHost := "peer0." + org + ".example.com:7051"
	userDir := filepath.Join(cryptoBase, "users", "User1@"+org+".example.com", "msp")

	certPEM := readFirst(filepath.Join(userDir, "signcerts"))
	keyPEM := readFirst(filepath.Join(userDir, "keystore"))
	tlsPEM := readFirst(filepath.Join(cryptoBase, "peers", "peer0."+org+".example.com", "tls"))

	x5cert, err := identity.CertificateFromPEM(certPEM)
	if err != nil {
		panic(fmt.Sprintf("%s cert: %v", org, err))
	}
	id, err := identity.NewX509Identity(mspID, x5cert)
	if err != nil {
		panic(err)
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		panic("bad key pem for " + org)
	}
	priv, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		panic(err)
	}
	ecKey, ok := priv.(*ecdsa.PrivateKey)
	if !ok {
		panic("not an ecdsa key for " + org)
	}
	sign, err := identity.NewPrivateKeySign(ecKey)
	if err != nil {
		panic(err)
	}

	certPool := x509.NewCertPool()
	tlsCert, _ := pem.Decode(tlsPEM)
	if tlsCert == nil {
		panic("bad tls pem for " + org)
	}
	x5, err := x509.ParseCertificate(tlsCert.Bytes)
	if err != nil {
		panic(err)
	}
	certPool.AddCert(x5)
	creds := credentials.NewClientTLSFromCert(certPool, "peer0."+org+".example.com")

	conn, err := grpc.NewClient(peerHost, grpc.WithTransportCredentials(creds))
	if err != nil {
		panic(err)
	}
	gw, err := client.Connect(id, client.WithSign(sign), client.WithClientConnection(conn))
	if err != nil {
		panic(err)
	}
	closer := func() { gw.Close(); conn.Close() }
	return gw.GetNetwork(channel).GetContract(ccName), closer
}

func main() {
	channel := envOr("CHANNEL", "sfchannel")
	total := atoiOr(envOr("TOTAL", "20000"), 20000)
	conc := atoiOr(envOr("CONC", "8"), 8)
	out := envOr("OUT", "/work/results/fabric_run.json")

	// Create one contract per (org, chaincode) pair; each worker routes
	// records to the contract matching the record's category.
	contracts := make(map[string]*client.Contract)
	var closers []func()
	for cat, route := range orgRoute {
		org, ccName := route[0], route[1]
		c, cl := newOrgContract(org, channel, ccName)
		contracts[cat] = c
		closers = append(closers, cl)
	}
	defer func() {
		for _, f := range closers {
			f()
		}
	}()

	// Source preparation is outside the measurement window.
	records := loadLogsFromMySQL()
	if len(records) != total {
		panic(fmt.Sprintf("expected %d source records, got %d", total, len(records)))
	}
	fmt.Printf("loaded %d factory logs from MySQL\n", len(records))

	var mu sync.Mutex
	lat := make([]float64, 0, total)
	fail := 0
	var firstErrs []string
	var lastCommitMs int64
	var firstSubmitMs int64

	// MODE=closedloop (default): each worker submits then waits for commit before the
	// next submit (at most `conc` transactions in flight - light-load latency probe).
	// MODE=openloop: submit as fast as possible without waiting (SubmitAsync returns
	// right after ordering), collect commit handles, then await all commits - this is
	// the saturation/flash-flood mode comparable to the SFChain/FISCO batch pipelines.
	openLoop := envOr("MODE", "closedloop") == "openloop"

	type pending struct {
		i        int
		submitTs time.Time // when the worker actually starts processing this record
		commit   *client.Commit
	}
	pendCh := make(chan pending, 4096)
	var commitWG sync.WaitGroup

	jobs := make(chan int, total)
	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)

	if openLoop {
		// commit waiter goroutines: drain pending commits, record latency
		for w := 0; w < conc; w++ {
			commitWG.Add(1)
			go func() {
				defer commitWG.Done()
				for p := range pendCh {
					status, err := p.commit.Status()
					if err != nil || !status.Successful {
						mu.Lock()
						fail++
						if len(firstErrs) < 5 {
							firstErrs = append(firstErrs, fmt.Sprintf("commit[%d]: err=%v successful=%v", p.i, err, status != nil && status.Successful))
						}
						mu.Unlock()
						continue
					}
					d := float64(time.Since(p.submitTs).Microseconds()) / 1000.0
					mu.Lock()
					lat = append(lat, d)
					if now := time.Now().UnixMilli(); now > lastCommitMs {
						lastCommitMs = now
					}
					mu.Unlock()
				}
			}()
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				j := records[i]
				// Route to the responsible org's contract (role-specific endorsement)
				cc := contracts[j.category]
				if cc == nil {
					panic("unsupported source-log category: " + j.category)
				}
				submitTs := time.Now()
				_, commit, err := cc.SubmitAsync("PutLog", client.WithArguments(j.id, j.payload))
				if err != nil {
					mu.Lock()
					fail++
					if len(firstErrs) < 5 {
						firstErrs = append(firstErrs, fmt.Sprintf("submit[%d]: %v", i, err))
					}
					mu.Unlock()
					continue
				}
				mu.Lock()
				if firstSubmitMs == 0 || submitTs.UnixMilli() < firstSubmitMs {
					firstSubmitMs = submitTs.UnixMilli()
				}
				mu.Unlock()
				if openLoop {
					pendCh <- pending{i: i, submitTs: submitTs, commit: commit}
					continue
				}
				status, err := commit.Status()
				if err != nil || !status.Successful {
					mu.Lock()
					fail++
					if len(firstErrs) < 5 {
						firstErrs = append(firstErrs, fmt.Sprintf("commit[%d]: err=%v successful=%v", i, err, status != nil && status.Successful))
					}
					mu.Unlock()
					continue
				}
				d := float64(time.Since(submitTs).Microseconds()) / 1000.0
				mu.Lock()
				lat = append(lat, d)
				if now := time.Now().UnixMilli(); now > lastCommitMs {
					lastCommitMs = now
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if openLoop {
		close(pendCh)
		commitWG.Wait()
	}
	// Throughput window: first successful worker submission -> last commit.
	var wall time.Duration
	if firstSubmitMs > 0 && lastCommitMs > firstSubmitMs {
		wall = time.Duration((lastCommitMs - firstSubmitMs) * int64(time.Millisecond))
	} else {
		wall = time.Since(time.UnixMilli(firstSubmitMs))
	}

	sort.Float64s(lat)
	var sum float64
	for _, v := range lat {
		sum += v
	}
	r := result{Count: len(lat), Fail: fail}
	if len(lat) > 0 {
		r.P50Ms = pct(lat, 50)
		r.P90Ms = pct(lat, 90)
		r.P95Ms = pct(lat, 95)
		r.P99Ms = pct(lat, 99)
		r.AvgMs = sum / float64(len(lat))
	}
	r.ThroughputTPS = float64(len(lat)) / wall.Seconds()
	os.MkdirAll(filepath.Dir(out), 0o755)
	b, _ := json.MarshalIndent(r, "", "  ")
	os.WriteFile(out, b, 0o644)
	fmt.Println(string(b))
	fmt.Printf("wall=%.3fs confirmed=%d fail=%d\n", wall.Seconds(), r.Count, fail)
	for _, e := range firstErrs {
		fmt.Println("ERRDETAIL", e)
	}
}
