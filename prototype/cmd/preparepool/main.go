// preparepool stages fresh, real-key, pre-endorsed records for the RQ2/RQ3 workloads and RQ4 audit input.
// Preparation is explicitly outside the measured post-admission window.
package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"sfchain/pkg/crypto"
	"sfchain/pkg/types"
)

func main() {
	dsn := flag.String("dsn", "root:qwer@123@tcp(127.0.0.1:3306)/sfchain", "isolated experiment DSN")
	total := flag.Int("total", 4*100*10, "total pre-endorsed records, divisible by four")
	flag.Parse()
	if *total <= 0 || *total%4 != 0 {
		log.Fatal("total must be positive and divisible by four")
	}
	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	roles := []string{"manager", "developer", "tester", "operator"}
	type key struct{ id, priv, pub string }
	keys := make([]key, 4)
	for i, role := range roles {
		if err := db.QueryRow("SELECT user_id,private_key,public_key FROM users WHERE role=? AND private_key IS NOT NULL LIMIT 1", role).Scan(&keys[i].id, &keys[i].priv, &keys[i].pub); err != nil {
			log.Fatal(err)
		}
	}
	for _, table := range []string{"software_factory_logs", "transactions", "processed_transactions"} {
		if _, err := db.Exec("TRUNCATE " + table); err != nil {
			log.Fatal(err)
		}
	}
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := txn.Prepare("INSERT INTO transactions(tx_id,data,user_id,management_signature,user_signature,type,status,created_at,updated_at) VALUES (?,?,?,?,?,?,'endorsed',NOW(3),NOW(3))")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	prefix := fmt.Sprintf("evidence-%d", time.Now().UnixNano())
	for i := 0; i < *total; i++ {
		k := keys[i%4]
		tx := types.Transaction{UserID: k.id, TxType: types.TransactionType(i%4 + 1), Endorsed: true,
			Timestamp: float64(time.Now().UnixNano()) / 1e6,
			LogData:   map[string]interface{}{"id": fmt.Sprintf("%s-%d", prefix, i), "user_id": k.id, "category": types.TransactionType(i%4 + 1).String(), "message": "Synthetic software-factory execution evidence", "operation": "build-test-deploy", "status": "done", "sequence": i}}
		tx.TXID = tx.CalculateTXID()
		sig := crypto.ECDSASign(k.priv, []byte(tx.TXID))
		if sig == "" {
			log.Fatal("ECDSA signing failed")
		}
		tx.Endorsements = []types.Endorsement{{UserID: k.id, PublicKey: k.pub, Signature: sig, Role: roles[i%4], Timestamp: tx.Timestamp}}
		b, err := json.Marshal(tx)
		if err != nil {
			log.Fatal(err)
		}
		if _, err = stmt.Exec(tx.TXID, string(b), tx.UserID, "", sig, tx.TxType.String()); err != nil {
			log.Fatal(err)
		}
	}
	if err := txn.Commit(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Staged %d pre-endorsed records; not an ingestion benchmark\n", *total)
}
