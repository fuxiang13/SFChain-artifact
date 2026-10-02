// seedusers creates disposable benchmark identities with real secp256k1 keys.
// It does not ship private keys or overwrite identities already in the database.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"sfchain/pkg/crypto"
)

func main() {
	dsn := flag.String("dsn", "root:qwer@123@tcp(sf-mysql:3306)/sfchain", "isolated benchmark database DSN")
	flag.Parse()
	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
		user_id VARCHAR(64) PRIMARY KEY, user_name VARCHAR(64) NOT NULL,
		role VARCHAR(32) NOT NULL, public_key VARCHAR(256) NOT NULL,
		private_key VARCHAR(256), email VARCHAR(128) NOT NULL,
		department VARCHAR(64), created_at DATETIME(3) NOT NULL,
		is_active BOOLEAN DEFAULT TRUE, endorsement_count INT DEFAULT 0)`)
	if err != nil {
		log.Fatal(err)
	}
	roles := []struct{ role, prefix, department string }{
		{"manager", "manager", "management"},
		{"developer", "dev", "development"},
		{"tester", "tester", "test"},
		{"operator", "ops", "operations"},
	}
	for _, role := range roles {
		for i := 1; i <= 6; i++ {
			id := fmt.Sprintf("%s%d", role.prefix, i)
			if role.role == "manager" && i == 1 {
				id = "admin" // the successor-trigger driver uses these four aliases
			}
			priv, pub, err := crypto.ECDSAGenerateKeyPair()
			if err != nil {
				log.Fatal(err)
			}
			if _, err = db.Exec(`INSERT IGNORE INTO users
				(user_id,user_name,role,public_key,private_key,email,department,created_at)
				VALUES (?,?,?,?,?,?,?,NOW(3))`,
				id, id, role.role, pub, priv, id+"@example.invalid", role.department); err != nil {
				log.Fatal(err)
			}
		}
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM users WHERE private_key IS NOT NULL").Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Disposable benchmark identities ready: %d\n", n)
}
