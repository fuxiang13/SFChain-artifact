package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"sfchain/internal/database"
	"sfchain/internal/database/mysql"
)

// loadgen - SFChain benchmark load generator
// injects software-factory logs into the software_factory_logs table at a controlled rate,
// exercising the full LogProcessor -> transaction pool -> endorsement -> block production pipeline.
//
// Usage:
//
//	./bin/loadgen -total 400 -rate 50        # 400 total, 50 per second
//	./bin/loadgen -total 20000 -rate 0       # 20000 total, full-speed batch writes
func main() {
	total := flag.Int("total", 400, "total number of logs to inject")
	rate := flag.Float64("rate", 0, "injection rate (rows/sec); 0 means full-speed batch writes")
	batch := flag.Int("batch", 200, "rows per batch (effective when rate=0)")
	mysqlUser := flag.String("user", "root", "MySQL user")
	mysqlPass := flag.String("password", "qwer@123", "MySQL password")
	mysqlHost := flag.String("host", "127.0.0.1", "MySQL host")
	mysqlPort := flag.Int("port", 3306, "MySQL port")
	flag.Parse()

	config := mysql.Config{
		Host:     *mysqlHost,
		Port:     *mysqlPort,
		User:     *mysqlUser,
		Password: *mysqlPass,
		DBName:   "sfchain",
		Charset:  "utf8mb4",
	}

	// connect to the user database for real user IDs (endorsement needs real ECDSA keys)
	userDB, err := mysql.NewUserDatabase(config)
	if err != nil {
		log.Fatalf("failed to connect to user database: %v", err)
	}
	defer userDB.Close()

	users, err := userDB.GetAllUsers()
	if err != nil {
		log.Fatalf("failed to get user list: %v", err)
	}
	var userIDs []string
	for _, u := range users {
		if m, ok := u.(map[string]interface{}); ok {
			if id, ok := m["user_id"].(string); ok {
				userIDs = append(userIDs, id)
			}
		}
	}
	if len(userIDs) == 0 {
		log.Fatalf("users table is empty; run user_service -generate-demo first")
	}
	fmt.Printf("using %d real users\n", len(userIDs))

	logDB, err := mysql.NewLogDatabase(config)
	if err != nil {
		log.Fatalf("failed to connect to log database: %v", err)
	}
	defer logDB.Close()

	generator := database.NewLogGenerator()
	generator.SetRealUserIDs(userIDs)

	categories := []database.LogCategory{
		database.CategoryManagement,
		database.CategoryDevelopment,
		database.CategoryTest,
		database.CategoryOperations,
	}

	start := time.Now()
	sent := 0

	if *rate <= 0 {
		// full-speed batch mode
		for sent < *total {
			n := *batch
			if sent+n > *total {
				n = *total - sent
			}
			logs := make([]interface{}, 0, n)
			for i := 0; i < n; i++ {
				logs = append(logs, generator.GenerateLog(categories[sent%4]))
				sent++
			}
			if err := logDB.AddLogs(logs); err != nil {
				log.Fatalf("batch write failed: %v", err)
			}
		}
	} else {
		// rate-limited mode: write rate rows per second
		interval := time.Second / time.Duration(*rate)
		if interval <= 0 {
			interval = time.Millisecond
		}
		logs := make([]interface{}, 0)
		for i := 0; i < *total; i++ {
			logs = append(logs, generator.GenerateLog(categories[i%4]))
			// batch accumulation: flush every second or every 50 rows
			if len(logs) >= 50 || i == *total-1 {
				if err := logDB.AddLogs(logs); err != nil {
					log.Fatalf("write failed: %v", err)
				}
				logs = make([]interface{}, 0, 50)
			}
			if i < *total-1 {
				time.Sleep(interval)
			}
		}
		sent = *total
	}

	elapsed := time.Since(start)
	fmt.Printf("injection done: %d rows / %.2f s (rate %.1f rows/sec)\n",
		sent, elapsed.Seconds(), float64(sent)/elapsed.Seconds())
}
