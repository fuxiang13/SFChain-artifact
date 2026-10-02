package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sfchain/internal/core"
	"sfchain/internal/database"
	"sfchain/internal/database/mysql"
	"sfchain/pkg/config"
	"strconv"
	"syscall"
	"time"
)

func main() {
	// command-line flags
	interval := flag.Duration("interval", 10*time.Second, "endorsement processing interval")
	maxWorkers := flag.Int("max-workers", 5, "maximum worker count")

	flag.Parse()

	// database configuration: defaults match the WSL deployment (fx@localhost); containerized deployments override via SF_DB_* environment variables
	envOr := func(k, d string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return d
	}
	dbPort := 3306
	if v, err := strconv.Atoi(envOr("SF_DB_PORT", "3306")); err == nil && v > 0 {
		dbPort = v
	}
	dbHost := envOr("SF_DB_HOST", "localhost")
	dbUser := envOr("SF_DB_USER", "fx")
	dbPass := envOr("SF_DB_PASSWORD", "123456")
	dbName := envOr("SF_DB_NAME", "sfchain")

	// configuration
	userConfig := &config.DatabaseConfig{
		Type:     "mysql",
		Host:     dbHost,
		Port:     dbPort,
		User:     dbUser,
		Password: dbPass,
		DBName:   dbName,
		Charset:  "utf8mb4",
	}

	txPoolConfig := &config.DatabaseConfig{
		Type:     "mysql",
		Host:     dbHost,
		Port:     dbPort,
		User:     dbUser,
		Password: dbPass,
		DBName:   dbName,
		Charset:  "utf8mb4",
	}

	var userDB database.UserDatabaseInterface
	if err := core.InitGlobalUserDatabaseManager(userConfig); err != nil {
		log.Printf("warning: failed to initialize global user database manager: %v", err)
		log.Println("note: will try to create a standalone user database connection")
		// try to create a standalone user database connection
		mysqlConfig := mysql.Config{
			Host:     "localhost",
			Port:     3306,
			User:     "fx",
			Password: "123456",
			DBName:   "sfchain",
			Charset:  "utf8mb4",
		}
		userDB, err = mysql.NewUserDatabase(mysqlConfig)
		if err != nil {
			log.Fatalf("failed to create user database: %v", err)
		}
		defer userDB.Close()
	} else {
		defer core.GetGlobalUserDatabaseManager().Close()
		userDB = core.GetGlobalUserDatabaseManager().GetDB()
	}

	var txPoolDB database.TransactionPoolDatabaseInterface
	if err := core.InitGlobalTxPoolManager(txPoolConfig); err != nil {
		log.Printf("warning: failed to initialize global transaction pool manager: %v", err)
		log.Println("note: will use a standalone transaction pool database connection, with a time-slice coordinator to avoid conflicts")
		// do not create a new transaction pool connection; the time-slice coordinator avoids conflicts
	} else {
		defer core.GetGlobalTxPoolManager().Close()
		txPoolDB = core.GetGlobalTxPoolManager().GetDB()
	}

	// print database information
	fmt.Println("database: MySQL")

	// check database connections
	if txPoolDB == nil {
		log.Fatalf("transaction pool database not initialized")
	}

	if userDB == nil {
		log.Fatalf("user database not initialized")
	}

	// create the endorsement service
	endorsementService := core.NewEndorsementService(txPoolDB, userDB)
	endorsementService.Interval = *interval
	endorsementService.MaxWorkers = *maxWorkers
	if v := os.Getenv("SF_ENDORSE_DIRECT"); v != "" {
		endorsementService.DirectPushURL = v + "/api/tx/endorsed"
		fmt.Printf("in-memory endorsement mode: signature results pushed directly to %s (not persisted)\n", endorsementService.DirectPushURL)
	}

	fmt.Println("=== endorsement service starting ===")
	fmt.Printf("processing interval: %v\n", *interval)
	fmt.Printf("max workers: %d\n", *maxWorkers)

	// start the endorsement service
	endorsementService.Start()

	// wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	fmt.Println("\nstop signal received, shutting down service...")
	endorsementService.Stop()
	fmt.Println("endorsement service stopped")
}
