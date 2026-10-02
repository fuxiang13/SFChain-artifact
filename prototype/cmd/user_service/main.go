package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sfchain/internal/core"
	"sfchain/internal/database"
	"sfchain/pkg/config"
	"sfchain/pkg/crypto"
	"syscall"
	"time"
)

type UserRole string

const (
	RoleManager   UserRole = "manager"
	RoleDeveloper UserRole = "developer"
	RoleTester    UserRole = "tester"
	RoleOperator  UserRole = "operator"
)

func main() {
	// Command-line flags
	generateDemo := flag.Bool("generate-demo", true, "generate demo users")
	userCount := flag.Int("user-count", 20, "number of users to generate")

	flag.Parse()

	// Try to initialize the global user database manager
	userConfig := &config.DatabaseConfig{
		Type:     "mysql",
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "qwer@123",
		DBName:   "sfchain",
		Charset:  "utf8mb4",
	}
	if err := core.InitGlobalUserDatabaseManager(userConfig); err != nil {
		log.Fatalf("Failed to initialize global user database manager: %v", err)
	}
	defer core.GetGlobalUserDatabaseManager().Close()

	// Get the user database instance
	userDB := core.GetGlobalUserDatabaseManager().GetDB()

	// Print database information
	fmt.Println("User database: MySQL")

	// Generate demo users if requested
	if *generateDemo {
		generateDemoUsers(userDB, *userCount)
	}

	// Show user statistics
	showUserStats(userDB)

	fmt.Println("=== User Management Service ===")
	fmt.Println("Service started; manage users via the API or directly in the database")
	fmt.Println("Press Ctrl+C to stop the service...")

	// Wait for an interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	fmt.Println("\nStop signal received, stopping the service...")
	fmt.Println("User management service stopped")
}

// generateDemoUsers generates demo users
func generateDemoUsers(userDB database.UserDatabaseInterface, count int) {
	// Check whether users already exist

	// Role list
	roles := []UserRole{RoleManager, RoleDeveloper, RoleTester, RoleOperator}

	// Department mapping
	departments := map[UserRole]string{
		RoleManager:   "Management",
		RoleDeveloper: "Development",
		RoleTester:    "Testing",
		RoleOperator:  "Operations",
	}

	// Name prefixes
	namePrefixes := []string{"Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"}

	// Name suffixes
	nameSuffixes := map[UserRole][]string{
		RoleManager:   {"Smith", "Jones", "Brown", "Davis"},
		RoleDeveloper: {"Miller", "Wilson", "Moore", "Taylor"},
		RoleTester:    {"Anderson", "Thomas", "Jackson", "White"},
		RoleOperator:  {"Harris", "Martin", "Thompson", "Garcia"},
	}

	// Generate the requested number of users
	var allUsers []map[string]interface{}
	for i := 0; i < count; i++ {
		// Pick a role
		role := roles[i%len(roles)]

		// Generate the user ID
		userID := fmt.Sprintf("%s%d", role, i+1)

		// Generate the user name
		prefixIndex := i % len(namePrefixes)
		suffixIndex := i % len(nameSuffixes[role])
		userName := namePrefixes[prefixIndex] + nameSuffixes[role][suffixIndex]

		// Create the user
		user := map[string]interface{}{
			"user_id":    userID,
			"user_name":  userName,
			"role":       string(role),
			"email":      fmt.Sprintf("%s@example.invalid", userID),
			"department": departments[role],
			"created_at": time.Now(),
			"is_active":  true,
		}

		allUsers = append(allUsers, user)
	}

	for _, user := range allUsers {
		// Generate an ECDSA key pair (secp256k1, used for transaction-level endorsement)
		privHex, pubHex, err := crypto.ECDSAGenerateKeyPair()
		if err != nil {
			log.Printf("Failed to generate key pair: %v", err)
			continue
		}

		user["public_key"] = pubHex
		user["private_key"] = privHex

		if err := userDB.AddUser(user); err != nil {
			log.Printf("Failed to add user %s: %v", user["user_id"], err)
		} else {
			fmt.Printf("Added user: %s (%s)\n", user["user_name"], user["role"])
		}
	}

	fmt.Printf("Generated %d demo users\n", len(allUsers))
}

// showUserStats shows user statistics
func showUserStats(userDB database.UserDatabaseInterface) {
	users, err := userDB.GetAllUsers()
	if err != nil {
		log.Printf("Failed to get user list: %v", err)
		return
	}

	fmt.Println("\n=== User Statistics ===")
	fmt.Printf("Total users: %d\n", len(users))

	roleCount := make(map[string]int)
	for _, userInterface := range users {
		if user, ok := userInterface.(map[string]interface{}); ok {
			if role, ok := user["role"].(string); ok {
				roleCount[role]++
			}
		}
	}

	fmt.Println("Distribution by role:")
	fmt.Printf("  management: %d\n", roleCount["manager"])
	fmt.Printf("  development: %d\n", roleCount["developer"])
	fmt.Printf("  testing: %d\n", roleCount["tester"])
	fmt.Printf("  operations: %d\n", roleCount["operator"])
}
