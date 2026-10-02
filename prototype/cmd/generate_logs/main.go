package main

import (
	"fmt"
	"sfchain/internal/database"
	"sfchain/internal/database/mysql"
)

func main() {
	// Connect to the user database to obtain real user IDs
	mysqlConfig := mysql.Config{
		Host:     "localhost",
		Port:     3306,
		User:     "fx",
		Password: "123456",
		DBName:   "sfchain",
		Charset:  "utf8mb4",
	}

	fmt.Printf("Connecting to user database: %s:%d/%s (user: %s)\n", mysqlConfig.Host, mysqlConfig.Port, mysqlConfig.DBName, mysqlConfig.User)

	userDB, err := mysql.NewUserDatabase(mysqlConfig)
	if err != nil {
		fmt.Printf("Failed to create user database: %v\n", err)
		return
	}
	defer userDB.Close()

	// Fetch all users
	users, err := userDB.GetAllUsers()
	if err != nil {
		fmt.Printf("Failed to get user list: %v\n", err)
		return
	}

	// Extract the list of user IDs
	var userIDs []string
	for _, user := range users {
		if userMap, ok := user.(map[string]interface{}); ok {
			if userID, ok := userMap["user_id"].(string); ok {
				userIDs = append(userIDs, userID)
			}
		}
	}

	fmt.Printf("Fetched %d users from the user database\n", len(userIDs))

	// Connect to the log database
	fmt.Printf("Connecting to log database: %s:%d/%s (user: %s)\n", mysqlConfig.Host, mysqlConfig.Port, mysqlConfig.DBName, mysqlConfig.User)

	db, err := mysql.NewLogDatabase(mysqlConfig)
	if err != nil {
		fmt.Printf("Failed to create log database: %v\n", err)
		return
	}
	defer db.Close()

	// Check the current log count in the database
	stats, err := db.GetStats()
	if err != nil {
		fmt.Printf("Failed to get log stats: %v\n", err)
	} else {
		if statsInterface, ok := stats.(*mysql.LogStats); ok {
			fmt.Printf("Log database currently holds %d logs\n", statsInterface.TotalCount)
		} else {
			fmt.Printf("Unable to parse log stats\n")
		}
	}

	// Generate logs using the log generator
	generator := database.NewLogGenerator()

	// Set the real user IDs
	if len(userIDs) > 0 {
		generator.SetRealUserIDs(userIDs)
		fmt.Printf("Using real user IDs: %v\n", userIDs)
	} else {
		fmt.Println("No real users found, using default user IDs")
	}

	// Generate unprocessed management-category logs
	var logs []interface{}
	for i := 0; i < 50; i++ {
		log := generator.GenerateLog(database.CategoryManagement)
		logs = append(logs, log)
	}

	// Generate unprocessed development-category logs
	for i := 0; i < 50; i++ {
		log := generator.GenerateLog(database.CategoryDevelopment)
		logs = append(logs, log)
	}

	// Generate unprocessed test-category logs
	for i := 0; i < 50; i++ {
		log := generator.GenerateLog(database.CategoryTest)
		logs = append(logs, log)
	}

	// Generate unprocessed operations-category logs
	for i := 0; i < 50; i++ {
		log := generator.GenerateLog(database.CategoryOperations)
		logs = append(logs, log)
	}

	// Add the logs to the database
	fmt.Printf("Adding %d new logs to the database\n", len(logs))
	if err := db.AddLogs(logs); err != nil {
		fmt.Printf("Failed to add logs to the database: %v\n", err)
		return
	}

	// Check the log count again
	stats, err = db.GetStats()
	if err != nil {
		fmt.Printf("Failed to get log stats: %v\n", err)
	} else {
		if statsInterface, ok := stats.(*mysql.LogStats); ok {
			fmt.Printf("Log database holds %d logs after insertion\n", statsInterface.TotalCount)
		} else {
			fmt.Printf("Unable to parse log stats\n")
		}
	}

	fmt.Println("Log generation completed successfully!")
}
