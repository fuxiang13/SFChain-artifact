package mysql

import (
	"database/sql"
	"fmt"
	"log"
	"sfchain/pkg/crypto"
	"sync"
	"time"
)

// User is user information
type User struct {
	UserID           string    `json:"user_id"`
	UserName         string    `json:"user_name"`
	Role             string    `json:"role"`
	PublicKey        string    `json:"public_key"`
	PrivateKey       string    `json:"private_key"`
	Email            string    `json:"email"`
	Department       string    `json:"department"`
	CreatedAt        time.Time `json:"created_at"`
	IsActive         bool      `json:"is_active"`
	EndorsementCount int       `json:"endorsement_count"`
}

// UserDatabase is the MySQL implementation of the user database
type UserDatabase struct {
	db    *DB
	mu    sync.RWMutex
	users map[string]*User
}

// NewUserDatabase creates a new MySQL user database
func NewUserDatabase(config Config) (*UserDatabase, error) {
	db, err := NewDB(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create database connection: %w", err)
	}

	udb := &UserDatabase{
		db:    db,
		users: make(map[string]*User),
	}

	// initialize the table schema
	if err := udb.initTables(); err != nil {
		return nil, fmt.Errorf("failed to initialize table schema: %w", err)
	}

	// load all users
	if err := udb.loadAllUsers(); err != nil {
		log.Printf("failed to load user data: %v", err)
	}

	// check whether the users table is empty; if so, generate demo users
	var count int
	err = udb.db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		log.Printf("failed to check user count: %v", err)
	}

	// if the users table is empty, generate demo users
	if count == 0 {
		udb.generateDemoUsers()
	}

	return udb, nil
}

// initTables initializes the table schema
func (udb *UserDatabase) initTables() error {
	// create the users table
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS users (
		user_id VARCHAR(64) PRIMARY KEY,
		user_name VARCHAR(64) NOT NULL,
		role VARCHAR(32) NOT NULL,
		public_key VARCHAR(256) NOT NULL,
		private_key VARCHAR(256),
		email VARCHAR(128) NOT NULL,
		department VARCHAR(64),
		created_at DATETIME(3) NOT NULL,
		is_active BOOLEAN DEFAULT TRUE,
		endorsement_count INT DEFAULT 0
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
	`
	_, err := udb.db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	// create indexes (MySQL does not support CREATE INDEX IF NOT EXISTS; check first)
	indexQueries := []struct {
		name  string
		query string
	}{
		{"idx_users_role", "CREATE INDEX idx_users_role ON users(role)"},
		{"idx_users_public_key", "CREATE INDEX idx_users_public_key ON users(public_key)"},
		{"idx_users_is_active", "CREATE INDEX idx_users_is_active ON users(is_active)"},
	}

	for _, idx := range indexQueries {
		// check whether the index already exists
		var count int
		checkQuery := `
			SELECT COUNT(*) 
			FROM information_schema.statistics 
			WHERE table_schema = DATABASE() 
			AND table_name = 'users' 
			AND index_name = ?`

		err := udb.db.QueryRow(checkQuery, idx.name).Scan(&count)
		if err != nil {
			log.Printf("failed to check index %s: %v", idx.name, err)
			continue
		}

		// if the index does not exist, create it
		if count == 0 {
			if _, err := udb.db.Exec(idx.query); err != nil {
				log.Printf("failed to create index %s: %v", idx.name, err)
			} else {
				log.Printf("index %s created successfully", idx.name)
			}
		}
	}

	log.Printf("user database table schema initialized successfully")
	return nil
}

// Close closes the database
func (udb *UserDatabase) Close() error {
	return udb.db.Close()
}

// AddUser adds a user
func (udb *UserDatabase) AddUser(user interface{}) error {
	u, ok := user.(*User)
	if !ok {
		// try to convert from map[string]interface{}
		if userMap, ok := user.(map[string]interface{}); ok {
			u = &User{}
			if userID, ok := userMap["user_id"].(string); ok {
				u.UserID = userID
			}
			if userName, ok := userMap["user_name"].(string); ok {
				u.UserName = userName
			}
			if role, ok := userMap["role"].(string); ok {
				u.Role = role
			}
			if publicKey, ok := userMap["public_key"].(string); ok {
				u.PublicKey = publicKey
			}
			if privateKey, ok := userMap["private_key"].(string); ok {
				u.PrivateKey = privateKey
			}
			if email, ok := userMap["email"].(string); ok {
				u.Email = email
			}
			if department, ok := userMap["department"].(string); ok {
				u.Department = department
			}
			if isActive, ok := userMap["is_active"].(bool); ok {
				u.IsActive = isActive
			}
			if endorsementCount, ok := userMap["endorsement_count"].(int); ok {
				u.EndorsementCount = endorsementCount
			}
		} else {
			return NewDatabaseError("AddUser", fmt.Errorf("invalid user type"))
		}
	}

	if u.UserID == "" {
		return NewDatabaseError("AddUser", fmt.Errorf("user ID must not be empty"))
	}

	if u.CreatedAt.IsZero() {
		u.CreatedAt = time.Now()
	}

	// insert into the database
	query := `INSERT INTO users 
		(user_id, user_name, role, public_key, private_key, email, department, created_at, is_active, endorsement_count) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := udb.db.Exec(query,
		u.UserID,
		u.UserName,
		u.Role,
		u.PublicKey,
		u.PrivateKey,
		u.Email,
		u.Department,
		u.CreatedAt,
		u.IsActive,
		u.EndorsementCount,
	)
	if err != nil {
		return WrapDatabaseError("AddUser", fmt.Errorf("failed to add user: %w", err))
	}

	// update the in-memory cache
	udb.mu.Lock()
	udb.users[u.UserID] = u
	udb.mu.Unlock()

	log.Printf("user added: %s (%s)", u.UserName, u.UserID)
	return nil
}

// GetUser gets a user
func (udb *UserDatabase) GetUser(id string) (interface{}, error) {
	udb.mu.RLock()
	if user, exists := udb.users[id]; exists {
		udb.mu.RUnlock()
		return user, nil
	}
	udb.mu.RUnlock()

	// query the database
	query := `SELECT user_id, user_name, role, public_key, private_key, email, department, created_at, is_active, endorsement_count 
		FROM users 
		WHERE user_id = ?`
	row := udb.db.QueryRow(query, id)

	user, err := udb.scanUser(row)
	if err != nil {
		return nil, err
	}

	// update the in-memory cache
	udb.mu.Lock()
	udb.users[user.UserID] = user
	udb.mu.Unlock()

	return user, nil
}

// GetUserByPublicKey gets a user by public key
func (udb *UserDatabase) GetUserByPublicKey(publicKey string) (interface{}, error) {
	// query the database
	query := `SELECT user_id, user_name, role, public_key, private_key, email, department, created_at, is_active, endorsement_count 
		FROM users 
		WHERE public_key = ?`
	row := udb.db.QueryRow(query, publicKey)

	user, err := udb.scanUser(row)
	if err != nil {
		return nil, err
	}

	// update the in-memory cache
	udb.mu.Lock()
	udb.users[user.UserID] = user
	udb.mu.Unlock()

	return user, nil
}

// GetAllUsers returns all users
func (udb *UserDatabase) GetAllUsers() ([]interface{}, error) {
	query := `SELECT user_id, user_name, role, public_key, private_key, email, department, created_at, is_active, endorsement_count FROM users`
	rows, err := udb.db.Query(query)
	if err != nil {
		return nil, WrapDatabaseError("GetAllUsers", fmt.Errorf("failed to query users: %w", err))
	}
	defer rows.Close()

	var users []interface{}
	for rows.Next() {
		var user User
		err := rows.Scan(
			&user.UserID,
			&user.UserName,
			&user.Role,
			&user.PublicKey,
			&user.PrivateKey,
			&user.Email,
			&user.Department,
			&user.CreatedAt,
			&user.IsActive,
			&user.EndorsementCount,
		)
		if err != nil {
			log.Printf("failed to parse user data: %v", err)
			continue
		}

		userMap := map[string]interface{}{
			"user_id":           user.UserID,
			"user_name":         user.UserName,
			"role":              user.Role,
			"public_key":        user.PublicKey,
			"private_key":       user.PrivateKey,
			"email":             user.Email,
			"department":        user.Department,
			"created_at":        user.CreatedAt,
			"is_active":         user.IsActive,
			"endorsement_count": user.EndorsementCount,
		}
		users = append(users, userMap)
	}

	return users, nil
}

// UpdateUser updates a user
func (udb *UserDatabase) UpdateUser(user interface{}) error {
	u, ok := user.(*User)
	if !ok {
		// try to convert from map[string]interface{}
		if userMap, ok := user.(map[string]interface{}); ok {
			u = &User{}
			if userID, ok := userMap["user_id"].(string); ok {
				u.UserID = userID
			}
			if userName, ok := userMap["user_name"].(string); ok {
				u.UserName = userName
			}
			if role, ok := userMap["role"].(string); ok {
				u.Role = role
			}
			if publicKey, ok := userMap["public_key"].(string); ok {
				u.PublicKey = publicKey
			}
			if privateKey, ok := userMap["private_key"].(string); ok {
				u.PrivateKey = privateKey
			}
			if email, ok := userMap["email"].(string); ok {
				u.Email = email
			}
			if department, ok := userMap["department"].(string); ok {
				u.Department = department
			}
			if isActive, ok := userMap["is_active"].(bool); ok {
				u.IsActive = isActive
			}
			if endorsementCount, ok := userMap["endorsement_count"].(int); ok {
				u.EndorsementCount = endorsementCount
			}
		} else {
			return NewDatabaseError("UpdateUser", fmt.Errorf("invalid user type"))
		}
	}

	if u.UserID == "" {
		return NewDatabaseError("UpdateUser", fmt.Errorf("user ID must not be empty"))
	}

	// update the database
	query := `UPDATE users SET user_name = ?, role = ?, public_key = ?, private_key = ?, email = ?, department = ?, is_active = ?, endorsement_count = ? 
		WHERE user_id = ?`

	_, err := udb.db.Exec(query,
		u.UserName,
		u.Role,
		u.PublicKey,
		u.PrivateKey,
		u.Email,
		u.Department,
		u.IsActive,
		u.EndorsementCount,
		u.UserID,
	)
	if err != nil {
		return WrapDatabaseError("UpdateUser", fmt.Errorf("failed to update user: %w", err))
	}

	// update the in-memory cache
	udb.mu.Lock()
	udb.users[u.UserID] = u
	udb.mu.Unlock()

	log.Printf("user updated: %s (%s)", u.UserName, u.UserID)
	return nil
}

// DeleteUser deletes a user
func (udb *UserDatabase) DeleteUser(id string) error {
	// delete from the database
	query := `DELETE FROM users WHERE user_id = ?`
	_, err := udb.db.Exec(query, id)
	if err != nil {
		return WrapDatabaseError("DeleteUser", fmt.Errorf("failed to delete user: %w", err))
	}

	// delete from the in-memory cache
	udb.mu.Lock()
	delete(udb.users, id)
	udb.mu.Unlock()

	return nil
}

// GetUsersByRole returns users by role
func (udb *UserDatabase) GetUsersByRole(role string) ([]interface{}, error) {
	// query the database
	query := `SELECT user_id, user_name, role, public_key, private_key, email, department, created_at, is_active, endorsement_count 
		FROM users 
		WHERE role = ?`
	rows, err := udb.db.Query(query, role)
	if err != nil {
		return nil, WrapDatabaseError("GetUsersByRole", fmt.Errorf("failed to query users: %w", err))
	}
	defer rows.Close()

	var users []interface{}
	for rows.Next() {
		user, err := udb.scanUserRow(rows)
		if err != nil {
			log.Printf("failed to parse user data: %v", err)
			continue
		}
		users = append(users, user)

		// update the in-memory cache
		udb.mu.Lock()
		udb.users[user.UserID] = user
		udb.mu.Unlock()
	}

	return users, nil
}

// UserExists checks whether a user exists
func (udb *UserDatabase) UserExists(id string) bool {
	udb.mu.RLock()
	_, exists := udb.users[id]
	udb.mu.RUnlock()

	if exists {
		return true
	}

	// query the database
	query := `SELECT COUNT(*) FROM users WHERE user_id = ?`
	var count int
	if err := udb.db.QueryRow(query, id).Scan(&count); err != nil {
		log.Printf("failed to check user existence: %v", err)
		return false
	}

	return count > 0
}

// GetActiveUsers returns active users
func (udb *UserDatabase) GetActiveUsers() []*User {
	udb.mu.RLock()
	defer udb.mu.RUnlock()

	var activeUsers []*User
	for _, user := range udb.users {
		if user.IsActive {
			activeUsers = append(activeUsers, user)
		}
	}

	return activeUsers
}

// IncrementEndorsementCount increments a user's endorsement count
func (udb *UserDatabase) IncrementEndorsementCount(userID string) error {
	// update the database
	query := `UPDATE users SET endorsement_count = endorsement_count + 1 WHERE user_id = ?`
	_, err := udb.db.Exec(query, userID)
	if err != nil {
		return WrapDatabaseError("IncrementEndorsementCount", fmt.Errorf("failed to update endorsement count: %w", err))
	}

	// update the in-memory cache
	udb.mu.Lock()
	if user, exists := udb.users[userID]; exists {
		user.EndorsementCount++
	}
	udb.mu.Unlock()

	return nil
}

// loadAllUsers loads all users
func (udb *UserDatabase) loadAllUsers() error {
	query := `SELECT user_id, user_name, role, public_key, private_key, email, department, created_at, is_active, endorsement_count 
		FROM users`

	rows, err := udb.db.Query(query)
	if err != nil {
		return WrapDatabaseError("loadAllUsers", fmt.Errorf("failed to query users: %w", err))
	}
	defer rows.Close()

	udb.mu.Lock()
	defer udb.mu.Unlock()

	for rows.Next() {
		user, err := udb.scanUserRow(rows)
		if err != nil {
			log.Printf("failed to parse user data: %v", err)
			continue
		}
		udb.users[user.UserID] = user
	}

	log.Printf("loaded %d users", len(udb.users))
	return nil
}

// generateDemoUsers generates demo users
func (udb *UserDatabase) generateDemoUsers() {
	// generate the administrator user
	adminPriv, adminPub, _ := crypto.ECDSAGenerateKeyPair()
	admin := &User{
		UserID:           "admin",
		UserName:         "admin",
		Role:             "management",
		PublicKey:        adminPub,
		PrivateKey:       adminPriv,
		Email:            "admin@example.com",
		Department:       "management",
		CreatedAt:        time.Now(),
		IsActive:         true,
		EndorsementCount: 0,
	}

	// generate the developer user
	dev1Priv, dev1Pub, _ := crypto.ECDSAGenerateKeyPair()
	dev1 := &User{
		UserID:           "dev1",
		UserName:         "developer1",
		Role:             "development",
		PublicKey:        dev1Pub,
		PrivateKey:       dev1Priv,
		Email:            "dev1@example.com",
		Department:       "development dept",
		CreatedAt:        time.Now(),
		IsActive:         true,
		EndorsementCount: 0,
	}

	// generate the tester user
	tester1Priv, tester1Pub, _ := crypto.ECDSAGenerateKeyPair()
	tester1 := &User{
		UserID:           "tester1",
		UserName:         "tester1",
		Role:             "test",
		PublicKey:        tester1Pub,
		PrivateKey:       tester1Priv,
		Email:            "tester1@example.com",
		Department:       "test dept",
		CreatedAt:        time.Now(),
		IsActive:         true,
		EndorsementCount: 0,
	}

	// generate the operations user
	ops1Priv, ops1Pub, _ := crypto.ECDSAGenerateKeyPair()
	ops1 := &User{
		UserID:           "ops1",
		UserName:         "operator1",
		Role:             "operations",
		PublicKey:        ops1Pub,
		PrivateKey:       ops1Priv,
		Email:            "ops1@example.com",
		Department:       "operations dept",
		CreatedAt:        time.Now(),
		IsActive:         true,
		EndorsementCount: 0,
	}

	// add the users in batch
	allUsers := []*User{admin, dev1, tester1, ops1}

	for _, user := range allUsers {
		if err := udb.AddUser(user); err != nil {
			log.Printf("failed to add demo user: %v", err)
		}
	}

	log.Printf("generated %d demo users", len(allUsers))
}

// scanUser scans a user from a single row
func (udb *UserDatabase) scanUser(row *sql.Row) (*User, error) {
	var user User

	err := row.Scan(
		&user.UserID,
		&user.UserName,
		&user.Role,
		&user.PublicKey,
		&user.PrivateKey,
		&user.Email,
		&user.Department,
		&user.CreatedAt,
		&user.IsActive,
		&user.EndorsementCount,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("user not found")
		}
		return nil, WrapDatabaseError("scanUser", fmt.Errorf("failed to scan user: %w", err))
	}

	return &user, nil
}

// scanUserRow scans a user from a result-set row
func (udb *UserDatabase) scanUserRow(rows *sql.Rows) (*User, error) {
	var user User

	err := rows.Scan(
		&user.UserID,
		&user.UserName,
		&user.Role,
		&user.PublicKey,
		&user.PrivateKey,
		&user.Email,
		&user.Department,
		&user.CreatedAt,
		&user.IsActive,
		&user.EndorsementCount,
	)

	if err != nil {
		return nil, WrapDatabaseError("scanUserRow", fmt.Errorf("failed to scan user row: %w", err))
	}

	return &user, nil
}
