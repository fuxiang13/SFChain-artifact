package mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"
)

// LogCategory is a log category
type LogCategory string

const (
	CategoryManagement  LogCategory = "management"
	CategoryDevelopment LogCategory = "development"
	CategoryTest        LogCategory = "test"
	CategoryOperations  LogCategory = "operations"
)

// LogQuery is a log query
type LogQuery struct {
	Category  LogCategory
	Level     string
	UserID    string
	Module    string
	Project   string
	StartTime float64
	EndTime   float64
	Status    string
	Processed *bool
	Limit     int
	Offset    int
}

// LogStats is log statistics
type LogStats struct {
	TotalCount     int
	ProcessedCount int
	PendingCount   int
	CategoryStats  map[string]int
	LevelStats     map[string]int
	LatestLogs     []*SoftwareFactoryLog
}

// SoftwareFactoryLog is a software-factory log
type SoftwareFactoryLog struct {
	ID        string                 `json:"id"`
	Category  LogCategory            `json:"category"`
	Timestamp float64                `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	UserID    string                 `json:"user_id"`
	Module    string                 `json:"module"`
	Project   string                 `json:"project"`
	Operation string                 `json:"operation"`
	Status    string                 `json:"status"`
	Processed bool                   `json:"processed"`
	TXID      string                 `json:"tx_id"`
	CreatedAt time.Time              `json:"created_at"`
	Metadata  map[string]interface{} `json:"metadata"`
}

// Validate validates log data
func (l *SoftwareFactoryLog) Validate() bool {
	return l.ID != "" && l.Category != "" && l.Timestamp > 0
}

// ToTransactionLog converts to the transaction log format
func (l *SoftwareFactoryLog) ToTransactionLog() map[string]interface{} {
	return map[string]interface{}{
		"id":         l.ID,
		"category":   l.Category,
		"timestamp":  l.Timestamp,
		"level":      l.Level,
		"message":    l.Message,
		"user_id":    l.UserID,
		"module":     l.Module,
		"project":    l.Project,
		"operation":  l.Operation,
		"status":     l.Status,
		"processed":  l.Processed,
		"tx_id":      l.TXID,
		"created_at": l.CreatedAt,
		"metadata":   l.Metadata,
	}
}

// BoolPtr returns a bool pointer
func BoolPtr(b bool) *bool {
	return &b
}

// LogDatabase is the MySQL implementation of the log database
type LogDatabase struct {
	db   *DB
	mu   sync.RWMutex
	logs map[string]*SoftwareFactoryLog
}

// NewLogDatabase creates a new MySQL log database
func NewLogDatabase(config Config) (*LogDatabase, error) {
	db, err := NewDB(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create database connection: %w", err)
	}

	ldb := &LogDatabase{
		db:   db,
		logs: make(map[string]*SoftwareFactoryLog),
	}

	// initialize the table schema
	if err := ldb.initTables(); err != nil {
		return nil, fmt.Errorf("failed to initialize table schema: %w", err)
	}

	// load recent logs
	if err := ldb.loadRecentLogs(); err != nil {
		log.Printf("failed to load log data: %v", err)
	}

	return ldb, nil
}

// initTables initializes the table schema
func (ldb *LogDatabase) initTables() error {
	// create the software_factory_logs table
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS software_factory_logs (
		id VARCHAR(64) PRIMARY KEY,
		category VARCHAR(32) NOT NULL,
		timestamp DOUBLE NOT NULL,
		level VARCHAR(16) NOT NULL,
		message TEXT NOT NULL,
		user_id VARCHAR(64),
		module VARCHAR(64),
		project VARCHAR(64),
		operation VARCHAR(64),
		status VARCHAR(32),
		processed BOOLEAN DEFAULT FALSE,
		tx_id VARCHAR(64),
		metadata JSON,
		created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
	`
	_, err := ldb.db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create software_factory_logs table: %w", err)
	}

	// create indexes (MySQL does not support CREATE INDEX IF NOT EXISTS; check first)
	indexQueries := []struct {
		name  string
		query string
	}{
		{"idx_software_factory_logs_category", "CREATE INDEX idx_software_factory_logs_category ON software_factory_logs(category)"},
		{"idx_software_factory_logs_processed", "CREATE INDEX idx_software_factory_logs_processed ON software_factory_logs(processed)"},
		{"idx_software_factory_logs_timestamp", "CREATE INDEX idx_software_factory_logs_timestamp ON software_factory_logs(timestamp)"},
		{"idx_software_factory_logs_tx_id", "CREATE INDEX idx_software_factory_logs_tx_id ON software_factory_logs(tx_id)"},
	}

	for _, idx := range indexQueries {
		// check whether the index already exists
		var count int
		checkQuery := `
			SELECT COUNT(*) 
			FROM information_schema.statistics 
			WHERE table_schema = DATABASE() 
			AND table_name = 'software_factory_logs' 
			AND index_name = ?`

		err := ldb.db.QueryRow(checkQuery, idx.name).Scan(&count)
		if err != nil {
			log.Printf("failed to check index %s: %v", idx.name, err)
			continue
		}

		// if the index does not exist, create it
		if count == 0 {
			if _, err := ldb.db.Exec(idx.query); err != nil {
				log.Printf("failed to create index %s: %v", idx.name, err)
			} else {
				log.Printf("index %s created successfully", idx.name)
			}
		}
	}

	log.Printf("log database table schema initialized successfully")
	return nil
}

// Close closes the database
func (ldb *LogDatabase) Close() error {
	return ldb.db.Close()
}

// AddLog adds a log
func (ldb *LogDatabase) AddLog(logData interface{}) error {
	var sfLog *SoftwareFactoryLog

	// try to convert to the *SoftwareFactoryLog type
	if logMySQL, ok := logData.(*SoftwareFactoryLog); ok {
		sfLog = logMySQL
	} else {
		// try to convert from map[string]interface{}
		if logMap, ok := logData.(map[string]interface{}); ok {
			sfLog = &SoftwareFactoryLog{}
			if id, ok := logMap["id"].(string); ok {
				sfLog.ID = id
			}
			if category, ok := logMap["category"].(string); ok {
				sfLog.Category = LogCategory(category)
			}
			if timestamp, ok := logMap["timestamp"].(float64); ok {
				sfLog.Timestamp = timestamp
			}
			if level, ok := logMap["level"].(string); ok {
				sfLog.Level = level
			}
			if message, ok := logMap["message"].(string); ok {
				sfLog.Message = message
			}
			if userID, ok := logMap["user_id"].(string); ok {
				sfLog.UserID = userID
			}
			if module, ok := logMap["module"].(string); ok {
				sfLog.Module = module
			}
			if project, ok := logMap["project"].(string); ok {
				sfLog.Project = project
			}
			if operation, ok := logMap["operation"].(string); ok {
				sfLog.Operation = operation
			}
			if status, ok := logMap["status"].(string); ok {
				sfLog.Status = status
			}
			if processed, ok := logMap["processed"].(bool); ok {
				sfLog.Processed = processed
			}
			if txID, ok := logMap["tx_id"].(string); ok {
				sfLog.TXID = txID
			}
			if metadata, ok := logMap["metadata"].(map[string]interface{}); ok {
				sfLog.Metadata = metadata
			}
			if createdAt, ok := logMap["created_at"].(time.Time); ok {
				sfLog.CreatedAt = createdAt
			} else {
				sfLog.CreatedAt = time.Now()
			}
		} else {
			return NewDatabaseError("AddLog", fmt.Errorf("invalid log type"))
		}
	}

	if !sfLog.Validate() {
		return NewDatabaseError("AddLog", fmt.Errorf("invalid log data"))
	}

	metadata, err := json.Marshal(sfLog.Metadata)
	if err != nil {
		return WrapDatabaseError("AddLog", fmt.Errorf("failed to serialize metadata: %w", err))
	}

	// insert into the database
	query := `INSERT INTO software_factory_logs 
		(id, category, timestamp, level, message, user_id, module, project, operation, status, processed, tx_id, metadata) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
		ON DUPLICATE KEY UPDATE 
			category = VALUES(category), 
			level = VALUES(level), 
			message = VALUES(message), 
			status = VALUES(status), 
			processed = VALUES(processed), 
			tx_id = VALUES(tx_id), 
			metadata = VALUES(metadata)`

	_, err = ldb.db.Exec(query,
		sfLog.ID,
		sfLog.Category,
		sfLog.Timestamp,
		sfLog.Level,
		sfLog.Message,
		sfLog.UserID,
		sfLog.Module,
		sfLog.Project,
		sfLog.Operation,
		sfLog.Status,
		sfLog.Processed,
		sfLog.TXID,
		metadata,
	)
	if err != nil {
		return WrapDatabaseError("AddLog", fmt.Errorf("failed to save log: %w", err))
	}

	// update the in-memory cache
	ldb.mu.Lock()
	ldb.logs[sfLog.ID] = sfLog
	ldb.mu.Unlock()

	log.Printf("log added: %s (category: %s, level: %s)", sfLog.ID[:16], sfLog.Category, sfLog.Level)
	return nil
}

// AddLogs adds logs in batch
func (ldb *LogDatabase) AddLogs(logs []interface{}) error {
	// begin a transaction
	tx, err := ldb.db.Begin()
	if err != nil {
		return WrapDatabaseError("AddLogs", fmt.Errorf("failed to begin transaction: %w", err))
	}

	// prepare the statement
	query := `INSERT INTO software_factory_logs 
		(id, category, timestamp, level, message, user_id, module, project, operation, status, processed, tx_id, metadata) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
		ON DUPLICATE KEY UPDATE 
			category = VALUES(category), 
			level = VALUES(level), 
			message = VALUES(message), 
			status = VALUES(status), 
			processed = VALUES(processed), 
			tx_id = VALUES(tx_id), 
			metadata = VALUES(metadata)`

	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return WrapDatabaseError("AddLogs", fmt.Errorf("failed to prepare statement: %w", err))
	}
	defer stmt.Close()

	// perform the batch insert
	for _, logData := range logs {
		var sfLog *SoftwareFactoryLog

		// try to convert to the *SoftwareFactoryLog type
		if logMySQL, ok := logData.(*SoftwareFactoryLog); ok {
			sfLog = logMySQL
		} else {
			// try to convert from map[string]interface{}
			if logMap, ok := logData.(map[string]interface{}); ok {
				sfLog = &SoftwareFactoryLog{}
				if id, ok := logMap["id"].(string); ok {
					sfLog.ID = id
				}
				if category, ok := logMap["category"].(string); ok {
					sfLog.Category = LogCategory(category)
				}
				if timestamp, ok := logMap["timestamp"].(float64); ok {
					sfLog.Timestamp = timestamp
				}
				if level, ok := logMap["level"].(string); ok {
					sfLog.Level = level
				}
				if message, ok := logMap["message"].(string); ok {
					sfLog.Message = message
				}
				if userID, ok := logMap["user_id"].(string); ok {
					sfLog.UserID = userID
				}
				if module, ok := logMap["module"].(string); ok {
					sfLog.Module = module
				}
				if project, ok := logMap["project"].(string); ok {
					sfLog.Project = project
				}
				if operation, ok := logMap["operation"].(string); ok {
					sfLog.Operation = operation
				}
				if status, ok := logMap["status"].(string); ok {
					sfLog.Status = status
				}
				if processed, ok := logMap["processed"].(bool); ok {
					sfLog.Processed = processed
				}
				if txID, ok := logMap["tx_id"].(string); ok {
					sfLog.TXID = txID
				}
				if metadata, ok := logMap["metadata"].(map[string]interface{}); ok {
					sfLog.Metadata = metadata
				}
				if createdAt, ok := logMap["created_at"].(time.Time); ok {
					sfLog.CreatedAt = createdAt
				} else {
					sfLog.CreatedAt = time.Now()
				}
			} else {
				// try reflection to handle other SoftwareFactoryLog types
				v := reflect.ValueOf(logData)
				if v.Kind() == reflect.Ptr && v.IsValid() {
					v = v.Elem()
					if v.Kind() == reflect.Struct {
						sfLog = &SoftwareFactoryLog{}

						// try to get field values
						if idField := v.FieldByName("ID"); idField.IsValid() && idField.Kind() == reflect.String {
							sfLog.ID = idField.String()
						}
						if categoryField := v.FieldByName("Category"); categoryField.IsValid() {
							if categoryField.Kind() == reflect.String {
								sfLog.Category = LogCategory(categoryField.String())
							} else {
								// try to get the value of the Category field
								categoryValue := reflect.Indirect(categoryField)
								if categoryValue.Kind() == reflect.String {
									sfLog.Category = LogCategory(categoryValue.String())
								}
							}
						}
						if timestampField := v.FieldByName("Timestamp"); timestampField.IsValid() && timestampField.Kind() == reflect.Float64 {
							sfLog.Timestamp = timestampField.Float()
						}
						if levelField := v.FieldByName("Level"); levelField.IsValid() && levelField.Kind() == reflect.String {
							sfLog.Level = levelField.String()
						}
						if messageField := v.FieldByName("Message"); messageField.IsValid() && messageField.Kind() == reflect.String {
							sfLog.Message = messageField.String()
						}
						if userIDField := v.FieldByName("UserID"); userIDField.IsValid() && userIDField.Kind() == reflect.String {
							sfLog.UserID = userIDField.String()
						}
						if moduleField := v.FieldByName("Module"); moduleField.IsValid() && moduleField.Kind() == reflect.String {
							sfLog.Module = moduleField.String()
						}
						if projectField := v.FieldByName("Project"); projectField.IsValid() && projectField.Kind() == reflect.String {
							sfLog.Project = projectField.String()
						}
						if operationField := v.FieldByName("Operation"); operationField.IsValid() && operationField.Kind() == reflect.String {
							sfLog.Operation = operationField.String()
						}
						if statusField := v.FieldByName("Status"); statusField.IsValid() && statusField.Kind() == reflect.String {
							sfLog.Status = statusField.String()
						}
						if processedField := v.FieldByName("Processed"); processedField.IsValid() && processedField.Kind() == reflect.Bool {
							sfLog.Processed = processedField.Bool()
						}
						if txIDField := v.FieldByName("TXID"); txIDField.IsValid() && txIDField.Kind() == reflect.String {
							sfLog.TXID = txIDField.String()
						}
						if createdAtField := v.FieldByName("CreatedAt"); createdAtField.IsValid() && createdAtField.Type() == reflect.TypeOf(time.Time{}) {
							sfLog.CreatedAt = createdAtField.Interface().(time.Time)
						}

						// try to get the Metadata field
						if metadataField := v.FieldByName("Metadata"); metadataField.IsValid() {
							if metadataField.Kind() == reflect.Map {
								if metadata, ok := metadataField.Interface().(map[string]interface{}); ok {
									sfLog.Metadata = metadata
								}
							}
						}
					}
				}

				// if still not convertible, return an error
				if sfLog == nil {
					tx.Rollback()
					return NewDatabaseError("AddLogs", fmt.Errorf("invalid log type"))
				}
			}
		}

		if !sfLog.Validate() {
			tx.Rollback()
			return NewDatabaseError("AddLogs", fmt.Errorf("invalid log data: %s", sfLog.ID))
		}

		metadata, err := json.Marshal(sfLog.Metadata)
		if err != nil {
			tx.Rollback()
			return WrapDatabaseError("AddLogs", fmt.Errorf("failed to serialize metadata: %w", err))
		}

		_, err = stmt.Exec(
			sfLog.ID,
			sfLog.Category,
			sfLog.Timestamp,
			sfLog.Level,
			sfLog.Message,
			sfLog.UserID,
			sfLog.Module,
			sfLog.Project,
			sfLog.Operation,
			sfLog.Status,
			sfLog.Processed,
			sfLog.TXID,
			metadata,
		)
		if err != nil {
			tx.Rollback()
			return WrapDatabaseError("AddLogs", fmt.Errorf("failed to save log: %w", err))
		}
	}

	// commit the transaction
	if err := tx.Commit(); err != nil {
		return WrapDatabaseError("AddLogs", fmt.Errorf("failed to commit transaction: %w", err))
	}

	// update the in-memory cache
	ldb.mu.Lock()
	for _, logData := range logs {
		var sfLog *SoftwareFactoryLog

		// try to convert to the *SoftwareFactoryLog type
		if logMySQL, ok := logData.(*SoftwareFactoryLog); ok {
			sfLog = logMySQL
		} else if logMap, ok := logData.(map[string]interface{}); ok {
			// try to convert from map[string]interface{}
			sfLog = &SoftwareFactoryLog{}
			if id, ok := logMap["id"].(string); ok {
				sfLog.ID = id
			}
			if category, ok := logMap["category"].(string); ok {
				sfLog.Category = LogCategory(category)
			}
			if timestamp, ok := logMap["timestamp"].(float64); ok {
				sfLog.Timestamp = timestamp
			}
			if level, ok := logMap["level"].(string); ok {
				sfLog.Level = level
			}
			if message, ok := logMap["message"].(string); ok {
				sfLog.Message = message
			}
			if userID, ok := logMap["user_id"].(string); ok {
				sfLog.UserID = userID
			}
			if module, ok := logMap["module"].(string); ok {
				sfLog.Module = module
			}
			if project, ok := logMap["project"].(string); ok {
				sfLog.Project = project
			}
			if operation, ok := logMap["operation"].(string); ok {
				sfLog.Operation = operation
			}
			if status, ok := logMap["status"].(string); ok {
				sfLog.Status = status
			}
			if processed, ok := logMap["processed"].(bool); ok {
				sfLog.Processed = processed
			}
			if txID, ok := logMap["tx_id"].(string); ok {
				sfLog.TXID = txID
			}
			if metadata, ok := logMap["metadata"].(map[string]interface{}); ok {
				sfLog.Metadata = metadata
			}
		} else {
			// try reflection to handle other SoftwareFactoryLog types
			v := reflect.ValueOf(logData)
			if v.Kind() == reflect.Ptr && v.IsValid() {
				v = v.Elem()
				if v.Kind() == reflect.Struct {
					sfLog = &SoftwareFactoryLog{}

					// try to get field values
					if idField := v.FieldByName("ID"); idField.IsValid() && idField.Kind() == reflect.String {
						sfLog.ID = idField.String()
					}
					if categoryField := v.FieldByName("Category"); categoryField.IsValid() {
						if categoryField.Kind() == reflect.String {
							sfLog.Category = LogCategory(categoryField.String())
						} else {
							// try to get the value of the Category field
							categoryValue := reflect.Indirect(categoryField)
							if categoryValue.Kind() == reflect.String {
								sfLog.Category = LogCategory(categoryValue.String())
							}
						}
					}
					if timestampField := v.FieldByName("Timestamp"); timestampField.IsValid() && timestampField.Kind() == reflect.Float64 {
						sfLog.Timestamp = timestampField.Float()
					}
					if levelField := v.FieldByName("Level"); levelField.IsValid() && levelField.Kind() == reflect.String {
						sfLog.Level = levelField.String()
					}
					if messageField := v.FieldByName("Message"); messageField.IsValid() && messageField.Kind() == reflect.String {
						sfLog.Message = messageField.String()
					}
					if userIDField := v.FieldByName("UserID"); userIDField.IsValid() && userIDField.Kind() == reflect.String {
						sfLog.UserID = userIDField.String()
					}
					if moduleField := v.FieldByName("Module"); moduleField.IsValid() && moduleField.Kind() == reflect.String {
						sfLog.Module = moduleField.String()
					}
					if projectField := v.FieldByName("Project"); projectField.IsValid() && projectField.Kind() == reflect.String {
						sfLog.Project = projectField.String()
					}
					if operationField := v.FieldByName("Operation"); operationField.IsValid() && operationField.Kind() == reflect.String {
						sfLog.Operation = operationField.String()
					}
					if statusField := v.FieldByName("Status"); statusField.IsValid() && statusField.Kind() == reflect.String {
						sfLog.Status = statusField.String()
					}
					if processedField := v.FieldByName("Processed"); processedField.IsValid() && processedField.Kind() == reflect.Bool {
						sfLog.Processed = processedField.Bool()
					}
					if txIDField := v.FieldByName("TXID"); txIDField.IsValid() && txIDField.Kind() == reflect.String {
						sfLog.TXID = txIDField.String()
					}
				}
			}
		}

		if sfLog != nil && sfLog.ID != "" {
			ldb.logs[sfLog.ID] = sfLog
		}
	}
	ldb.mu.Unlock()

	log.Printf("added %d logs in batch", len(logs))
	return nil
}

// GetLog gets a log
func (ldb *LogDatabase) GetLog(logID string) (interface{}, error) {
	ldb.mu.RLock()
	if sfLog, exists := ldb.logs[logID]; exists {
		ldb.mu.RUnlock()
		return sfLog, nil
	}
	ldb.mu.RUnlock()

	// query the database
	query := `SELECT id, category, timestamp, level, message, user_id, module, project, operation, status, processed, tx_id, metadata 
		FROM software_factory_logs 
		WHERE id = ?`
	row := ldb.db.QueryRow(query, logID)

	sfLog, err := ldb.scanLog(row)
	if err != nil {
		return nil, err
	}

	// update the in-memory cache
	ldb.mu.Lock()
	ldb.logs[logID] = sfLog
	ldb.mu.Unlock()

	return sfLog, nil
}

// QueryLogs queries logs
func (ldb *LogDatabase) QueryLogs(query interface{}) ([]interface{}, error) {
	// build the query SQL
	sqlQuery := `SELECT id, category, timestamp, level, message, user_id, module, project, operation, status, processed, tx_id, metadata 
		FROM software_factory_logs 
		WHERE 1=1`
	var args []interface{}

	// add query conditions
	if qPtr, ok := query.(*LogQuery); ok {
		q := *qPtr
		if q.Category != "" {
			sqlQuery += " AND category = ?"
			args = append(args, q.Category)
		}

		if q.Level != "" {
			sqlQuery += " AND level = ?"
			args = append(args, q.Level)
		}

		if q.UserID != "" {
			sqlQuery += " AND user_id = ?"
			args = append(args, q.UserID)
		}

		if q.Module != "" {
			sqlQuery += " AND module = ?"
			args = append(args, q.Module)
		}

		if q.Project != "" {
			sqlQuery += " AND project = ?"
			args = append(args, q.Project)
		}

		if q.StartTime > 0 {
			sqlQuery += " AND timestamp >= ?"
			args = append(args, q.StartTime)
		}

		if q.EndTime > 0 {
			sqlQuery += " AND timestamp <= ?"
			args = append(args, q.EndTime)
		}

		if q.Status != "" {
			sqlQuery += " AND status = ?"
			args = append(args, q.Status)
		}

		if q.Processed != nil {
			sqlQuery += " AND processed = ?"
			args = append(args, *q.Processed)
		}

		// add ordering and pagination
		sqlQuery += " ORDER BY timestamp DESC"

		if q.Limit > 0 {
			sqlQuery += " LIMIT ?"
			args = append(args, q.Limit)

			if q.Offset > 0 {
				sqlQuery += " OFFSET ?"
				args = append(args, q.Offset)
			}
		}
	} else if qVal, ok := query.(LogQuery); ok {
		q := qVal
		if q.Category != "" {
			sqlQuery += " AND category = ?"
			args = append(args, q.Category)
		}

		if q.Level != "" {
			sqlQuery += " AND level = ?"
			args = append(args, q.Level)
		}

		if q.UserID != "" {
			sqlQuery += " AND user_id = ?"
			args = append(args, q.UserID)
		}

		if q.Module != "" {
			sqlQuery += " AND module = ?"
			args = append(args, q.Module)
		}

		if q.Project != "" {
			sqlQuery += " AND project = ?"
			args = append(args, q.Project)
		}

		if q.StartTime > 0 {
			sqlQuery += " AND timestamp >= ?"
			args = append(args, q.StartTime)
		}

		if q.EndTime > 0 {
			sqlQuery += " AND timestamp <= ?"
			args = append(args, q.EndTime)
		}

		if q.Status != "" {
			sqlQuery += " AND status = ?"
			args = append(args, q.Status)
		}

		if q.Processed != nil {
			sqlQuery += " AND processed = ?"
			args = append(args, *q.Processed)
		}

		// add ordering and pagination
		sqlQuery += " ORDER BY timestamp DESC"

		if q.Limit > 0 {
			sqlQuery += " LIMIT ?"
			args = append(args, q.Limit)

			if q.Offset > 0 {
				sqlQuery += " OFFSET ?"
				args = append(args, q.Offset)
			}
		}
	}

	// execute the query
	rows, err := ldb.db.Query(sqlQuery, args...)
	if err != nil {
		return nil, WrapDatabaseError("QueryLogs", fmt.Errorf("failed to query logs: %w", err))
	}
	defer rows.Close()

	var sfLogs []interface{}
	for rows.Next() {
		sfLog, err := ldb.scanLogRow(rows)
		if err != nil {
			log.Printf("failed to parse log data: %v", err)
			continue
		}
		sfLogs = append(sfLogs, sfLog)

		// update the in-memory cache
		ldb.mu.Lock()
		ldb.logs[sfLog.ID] = sfLog
		ldb.mu.Unlock()
	}

	return sfLogs, nil
}

// GetPendingLogs returns pending logs
func (ldb *LogDatabase) GetPendingLogs(limit int) ([]interface{}, error) {
	query := `SELECT id, category, timestamp, level, message, user_id, module, project, operation, status, processed, tx_id, metadata 
		FROM software_factory_logs 
		WHERE processed = false 
		ORDER BY timestamp ASC`

	if limit > 0 {
		query += " LIMIT ?"
	}

	var rows *sql.Rows
	var err error
	if limit > 0 {
		rows, err = ldb.db.Query(query, limit)
	} else {
		rows, err = ldb.db.Query(query)
	}

	if err != nil {
		return nil, WrapDatabaseError("GetPendingLogs", fmt.Errorf("failed to query pending logs: %w", err))
	}
	defer rows.Close()

	var sfLogs []interface{}
	for rows.Next() {
		sfLog, err := ldb.scanLogRow(rows)
		if err != nil {
			log.Printf("failed to parse log data: %v", err)
			continue
		}
		sfLogs = append(sfLogs, sfLog)
	}

	return sfLogs, nil
}

// MarkAsProcessed marks a log as processed
func (ldb *LogDatabase) MarkAsProcessed(logID, txID string) error {
	query := `UPDATE software_factory_logs SET processed = true, tx_id = ? WHERE id = ?`
	_, err := ldb.db.Exec(query, txID, logID)
	if err != nil {
		return WrapDatabaseError("MarkAsProcessed", fmt.Errorf("failed to mark log as processed: %w", err))
	}

	// update the in-memory cache
	ldb.mu.Lock()
	if sfLog, exists := ldb.logs[logID]; exists {
		sfLog.Processed = true
		sfLog.TXID = txID
	}
	ldb.mu.Unlock()

	return nil
}

// MarkAsProcessedBatch marks logs as processed in batch (single UPDATE ... CASE, reducing per-row write amplification)
func (ldb *LogDatabase) MarkAsProcessedBatch(refs [][2]string) error {
	if len(refs) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString("UPDATE software_factory_logs SET processed = true, tx_id = CASE id ")
	args := make([]interface{}, 0, len(refs)*2+len(refs))
	for _, r := range refs {
		sb.WriteString("WHEN ? THEN ? ")
		args = append(args, r[0], r[1])
	}
	sb.WriteString("END WHERE id IN (")
	for i, r := range refs {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("?")
		args = append(args, r[0])
	}
	sb.WriteString(")")
	_, err := ldb.db.Exec(sb.String(), args...)
	if err != nil {
		return WrapDatabaseError("MarkAsProcessedBatch", fmt.Errorf("failed to mark logs as processed in batch: %w", err))
	}

	// update the in-memory cache
	ldb.mu.Lock()
	for _, r := range refs {
		if sfLog, exists := ldb.logs[r[0]]; exists {
			sfLog.Processed = true
			sfLog.TXID = r[1]
		}
	}
	ldb.mu.Unlock()

	return nil
}

// GetStats returns log statistics
func (ldb *LogDatabase) GetStats() (interface{}, error) {
	// get the total count
	totalQuery := `SELECT COUNT(*) FROM software_factory_logs`
	var totalCount int
	if err := ldb.db.QueryRow(totalQuery).Scan(&totalCount); err != nil {
		return nil, WrapDatabaseError("GetStats", fmt.Errorf("failed to get total log count: %w", err))
	}

	// get processed-state statistics
	statusQuery := `SELECT processed, COUNT(*) FROM software_factory_logs GROUP BY processed`
	rows, err := ldb.db.Query(statusQuery)
	if err != nil {
		return nil, WrapDatabaseError("GetStats", fmt.Errorf("failed to get processed-state statistics: %w", err))
	}
	defer rows.Close()

	var processedCount, pendingCount int
	for rows.Next() {
		var processed bool
		var count int
		if err := rows.Scan(&processed, &count); err != nil {
			log.Printf("failed to parse processed-state statistics: %v", err)
			continue
		}
		if processed {
			processedCount = count
		} else {
			pendingCount = count
		}
	}

	// get category statistics
	categoryQuery := `SELECT category, COUNT(*) FROM software_factory_logs GROUP BY category`
	rows, err = ldb.db.Query(categoryQuery)
	if err != nil {
		return nil, WrapDatabaseError("GetStats", fmt.Errorf("failed to get category statistics: %w", err))
	}
	defer rows.Close()

	categoryStats := make(map[string]int)
	for rows.Next() {
		var category string
		var count int
		if err := rows.Scan(&category, &count); err != nil {
			log.Printf("failed to parse category statistics: %v", err)
			continue
		}
		categoryStats[category] = count
	}

	// get level statistics
	levelQuery := `SELECT level, COUNT(*) FROM software_factory_logs GROUP BY level`
	rows, err = ldb.db.Query(levelQuery)
	if err != nil {
		return nil, WrapDatabaseError("GetStats", fmt.Errorf("failed to get level statistics: %w", err))
	}
	defer rows.Close()

	levelStats := make(map[string]int)
	for rows.Next() {
		var level string
		var count int
		if err := rows.Scan(&level, &count); err != nil {
			log.Printf("failed to parse level statistics: %v", err)
			continue
		}
		levelStats[level] = count
	}

	// get the latest log
	latestQuery := `SELECT id, category, timestamp, level, message, user_id, module, project, operation, status, processed, tx_id, metadata 
		FROM software_factory_logs 
		ORDER BY timestamp DESC 
		LIMIT 10`
	rows, err = ldb.db.Query(latestQuery)
	if err != nil {
		return nil, WrapDatabaseError("GetStats", fmt.Errorf("failed to get latest log: %w", err))
	}
	defer rows.Close()

	var latestSfLogs []*SoftwareFactoryLog
	for rows.Next() {
		sfLog, err := ldb.scanLogRow(rows)
		if err != nil {
			log.Printf("failed to parse latest log: %v", err)
			continue
		}
		latestSfLogs = append(latestSfLogs, sfLog)
	}

	stats := &LogStats{
		TotalCount:     totalCount,
		ProcessedCount: processedCount,
		PendingCount:   pendingCount,
		CategoryStats:  categoryStats,
		LevelStats:     levelStats,
		LatestLogs:     latestSfLogs,
	}

	return stats, nil
}

// BuildTransactionLogs builds transaction logs (category filtered by chain type: in the multi-chain architecture the
// management/development/test/operations log categories must be routed to their own business chains,
// otherwise logs are misrouted to chains by polling order)
func (ldb *LogDatabase) BuildTransactionLogs(chainType interface{}, limit int) ([]map[string]interface{}, error) {
	query := `SELECT id, category, timestamp, level, message, user_id, module, project, operation, status, processed, tx_id, metadata
		FROM software_factory_logs
		WHERE processed = false`
	args := make([]interface{}, 0, 2)
	if chainType != nil {
		if cat := fmt.Sprintf("%v", chainType); cat != "" && cat != "<nil>" {
			query += ` AND category = ?`
			args = append(args, cat)
		}
	}
	query += ` ORDER BY timestamp ASC`

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := ldb.db.Query(query, args...)
	if err != nil {
		return nil, WrapDatabaseError("BuildTransactionLogs", fmt.Errorf("failed to query logs: %w", err))
	}
	defer rows.Close()

	var transactionLogs []map[string]interface{}
	for rows.Next() {
		sfLog, err := ldb.scanLogRow(rows)
		if err != nil {
			log.Printf("failed to parse log data: %v", err)
			continue
		}
		transactionLogs = append(transactionLogs, sfLog.ToTransactionLog())
	}

	return transactionLogs, nil
}

// GetPendingLogsOrdered returns one deterministic, ascending snapshot for the
// current RQ1 protocol. It performs one ordered read without per-category
// polling or pacing.
func (ldb *LogDatabase) GetPendingLogsOrdered(limit int) ([]interface{}, error) {
	query := `SELECT id, category, timestamp, level, message, user_id, module,
		project, operation, status, processed, tx_id, metadata
		FROM software_factory_logs
		WHERE processed = false
		ORDER BY timestamp ASC, id ASC`
	args := []interface{}{}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := ldb.db.Query(query, args...)
	if err != nil {
		return nil, WrapDatabaseError("GetPendingLogsOrdered", fmt.Errorf("failed to query RQ1 logs: %w", err))
	}
	defer rows.Close()

	out := make([]interface{}, 0)
	for rows.Next() {
		sfLog, err := ldb.scanLogRow(rows)
		if err != nil {
			return nil, WrapDatabaseError("GetPendingLogsOrdered", fmt.Errorf("failed to parse RQ1 logs: %w", err))
		}
		out = append(out, sfLog)
	}
	if err := rows.Err(); err != nil {
		return nil, WrapDatabaseError("GetPendingLogsOrdered", fmt.Errorf("failed to read RQ1 logs: %w", err))
	}
	return out, nil
}

// GetLogsByTXID returns logs by transaction ID
func (ldb *LogDatabase) GetLogsByTXID(txID string) ([]interface{}, error) {
	query := `SELECT id, category, timestamp, level, message, user_id, module, project, operation, status, processed, tx_id, metadata 
		FROM software_factory_logs 
		WHERE tx_id = ? 
		ORDER BY timestamp ASC`

	rows, err := ldb.db.Query(query, txID)
	if err != nil {
		return nil, WrapDatabaseError("GetLogsByTXID", fmt.Errorf("failed to query logs: %w", err))
	}
	defer rows.Close()

	var sfLogs []interface{}
	for rows.Next() {
		sfLog, err := ldb.scanLogRow(rows)
		if err != nil {
			log.Printf("failed to parse log data: %v", err)
			continue
		}
		sfLogs = append(sfLogs, sfLog)
	}

	return sfLogs, nil
}

// DeleteOldLogs deletes old logs
func (ldb *LogDatabase) DeleteOldLogs(beforeTimestamp float64) (int, error) {
	query := `DELETE FROM software_factory_logs WHERE timestamp < ?`
	result, err := ldb.db.Exec(query, beforeTimestamp)
	if err != nil {
		return 0, WrapDatabaseError("DeleteOldLogs", fmt.Errorf("failed to delete old logs: %w", err))
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, WrapDatabaseError("DeleteOldLogs", fmt.Errorf("failed to get deleted row count: %w", err))
	}

	// update the in-memory cache
	ldb.mu.Lock()
	for id, log := range ldb.logs {
		if log.Timestamp < beforeTimestamp {
			delete(ldb.logs, id)
		}
	}
	ldb.mu.Unlock()

	return int(rowsAffected), nil
}

// IsProcessed checks whether a transaction has been processed
func (ldb *LogDatabase) IsProcessed(txID string) bool {
	query := `SELECT COUNT(*) FROM software_factory_logs WHERE tx_id = ?`
	var count int
	if err := ldb.db.QueryRow(query, txID).Scan(&count); err != nil {
		log.Printf("failed to check transaction processed state: %v", err)
		return false
	}

	return count > 0
}

// GetPendingCount returns the pending log count
func (ldb *LogDatabase) GetPendingCount() int {
	query := `SELECT COUNT(*) FROM software_factory_logs WHERE processed = false`
	var count int
	if err := ldb.db.QueryRow(query).Scan(&count); err != nil {
		log.Printf("failed to get pending log count: %v", err)
		return 0
	}

	return count
}

// GetDBPath returns the database path
func (ldb *LogDatabase) GetDBPath() string {
	// MySQL has no file path; return connection information
	return fmt.Sprintf("mysql://%s:%s@%s:%d/%s", ldb.db.config.User, "****", ldb.db.config.Host, ldb.db.config.Port, ldb.db.config.DBName)
}

// UpdateLogStatus updates a log status
func (ldb *LogDatabase) UpdateLogStatus(logID string, processed bool) error {
	query := `UPDATE software_factory_logs SET processed = ? WHERE id = ?`
	_, err := ldb.db.Exec(query, processed, logID)
	if err != nil {
		return WrapDatabaseError("UpdateLogStatus", fmt.Errorf("failed to update log status: %w", err))
	}

	// update the in-memory cache
	ldb.mu.Lock()
	if sfLog, exists := ldb.logs[logID]; exists {
		sfLog.Processed = processed
	}
	ldb.mu.Unlock()

	return nil
}

// DeleteLog deletes a log
func (ldb *LogDatabase) DeleteLog(logID string) error {
	query := `DELETE FROM software_factory_logs WHERE id = ?`
	_, err := ldb.db.Exec(query, logID)
	if err != nil {
		return WrapDatabaseError("DeleteLog", fmt.Errorf("failed to delete log: %w", err))
	}

	// update the in-memory cache
	ldb.mu.Lock()
	delete(ldb.logs, logID)
	ldb.mu.Unlock()

	return nil
}

// loadRecentLogs loads recent logs
func (ldb *LogDatabase) loadRecentLogs() error {
	query := `SELECT id, category, timestamp, level, message, user_id, module, project, operation, status, processed, tx_id, metadata 
		FROM software_factory_logs 
		ORDER BY timestamp DESC 
		LIMIT 1000`

	rows, err := ldb.db.Query(query)
	if err != nil {
		return WrapDatabaseError("loadRecentLogs", fmt.Errorf("failed to query logs: %w", err))
	}
	defer rows.Close()

	ldb.mu.Lock()
	defer ldb.mu.Unlock()

	for rows.Next() {
		sfLog, err := ldb.scanLogRow(rows)
		if err != nil {
			log.Printf("failed to parse log data: %v", err)
			continue
		}
		ldb.logs[sfLog.ID] = sfLog
	}

	log.Printf("loaded %d recent logs", len(ldb.logs))
	return nil
}

// scanLog scans a log from a single row
func (ldb *LogDatabase) scanLog(row *sql.Row) (*SoftwareFactoryLog, error) {
	var sfLog SoftwareFactoryLog
	var metadataJSON []byte
	var txID sql.NullString

	err := row.Scan(
		&sfLog.ID,
		&sfLog.Category,
		&sfLog.Timestamp,
		&sfLog.Level,
		&sfLog.Message,
		&sfLog.UserID,
		&sfLog.Module,
		&sfLog.Project,
		&sfLog.Operation,
		&sfLog.Status,
		&sfLog.Processed,
		&txID,
		&metadataJSON,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("log not found")
		}
		return nil, WrapDatabaseError("scanLog", fmt.Errorf("failed to scan log: %w", err))
	}
	sfLog.TXID = txID.String

	// parse metadata
	if len(metadataJSON) > 0 {
		var metadata map[string]interface{}
		if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
			return nil, WrapDatabaseError("scanLog", fmt.Errorf("failed to parse metadata: %w", err))
		}
		sfLog.Metadata = metadata
	} else {
		sfLog.Metadata = make(map[string]interface{})
	}

	sfLog.CreatedAt = time.Now() // use current time for now

	return &sfLog, nil
}

// scanLogRow scans a log from a result-set row
func (ldb *LogDatabase) scanLogRow(rows *sql.Rows) (*SoftwareFactoryLog, error) {
	var sfLog SoftwareFactoryLog
	var metadataJSON []byte
	var txID sql.NullString

	err := rows.Scan(
		&sfLog.ID,
		&sfLog.Category,
		&sfLog.Timestamp,
		&sfLog.Level,
		&sfLog.Message,
		&sfLog.UserID,
		&sfLog.Module,
		&sfLog.Project,
		&sfLog.Operation,
		&sfLog.Status,
		&sfLog.Processed,
		&txID,
		&metadataJSON,
	)

	if err != nil {
		return nil, WrapDatabaseError("scanLogRow", fmt.Errorf("failed to scan log row: %w", err))
	}
	sfLog.TXID = txID.String

	// parse metadata
	if len(metadataJSON) > 0 {
		var metadata map[string]interface{}
		if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
			return nil, WrapDatabaseError("scanLogRow", fmt.Errorf("failed to parse metadata: %w", err))
		}
		sfLog.Metadata = metadata
	} else {
		sfLog.Metadata = make(map[string]interface{})
	}

	sfLog.CreatedAt = time.Now() // use current time for now

	return &sfLog, nil
}
