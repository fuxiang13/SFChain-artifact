package database

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"time"
)

// LogGenerator generates logs (simulating software factory logs)
type LogGenerator struct {
	moduleNames  []string
	projectNames []string
	actionTypes  map[LogCategory][]string
	userIDs      map[string][]string
	realUserIDs  []string
}

// NewLogGenerator creates a new log generator
func NewLogGenerator() *LogGenerator {
	return &LogGenerator{
		moduleNames: []string{
			"access_control", "user_service", "payment", "notification",
			"database", "cache", "queue", "search", "analytics", "api_gateway",
		},
		projectNames: []string{
			"user-service", "payment-service", "order-service", "inventory-service",
			"notification-service", "analytics-platform", "admin-dashboard",
		},
		actionTypes: map[LogCategory][]string{
			CategoryManagement: {
				"user_login", "user_logout", "user_permission_update",
				"role_assignment", "password_change", "account_lock",
				"system_config_change", "security_alert",
			},
			CategoryDevelopment: {
				"code_commit", "code_review", "merge_request",
				"build_start", "build_complete", "build_failed",
				"deploy_start", "deploy_complete", "deploy_failed",
				"feature_toggle", "bug_fix", "refactoring",
			},
			CategoryTest: {
				"test_start", "test_complete", "test_failed",
				"test_coverage", "regression_test", "integration_test",
				"unit_test", "performance_test", "security_scan",
			},
			CategoryOperations: {
				"server_start", "server_stop", "server_restart",
				"db_backup", "db_restore", "cache_clear",
				"disk_cleanup", "log_rotation", "health_check",
				"auto_scaling", "failover", "incident_report",
			},
		},
		userIDs: map[string][]string{
			"management":  {"admin", "manager1", "security_officer"},
			"development": {"dev1", "dev2", "dev3", "tech_lead"},
			"test":        {"tester1", "tester2", "qa_engineer"},
			"operations":  {"ops1", "ops2", "sre_engineer", "dba"},
		},
		realUserIDs: []string{},
	}
}

// SetRealUserIDs sets the real user ID list
func (lg *LogGenerator) SetRealUserIDs(userIDs []string) {
	lg.realUserIDs = userIDs
}

// GenerateLog generates a single log
func (lg *LogGenerator) GenerateLog(category LogCategory) *SoftwareFactoryLog {
	var userID string

	// Use the real user IDs when available
	if len(lg.realUserIDs) > 0 {
		userID = lg.realUserIDs[time.Now().UnixNano()%int64(len(lg.realUserIDs))]
	} else {
		// Otherwise fall back to the default user IDs
		userIDs := lg.userIDs[string(category)]
		userID = userIDs[time.Now().UnixNano()%int64(len(userIDs))]
	}

	actionTypes := lg.actionTypes[category]
	action := actionTypes[time.Now().UnixNano()%int64(len(actionTypes))]

	module := lg.moduleNames[time.Now().UnixNano()%int64(len(lg.moduleNames))]
	project := lg.projectNames[time.Now().UnixNano()%int64(len(lg.projectNames))]

	// Determine the log level
	var level string
	switch action {
	case "build_failed", "deploy_failed", "test_failed", "server_stop", "security_alert":
		level = "error"
	case "user_permission_update", "server_restart", "failover", "account_lock":
		level = "warning"
	default:
		level = "info"
	}

	// Determine the status
	status := "success"
	if level == "error" {
		status = "failed"
	}

	log := &SoftwareFactoryLog{
		ID:        lg.generateID(),
		Category:  category,
		Level:     level,
		Timestamp: float64(time.Now().UnixNano()) / 1e6,
		UserID:    userID,
		Module:    module,
		Project:   project,
		Operation: action,
		Status:    status,
		Message:   lg.GenerateMessage(action, userID, status),
		Processed: false,
		CreatedAt: time.Now(),
	}

	return log
}

// GenerateDetails generates the detail map
func (lg *LogGenerator) GenerateDetails(action, status string) map[string]interface{} {
	details := make(map[string]interface{})

	switch action {
	case "code_commit":
		details["commit_hash"] = lg.generateCommitHash()
		details["files_changed"] = randInt(1, 20)
		details["lines_added"] = randInt(0, 500)
		details["lines_removed"] = randInt(0, 200)
		details["branch"] = "main"

	case "build_start", "build_complete", "build_failed":
		details["build_id"] = fmt.Sprintf("build-%d", time.Now().Unix())
		details["build_number"] = randInt(100, 1000)
		details["duration_seconds"] = randFloat(10, 300)
		if action == "build_failed" {
			details["error_count"] = randInt(1, 10)
		}

	case "deploy_start", "deploy_complete", "deploy_failed":
		details["deployment_id"] = fmt.Sprintf("deploy-%d", time.Now().Unix())
		details["environment"] = []string{"staging", "production"}[randInt(0, 2)]
		details["version"] = fmt.Sprintf("v%d.%d.%d", randInt(1, 5), randInt(0, 20), randInt(0, 100))
		details["rollback_available"] = true

	case "test_start", "test_complete", "test_failed":
		details["test_suite"] = fmt.Sprintf("suite-%d", randInt(1, 50))
		details["test_count"] = randInt(50, 500)
		details["passed_count"] = randInt(40, 490)
		details["failed_count"] = randInt(0, 10)
		details["duration_seconds"] = randFloat(30, 600)
		details["coverage"] = fmt.Sprintf("%.1f%%", randFloat(60, 95))

	case "user_login", "user_logout":
		details["session_id"] = lg.generateID()
		details["browser"] = []string{"Chrome", "Firefox", "Safari", "Edge"}[randInt(0, 4)]
		details["os"] = []string{"Windows", "macOS", "Linux", "iOS", "Android"}[randInt(0, 5)]

	case "server_start", "server_stop", "server_restart":
		details["server_id"] = fmt.Sprintf("server-%s", lg.generateID()[:8])
		details["instance_type"] = []string{"t3.medium", "t3.large", "m5.large"}[randInt(0, 3)]
		details["region"] = []string{"us-east-1", "eu-west-1", "ap-northeast-1"}[randInt(0, 3)]

	case "db_backup", "db_restore":
		details["database"] = []string{"postgres", "mysql", "mongodb", "redis"}[randInt(0, 4)]
		details["backup_size_gb"] = randFloat(1, 100)
		details["duration_seconds"] = randFloat(60, 1800)

	default:
		details["action"] = action
		details["status"] = status
	}

	return details
}

// GenerateMessage generates the log message
func (lg *LogGenerator) GenerateMessage(action, userID, status string) string {
	actionMessages := map[string]string{
		"code_commit":     fmt.Sprintf("User %s committed code changes", userID),
		"build_complete":  fmt.Sprintf("Build completed successfully by %s", userID),
		"build_failed":    fmt.Sprintf("Build failed for user %s", userID),
		"deploy_complete": fmt.Sprintf("Deployment completed successfully by %s", userID),
		"deploy_failed":   fmt.Sprintf("Deployment failed for user %s", userID),
		"test_complete":   fmt.Sprintf("Test suite completed by %s", userID),
		"user_login":      fmt.Sprintf("User %s logged in", userID),
		"user_logout":     fmt.Sprintf("User %s logged out", userID),
		"server_restart":  fmt.Sprintf("Server restarted by %s", userID),
		"db_backup":       fmt.Sprintf("Database backup completed by %s", userID),
	}

	if msg, ok := actionMessages[action]; ok {
		return msg
	}
	return fmt.Sprintf("User %s performed action: %s (%s)", userID, action, status)
}

// GenerateBatch generates logs in batch
func (lg *LogGenerator) GenerateBatch(count int) []*SoftwareFactoryLog {
	logs := make([]*SoftwareFactoryLog, count)
	categories := []LogCategory{
		CategoryManagement, CategoryDevelopment, CategoryTest, CategoryOperations,
	}

	for i := 0; i < count; i++ {
		category := categories[time.Now().UnixNano()/100%4]
		log.Printf("Generated %s category log %d", category, time.Now().UnixNano())
		logs[i] = lg.GenerateLog(category)
	}

	return logs
}

// GenerateLogsForCategory generates logs for the given category
func (lg *LogGenerator) GenerateLogsForCategory(category LogCategory, count int) []*SoftwareFactoryLog {
	logs := make([]*SoftwareFactoryLog, count)
	for i := 0; i < count; i++ {
		logs[i] = lg.GenerateLog(category)
	}
	return logs
}

// generateID generates a unique ID
func (lg *LogGenerator) generateID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// generateCommitHash generates a commit hash
func (lg *LogGenerator) generateCommitHash() string {
	bytes := make([]byte, 7)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// generateIP generates a random IP
func (lg *LogGenerator) generateIP() string {
	return fmt.Sprintf("192.168.%d.%d", randInt(0, 255), randInt(1, 254))
}

// generateUserAgent generates a random User-Agent
func (lg *LogGenerator) generateUserAgent() string {
	browsers := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
	}
	return fmt.Sprintf("%s (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36", browsers[randInt(0, 3)])
}

// randInt generates a random integer in [min, max)
func randInt(min, max int) int {
	if max <= min {
		return min
	}
	return min + int(time.Now().UnixNano()/100%int64(max-min))
}

// randFloat generates a random float
func randFloat(min, max float64) float64 {
	if max <= min {
		return min
	}
	// Use a more uniformly distributed random source
	var b [8]byte
	rand.Read(b[:])
	r := float64(binary.LittleEndian.Uint64(b[:])) / (1 << 64)
	return min + r*(max-min)
}
