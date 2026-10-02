package database

import (
	"log"
	"sync"
	"time"
)

// LogManager is the log manager (standalone service)
type LogManager struct {
	DB               LogDatabaseInterface
	Generator        *LogGenerator
	StopChan         chan struct{}
	mu               sync.RWMutex
	AutoGenerate     bool
	GenerationConfig *GenerationConfig
}

// GenerationConfig is the log generation configuration
type GenerationConfig struct {
	Interval         time.Duration // generation interval
	BatchSize        int           // number generated per round
	TxThreshold      int           // transaction generation threshold
	EnableSimulation bool          // enable log simulation
}

// DefaultGenerationConfig returns the default configuration
func DefaultGenerationConfig() *GenerationConfig {
	return &GenerationConfig{
		Interval:         10 * time.Second,
		BatchSize:        20,
		TxThreshold:      10,
		EnableSimulation: true,
	}
}

// NewLogManager creates a new log manager (standalone service)
func NewLogManager(db LogDatabaseInterface) *LogManager {
	return &LogManager{
		DB:               db,
		Generator:        NewLogGenerator(),
		StopChan:         make(chan struct{}),
		AutoGenerate:     true,
		GenerationConfig: DefaultGenerationConfig(),
	}
}

// Start starts the log manager (log generation only, no transaction processing)
func (lm *LogManager) Start() {
	log.Println("Starting log generation service...")

	// Start the log simulation generator
	if lm.GenerationConfig.EnableSimulation {
		go lm.simulationLoop()
	}

	log.Println("Log generation service started")
}

// Stop stops the log manager
func (lm *LogManager) Stop() {
	close(lm.StopChan)
	log.Println("Log generation service stopped")
}

// SetGenerationConfig sets the generation configuration
func (lm *LogManager) SetGenerationConfig(config *GenerationConfig) {
	lm.GenerationConfig = config
}

// simulationLoop is the log simulation generation loop
func (lm *LogManager) simulationLoop() {
	ticker := time.NewTicker(lm.GenerationConfig.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			lm.generateSimulationLogs()
		case <-lm.StopChan:
			return
		}
	}
}

// generateSimulationLogs generates simulated logs
func (lm *LogManager) generateSimulationLogs() {
	// Generate logs of each category at random
	sfLogs := lm.Generator.GenerateBatch(lm.GenerationConfig.BatchSize)

	// Convert to []interface{}
	logs := make([]interface{}, len(sfLogs))
	for i, log := range sfLogs {
		logs[i] = log
	}

	if err := lm.DB.AddLogs(logs); err != nil {
		log.Printf("Failed to generate simulated logs: %v", err)
		return
	}

	log.Printf("Generated %d simulated logs", len(logs))
}

// Note: transaction processing has been removed; only log generation remains.
// The management node processes transactions by reading logs directly from the
// log database.
