package config

import (
	"os"

	"sfchain/pkg/types"

	"gopkg.in/yaml.v2"
)

// NodeConfig is the node configuration structure
type NodeConfig struct {
	Node            NodeInfo               `yaml:"node" json:"node"`
	Network         NetworkConfig          `yaml:"network" json:"network"`
	Database        DatabaseConfig         `yaml:"database" json:"database"`
	Blockchain      map[string]ChainConfig `yaml:"blockchain" json:"blockchain"`
	API             APIConfig              `yaml:"api" json:"api"`
	Consensus       ConsensusConfig        `yaml:"consensus" json:"consensus"`
	Logging         LoggingConfig          `yaml:"logging" json:"logging"`
	Monitoring      MonitoringConfig       `yaml:"monitoring" json:"monitoring"`
	Users           UsersConfig            `yaml:"users" json:"users"`
	BlockGeneration BlockGenerationConfig  `yaml:"block_generation" json:"block_generation"`
}

type NodeInfo struct {
	NodeID     string `yaml:"id" json:"id"`
	NodeType   string `yaml:"type" json:"type"`
	Port       int    `yaml:"port" json:"port"`
	Address    string `yaml:"address" json:"address"`
	PublicKey  string `yaml:"public_key" json:"public_key"`
	PrivateKey string `yaml:"private_key" json:"private_key"`
}

type NetworkConfig struct {
	Nodes []NodeInfo `yaml:"nodes" json:"nodes"`
}

type DatabaseConfig struct {
	Type     string `yaml:"type" json:"type"`
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	User     string `yaml:"user" json:"user"`
	Password string `yaml:"password" json:"password"`
	DBName   string `yaml:"dbname" json:"dbname"`
	Charset  string `yaml:"charset" json:"charset"`
}

type ChainConfig struct {
	BlockInterval        int `yaml:"block_interval" json:"block_interval"`
	TransactionThreshold int `yaml:"transaction_threshold" json:"transaction_threshold"`
}

type APIConfig struct {
	Enabled            bool     `yaml:"enabled" json:"enabled"`
	Port               int      `yaml:"port" json:"port"`
	CORSAllowedOrigins []string `yaml:"cors_allowed_origins" json:"cors_allowed_origins"`
	RateLimit          int      `yaml:"rate_limit" json:"rate_limit"`
}

type ConsensusConfig struct {
	SignatureTimeout int     `yaml:"signature_timeout" json:"signature_timeout"`
	MinSignatures    int     `yaml:"min_signatures" json:"min_signatures"`
	ThresholdRatio   float64 `yaml:"threshold_ratio" json:"threshold_ratio"`
	ManagerPublicKey string  `yaml:"manager_public_key" json:"manager_public_key"`
}

type LoggingConfig struct {
	Level      string `yaml:"level" json:"level"`
	Path       string `yaml:"path" json:"path"`
	MaxSize    int    `yaml:"max_size" json:"max_size"`
	MaxBackups int    `yaml:"max_backups" json:"max_backups"`
	MaxAge     int    `yaml:"max_age" json:"max_age"`
}

type MonitoringConfig struct {
	Enabled     bool   `yaml:"enabled" json:"enabled"`
	Port        int    `yaml:"port" json:"port"`
	MetricsPath string `yaml:"metrics_path" json:"metrics_path"`
}

type UsersConfig struct {
	InitialUsers []UserInfo `yaml:"initial_users" json:"initial_users"`
}

type UserInfo struct {
	ID        string `yaml:"id" json:"id"`
	Name      string `yaml:"name" json:"name"`
	Role      string `yaml:"role" json:"role"`
	PublicKey string `yaml:"public_key" json:"public_key"`
}

type BlockGenerationConfig struct {
	BlockSize   int  `yaml:"block_size" json:"block_size"`
	TpsPriority bool `yaml:"tps_priority" json:"tps_priority"`
}

// LoadConfig loads the full configuration from a file
func LoadConfig(configPath string) (*NodeConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var config NodeConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// ToTypesNodeInfo converts config.NodeInfo into types.NodeInfo
func (ni *NodeInfo) ToTypesNodeInfo() types.NodeInfo {
	return types.NodeInfo{
		NodeID:     ni.NodeID,
		NodeType:   types.NodeTypeFromString(ni.NodeType),
		Port:       ni.Port,
		Address:    ni.Address,
		PublicKey:  ni.PublicKey,
		PrivateKey: ni.PrivateKey,
	}
}
