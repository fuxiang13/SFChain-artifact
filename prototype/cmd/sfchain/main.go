package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sfchain/internal/core"
	"sfchain/internal/network"
	config "sfchain/pkg/config"
	"sfchain/pkg/types"
	"syscall"
	"time"
)

func main() {
	// log timestamps precise to the microsecond (performance comparison experiments need sub-second log parsing resolution)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// parse command-line flags
	nodeType := flag.String("type", "", "node type (management|development|test|operations)")
	nodePort := flag.Int("port", 0, "node port")
	nodeID := flag.String("id", "", "node ID")
	configPath := flag.String("config", "", "configuration file path")
	logInterval := flag.Int("log-interval", 0, "log processing interval (seconds); 0=do not override (keep the SFCHAIN_LOG_POLL_INTERVAL env/default)")

	flag.Parse()

	// prefer the configuration file
	if *configPath != "" {
		log.Printf("starting node with configuration file: %s", *configPath)
		startWithConfig(*configPath, *logInterval)
	} else {
		log.Printf("starting node with command-line flags: type=%s, id=%s, port=%d, log-interval=%d", *nodeType, *nodeID, *nodePort, *logInterval)
		startWithFlags(*nodeType, *nodeID, *nodePort, *logInterval)
	}
}

func startWithConfig(configPath string, logInterval int) {
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("failed to load configuration file: %v", err)
	}

	fmt.Printf("=== starting %s node (%s:%d) ===\n", cfg.Node.NodeType, cfg.Node.NodeID, cfg.Node.Port)

	// create and initialize the node; enhanced mode is always enabled
	node, err := createAndInitNode(cfg)
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}

	// start the corresponding features based on node type
	switch n := node.(type) {
	case *core.ManagementNode:
		// set the log processing interval (0 = do not override; keep env SFCHAIN_LOG_POLL_INTERVAL)
		if logInterval > 0 {
			n.LogProcessor.SetInterval(time.Duration(logInterval) * time.Second)
			log.Printf("log processing interval set to: %ds", logInterval)
		}

		// always enable enhanced-mode features
		go n.GenerateBlocks()
		log.Printf("management node block generator started")

		// start the log processor
		go n.LogProcessor.Start()
		log.Printf("log processor started")

		// start the HTTP server
		n.NodeAPI = network.NewNodeAPI(n)
		go func() {
			if err := n.NodeAPI.Start(cfg.Node.Port); err != nil {
				log.Printf("HTTP server failed to start: %v", err)
			}
		}()
		log.Printf("HTTP server started, listening on port: %d", cfg.Node.Port)
	case *core.DTONode:
		log.Printf("%s node started, waiting to receive blocks", n.GetNodeCategory().String())

		// start the HTTP server
		n.NodeAPI = network.NewNodeAPI(n)
		go func() {
			if err := n.NodeAPI.Start(cfg.Node.Port); err != nil {
				log.Printf("HTTP server failed to start: %v", err)
			}
		}()
		log.Printf("HTTP server started, listening on port: %d", cfg.Node.Port)
	}

	// print node information
	fmt.Printf("================================================================================\n")
	fmt.Printf("========================== SFCHAIN NODE START MARKER v2 ==================================\n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("node type: %s\n", cfg.Node.NodeType)
	fmt.Printf("node ID: %s\n", cfg.Node.NodeID)
	fmt.Printf("listening port: %d\n", cfg.Node.Port)
	fmt.Printf("network node count: %d\n", len(cfg.Network.Nodes))
	fmt.Printf("database: MySQL\n")
	fmt.Printf("%s node started successfully!\n", cfg.Node.NodeType)
	fmt.Printf("================================================================================\n")

	// wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	fmt.Println("\nstop signal received, shutting down service...")

	// release database manager locks
	core.GetGlobalUserDatabaseManager().Close()
	core.GetGlobalLogDBManager().Close()
	core.GetGlobalTxPoolManager().Close()

	fmt.Println("node service stopped")
}

func startWithFlags(nodeType, nodeID string, nodePort int, logInterval int) {
	if nodeType == "" || nodePort == 0 || nodeID == "" {
		log.Fatal("node type, port, and ID must be specified")
	}

	fmt.Printf("=== starting %s node (%s:%d) ===\n", nodeType, nodeID, nodePort)

	node, err := createNode(nodeType, nodeID, nodePort)
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}

	// start special features for the management node
	if n, ok := node.(*core.ManagementNode); ok {
		// create the default configuration
		cfg := &config.NodeConfig{
			Node: config.NodeInfo{
				NodeID:    nodeID,
				NodeType:  nodeType,
				Port:      nodePort,
				PublicKey: "management_public_key", // default public key
			},
			Database: config.DatabaseConfig{
				Type:     "mysql",
				Host:     "localhost",
				Port:     3306,
				User:     "fx",
				Password: "123456",
				DBName:   "sfchain",
			},
		}

		// initialize the node with configuration
		if err := n.InitializeWithConfig(cfg); err != nil {
			log.Fatalf("failed to initialize management node: %v", err)
		}

		// set the log processing interval (0 = do not override; keep env SFCHAIN_LOG_POLL_INTERVAL)
		if logInterval > 0 {
			n.LogProcessor.SetInterval(time.Duration(logInterval) * time.Second)
			log.Printf("log processing interval set to: %ds", logInterval)
		}

		// always enable enhanced-mode features
		go n.GenerateBlocks()
		log.Printf("management node block generator started")

		// start the log processor
		go n.LogProcessor.Start()
		log.Printf("log processor started")

		// start the HTTP server
		n.NodeAPI = network.NewNodeAPI(n)
		go func() {
			if err := n.NodeAPI.Start(nodePort); err != nil {
				log.Printf("HTTP server failed to start: %v", err)
			}
		}()
		log.Printf("HTTP server started, listening on port: %d", nodePort)
	}

	// start special features for DTO nodes
	if n, ok := node.(*core.DTONode); ok {
		// create the default configuration
		cfg := &config.NodeConfig{
			Node: config.NodeInfo{
				NodeID:    nodeID,
				NodeType:  nodeType,
				Port:      nodePort,
				PublicKey: nodeType + "_public_key", // default public key
			},
			Database: config.DatabaseConfig{
				Type:     "mysql",
				Host:     "localhost",
				Port:     3306,
				User:     "fx",
				Password: "123456",
				DBName:   "sfchain",
			},
		}

		// initialize the node with configuration
		if err := n.InitializeWithConfig(cfg); err != nil {
			log.Fatalf("failed to initialize DTO node: %v", err)
		}

		// start the HTTP server
		n.NodeAPI = network.NewNodeAPI(n)
		go func() {
			if err := n.NodeAPI.Start(nodePort); err != nil {
				log.Printf("HTTP server failed to start: %v", err)
			}
		}()
		log.Printf("HTTP server started, listening on port: %d", nodePort)
	}

	fmt.Printf("%s node started successfully! listening port: %d\n", nodeType, nodePort)

	// wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	fmt.Println("\nstop signal received, shutting down service...")

	// release database manager locks
	core.GetGlobalUserDatabaseManager().Close()
	core.GetGlobalLogDBManager().Close()
	core.GetGlobalTxPoolManager().Close()

	fmt.Println("node service stopped")
}

func createAndInitNode(cfg *config.NodeConfig) (interface{}, error) {
	switch types.NodeTypeFromString(cfg.Node.NodeType) {
	case types.NodeTypeManagement:
		// management node (with BLS aggregate signature support)
		node, err := core.NewManagementNode(cfg.Node.NodeID, cfg.Node.Port)
		if err != nil {
			return nil, err
		}
		// no longer pass enableEnhanced; enhanced mode is always enabled
		if err := node.InitializeWithConfig(cfg); err != nil {
			return nil, err
		}
		return node, nil

	case types.NodeTypeDevelopment, types.NodeTypeTest, types.NodeTypeOperations:
		// development, test, and operations nodes use the common implementation
		nodeType := types.NodeTypeFromString(cfg.Node.NodeType)
		node := core.NewDTONode(cfg.Node.NodeID, cfg.Node.Port, nodeType)
		if err := node.InitializeWithConfig(cfg); err != nil {
			return nil, err
		}
		return node, nil

	default:
		return nil, fmt.Errorf("unsupported node type: %s", cfg.Node.NodeType)
	}
}

func createNode(nodeType, nodeID string, port int) (interface{}, error) {
	switch types.NodeTypeFromString(nodeType) {
	case types.NodeTypeManagement:
		return core.NewManagementNode(nodeID, port)
	case types.NodeTypeDevelopment, types.NodeTypeTest, types.NodeTypeOperations:
		nodeCategory := types.NodeTypeFromString(nodeType)
		return core.NewDTONode(nodeID, port, nodeCategory), nil
	default:
		return nil, fmt.Errorf("unsupported node type: %s", nodeType)
	}
}
