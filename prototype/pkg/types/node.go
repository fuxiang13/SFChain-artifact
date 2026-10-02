package types

// NodeInfo is the node information structure
type NodeInfo struct {
	NodeID     string
	NodeType   NodeType
	Port       int
	Address    string
	PublicKey  string
	PrivateKey string
}

// NodeType is the node type
type NodeType int

const (
	NodeTypeManagement  NodeType = 1
	NodeTypeDevelopment NodeType = 2
	NodeTypeTest        NodeType = 3
	NodeTypeOperations  NodeType = 4
)

// String returns the string representation of the node type
func (nt NodeType) String() string {
	switch nt {
	case NodeTypeManagement:
		return "management"
	case NodeTypeDevelopment:
		return "development"
	case NodeTypeTest:
		return "test"
	case NodeTypeOperations:
		return "operations"
	default:
		return "unknown"
	}
}

// NodeTypeFromString parses the node type from a string
func NodeTypeFromString(s string) NodeType {
	switch s {
	case "management":
		return NodeTypeManagement
	case "development":
		return NodeTypeDevelopment
	case "test":
		return NodeTypeTest
	case "operations":
		return NodeTypeOperations
	default:
		return NodeTypeManagement // defaults to the management node type
	}
}
