package discovery

import "time"

// NodeState define the node state type
type NodeState int32

const (
	// NodeUp node starting up
	NodeUp NodeState = 0
	// NodeDown node shutdown
	NodeDown NodeState = 1
	// NodeKeepalive node keepalive
	NodeKeepalive NodeState = 2

	DefaultPublishPrefix   = "node.publish"
	DefaultDiscoveryPrefix = "node.discovery"

	DefaultLivecycle = 2 * time.Second
	DefaultExpire    = 5
)

type Action string

const (
	Save   Action = "save"
	Update Action = "update"
	Delete Action = "delete"
	Get    Action = "get"
)

type Protocol string

const (
	GRPC    Protocol = "grpc"
	NGRPC   Protocol = "nats-grpc"
	JSONRPC Protocol = "json-rpc"
)

type RPC struct {
	Protocol Protocol
	Addr     string
	Params   map[string]string
}

// Node represents a node info
type Node struct {
	DC        string
	Service   string
	NID       string
	RPC       RPC
	ExtraInfo map[string]interface{}
}

// ID return the node id with scheme prefix
func (n *Node) ID() string {
	return n.DC + "." + n.NID
}

type Request struct {
	Action  Action
	Node    Node
	Service string
	Params  map[string]interface{}
}

type Response struct {
	Success bool
	Reason  string
}

type GetResponse struct {
	Nodes []Node
}
