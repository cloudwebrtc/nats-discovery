package discovery

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/cloudwebrtc/nats-discovery/pkg/util"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
)

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

	Save   = "save"
	Update = "update"
	Delete = "delete"
	Get    = "get"

	DefaultExpire = 5
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

type KeepAlive struct {
	Action string
	Node   Node
}

type GetResponse struct {
	Nodes []Node
}

type NodeItem struct {
	subj   string
	expire int64
	node   *Node
}

type Registry struct {
	nc         *nats.Conn
	sub        *nats.Subscription
	nodes      map[string]*NodeItem
	ctx        context.Context
	cancel     context.CancelFunc
	mutex      sync.Mutex
	handleNode func(action string, node Node)
}

func (s *Registry) Close() {
	s.cancel()
	s.sub.Unsubscribe()
}

// NewService create a service instance
func NewRegistry(nc *nats.Conn) (*Registry, error) {
	s := &Registry{
		nc:    nc,
		nodes: make(map[string]*NodeItem),
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	return s, nil
}

func (s *Registry) checkExpires(now int64) error {
	for key, item := range s.nodes {
		if item.expire <= now {
			subj := strings.ReplaceAll(item.subj, DefaultPublishPrefix, DefaultDiscoveryPrefix)
			log.Infof("node.delete %v, %v", subj, key)
			d, err := util.Marshal(&KeepAlive{
				Action: Delete, Node: *item.node,
			})
			if err != nil {
				log.Errorf("%v", err)
				return err
			}

			if err := s.nc.Publish(subj, d); err != nil {
				log.Errorf("node start error: err=%v, id=%s", err, item.node.ID())
				return nil
			}
			s.mutex.Lock()
			defer s.mutex.Unlock()
			s.handleNode(Delete, *item.node)
			delete(s.nodes, key)
		}
	}
	return nil
}

func (s *Registry) GetNodes(service string) ([]Node, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	nodes := []Node{}
	for _, item := range s.nodes {
		if strings.Contains(item.node.Service, service) || service == "*" {
			nodes = append(nodes, *item.node)
		}
	}
	return nodes, nil
}

func (s *Registry) Listen(handleNode func(action string, node Node)) error {
	var err error
	subj := DefaultPublishPrefix + ".>"
	msgCh := make(chan *nats.Msg)
	s.handleNode = handleNode

	if s.sub, err = s.nc.Subscribe(subj, func(msg *nats.Msg) {
		msgCh <- msg
	}); err != nil {
		return err
	}

	log.Infof("Registry: listen prefix => %v", subj)

	handleMsg := func(msg *nats.Msg) error {
		log.Debugf("handle storage key: %v", msg.Subject)
		var event KeepAlive
		err := util.Unmarshal(msg.Data, &event)
		if err != nil {
			log.Errorf("connect: error parsing offer: %v", err)
			return err
		}
		nid := event.Node.ID()
		switch event.Action {
		case Save:
			s.mutex.Lock()
			if _, ok := s.nodes[nid]; !ok {
				log.Infof("node.save")
				subj := strings.ReplaceAll(msg.Subject, DefaultPublishPrefix, DefaultDiscoveryPrefix)
				s.nc.Publish(subj, msg.Data)
				s.nodes[nid] = &NodeItem{
					expire: time.Now().Unix() + DefaultExpire,
					node:   &event.Node,
					subj:   msg.Subject,
				}
				s.handleNode(event.Action, event.Node)
			}
			s.mutex.Unlock()
		case Update:
			s.mutex.Lock()
			if node, ok := s.nodes[nid]; ok {
				log.Debugf("node.update")
				node.expire = time.Now().Unix() + DefaultExpire
				s.handleNode(event.Action, event.Node)
			} else {
				event.Action = Save
				subj := strings.ReplaceAll(msg.Subject, DefaultPublishPrefix, DefaultDiscoveryPrefix)
				s.nc.Publish(subj, msg.Data)
				s.nodes[nid] = &NodeItem{
					expire: time.Now().Unix() + DefaultExpire,
					node:   &event.Node,
					subj:   msg.Subject,
				}
				s.handleNode(event.Action, event.Node)
			}
			s.mutex.Unlock()
		case Delete:
			if _, ok := s.nodes[nid]; ok {
				log.Infof("node.delete")
				subj := strings.ReplaceAll(msg.Subject, DefaultPublishPrefix, DefaultDiscoveryPrefix)
				s.nc.Publish(subj, msg.Data)
				s.handleNode(event.Action, event.Node)
			}
			delete(s.nodes, nid)
		case Get:
			log.Infof("node.get")
			resp := &GetResponse{}
			s.mutex.Lock()
			for _, item := range s.nodes {
				if strings.Contains(item.subj, msg.Subject) {
					resp.Nodes = append(resp.Nodes, *item.node)
				}
			}
			s.mutex.Unlock()
			data, err := util.Marshal(resp)
			if err != nil {
				log.Errorf("%v", err)
				return err
			}
			msg.Respond(data)
		default:
			log.Warnf("unkonw message: %v", msg.Data)
			return fmt.Errorf("unkonw message: %v", msg.Data)
		}
		return nil
	}

	go func() error {
		defer close(msgCh)
		now := time.Now().Unix()
		t := time.NewTicker(time.Second * 1)
		for {
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case <-t.C:
				now++
				if err := s.checkExpires(now); err != nil {
					log.Warnf("checkExpires err: %v", err)
					return err
				}
			case msg, ok := <-msgCh:
				if ok {
					err := handleMsg(msg)
					if err != nil {
						return err
					}
					break
				}
				return io.EOF
			}
		}
	}()

	return nil
}
