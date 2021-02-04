package registry

import (
	"context"
	"fmt"
	"io"
	"strings"
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
	DC      string
	Service string
	NID     string
	RPC     RPC
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
	nc     *nats.Conn
	sub    *nats.Subscription
	nodes  map[string]*NodeItem
	ctx    context.Context
	cancel context.CancelFunc
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

			delete(s.nodes, key)
		}
	}
	return nil
}

func (s *Registry) Listen() error {
	var err error
	subj := DefaultPublishPrefix + ".>"

	msgCh := make(chan *nats.Msg)

	if s.sub, err = s.nc.ChanSubscribe(subj, msgCh); err != nil {
		return err
	}

	log.Infof("Registry: listen prefix => %v", subj)

	handleMsg := func(msg *nats.Msg) error {
		log.Infof("handle storage key: %v", msg.Subject)
		var event KeepAlive
		err := util.Unmarshal(msg.Data, &event)
		if err != nil {
			log.Errorf("connect: error parsing offer: %v", err)
			return err
		}
		nid := event.Node.ID()
		switch event.Action {
		case Save:
			if _, ok := s.nodes[nid]; !ok {
				log.Infof("node.save")
				subj := strings.ReplaceAll(msg.Subject, DefaultPublishPrefix, DefaultDiscoveryPrefix)
				s.nc.Publish(subj, msg.Data)
				s.nodes[nid] = &NodeItem{
					expire: time.Now().Unix() + DefaultExpire,
					node:   &event.Node,
					subj:   msg.Subject,
				}
			}
		case Update:
			if node, ok := s.nodes[nid]; ok {
				log.Infof("node.update")
				node.expire = time.Now().Unix() + DefaultExpire
			}
		case Delete:
			if _, ok := s.nodes[nid]; ok {
				log.Infof("node.delete")
				subj := strings.ReplaceAll(msg.Subject, DefaultPublishPrefix, DefaultDiscoveryPrefix)
				s.nc.Publish(subj, msg.Data)
			}
			delete(s.nodes, nid)
		case Get:
			log.Infof("node.get")
			resp := &GetResponse{}
			for _, item := range s.nodes {
				if strings.Contains(item.subj, msg.Subject) {
					resp.Nodes = append(resp.Nodes, *item.node)
				}
			}
			data, err := util.Marshal(resp)
			if err != nil {
				log.Errorf("%v", err)
				return err
			}
			s.nc.Publish(msg.Reply, data)
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
