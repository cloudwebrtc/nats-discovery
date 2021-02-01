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

// AppState define the node state type
type AppState int32

const (
	// AppUp app starting up
	AppUp AppState = 0
	// AppDown app shutdown
	AppDown AppState = 1
	// AppKeepalive app keepalive
	AppKeepalive AppState = 2

	DefaultPublishPrefix   = "app.publish"
	DefaultDiscoveryPrefix = "app.discovery"

	DefaultLivecycle = 2 * time.Second

	Save   = "save"
	Update = "update"
	Delete = "delete"
	Get    = "get"

	DefaultExpire = 5
)

// Node represents a node info
type Node struct {
	DC      string
	Service string
	NID     string
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
	s.nc.Close()
}

// NewService create a service instance
func NewRegistry(natsURL string) (*Registry, error) {
	opts := []nats.Option{nats.Name("nats-discovery registry server")}
	// Connect to the NATS server.
	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		log.Errorf("%v", err)
		return nil, err
	}

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
			log.Infof("app.delete %v, %v", subj, key)
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
				log.Infof("app.save")
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
				log.Infof("app.update")
				node.expire = time.Now().Unix() + DefaultExpire
			}
		case Delete:
			if _, ok := s.nodes[nid]; ok {
				log.Infof("app.delete")
				subj := strings.ReplaceAll(msg.Subject, DefaultPublishPrefix, DefaultDiscoveryPrefix)
				s.nc.Publish(subj, msg.Data)
			}
			delete(s.nodes, nid)
		case Get:
			log.Infof("app.get")
			resp := &GetResponse{}
			for _, item := range s.nodes {
				resp.Nodes = append(resp.Nodes, *item.node)
			}
			data, err := util.Marshal(resp)
			if err != nil {
				log.Errorf("%v", err)
				return err
			}
			log.Infof("app.get")
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
