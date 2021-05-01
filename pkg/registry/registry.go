package registry

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/cloudwebrtc/nats-discovery/pkg/discovery"
	"github.com/cloudwebrtc/nats-discovery/pkg/util"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
)

type NodeItem struct {
	subj   string
	expire int64
	node   *discovery.Node
}

type Registry struct {
	nc     *nats.Conn
	sub    *nats.Subscription
	nodes  map[string]*NodeItem
	ctx    context.Context
	cancel context.CancelFunc
	mutex  sync.Mutex
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

func (s *Registry) checkExpires(now int64, handleNodeAction func(action discovery.Action, node discovery.Node) (bool, error)) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for key, item := range s.nodes {
		if item.expire <= now {
			discoverySubj := strings.ReplaceAll(item.subj, discovery.DefaultPublishPrefix, discovery.DefaultDiscoveryPrefix)
			log.Infof("node.delete %v, %v", discoverySubj, key)
			d, err := util.Marshal(&discovery.Request{
				Action: discovery.Delete, Node: *item.node,
			})
			if err != nil {
				log.Errorf("%v", err)
				return err
			}

			if err := s.nc.Publish(discoverySubj, d); err != nil {
				log.Errorf("node start error: err=%v, id=%s", err, item.node.ID())
				return nil
			}
			handleNodeAction(discovery.Delete, *item.node)
			delete(s.nodes, key)
		}
	}
	return nil
}

func (s *Registry) GetNodes(service string) ([]discovery.Node, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	nodes := []discovery.Node{}
	for _, item := range s.nodes {
		if item.node.Service == service || service == "*" {
			nodes = append(nodes, *item.node)
		}
	}
	return nodes, nil
}

func (s *Registry) Listen(
	handleNodeAction func(action discovery.Action, node discovery.Node) (bool, error),
	handleGetNodes func(service string, params map[string]interface{}) ([]discovery.Node, error)) error {
	var err error

	if handleNodeAction == nil || handleGetNodes == nil {
		err = fmt.Errorf("Listen callback must be set for Registry.Listen")
		log.Warnf("Listen: err => %v", err)
		return err
	}

	subj := discovery.DefaultPublishPrefix + ".>"
	msgCh := make(chan *nats.Msg)

	if s.sub, err = s.nc.Subscribe(subj, func(msg *nats.Msg) {
		msgCh <- msg
	}); err != nil {
		return err
	}

	log.Infof("Registry: listen prefix => %v", subj)

	handleNatsMsg := func(msg *nats.Msg) error {
		s.mutex.Lock()
		defer s.mutex.Unlock()

		log.Debugf("handle storage key: %v", msg.Subject)
		var req discovery.Request
		err := util.Unmarshal(msg.Data, &req)
		if err != nil {
			log.Errorf("connect: error parsing offer: %v", err)
			return err
		}
		nid := req.Node.ID()

		resp := discovery.Response{
			Success: true,
		}
		switch req.Action {
		case discovery.Save:
			if _, ok := s.nodes[nid]; !ok {
				log.Infof("node.save")
				// accept or reject
				if ok, err := handleNodeAction(req.Action, req.Node); !ok {
					log.Errorf("aciont %v, rejected %v", req.Action, err)
					resp.Success = false
					resp.Reason = fmt.Sprint(err)
					break
				}

				discoverySubj := strings.ReplaceAll(msg.Subject, discovery.DefaultPublishPrefix, discovery.DefaultDiscoveryPrefix)
				s.nc.Publish(discoverySubj, msg.Data)

				s.nodes[nid] = &NodeItem{
					expire: time.Now().Unix() + discovery.DefaultExpire,
					node:   &req.Node,
					subj:   msg.Subject,
				}
			}
		case discovery.Update:
			if node, ok := s.nodes[nid]; ok {
				log.Debugf("node.update")
				node.expire = time.Now().Unix() + discovery.DefaultExpire
				if ok, err := handleNodeAction(req.Action, req.Node); !ok {
					log.Errorf("aciont %v, rejected %v", req.Action, err)
					resp.Success = false
					resp.Reason = fmt.Sprint(err)
				}
			} else {
				req.Action = discovery.Save
				if ok, err := handleNodeAction(req.Action, req.Node); !ok {
					log.Errorf("aciont %v, rejected %v", req.Action, err)
					resp.Success = false
					resp.Reason = fmt.Sprint(err)
					break
				}
				discoverySubj := strings.ReplaceAll(msg.Subject, discovery.DefaultPublishPrefix, discovery.DefaultDiscoveryPrefix)
				s.nc.Publish(discoverySubj, msg.Data)
				s.nodes[nid] = &NodeItem{
					expire: time.Now().Unix() + discovery.DefaultExpire,
					node:   &req.Node,
					subj:   msg.Subject,
				}
			}
		case discovery.Delete:
			if _, ok := s.nodes[nid]; ok {
				log.Infof("node.delete")
				if ok, err := handleNodeAction(req.Action, req.Node); !ok {
					log.Errorf("aciont %v, rejected %v", req.Action, err)
					resp.Success = false
					resp.Reason = fmt.Sprint(err)
					break
				}
				discoverySubj := strings.ReplaceAll(msg.Subject, discovery.DefaultPublishPrefix, discovery.DefaultDiscoveryPrefix)
				s.nc.Publish(discoverySubj, msg.Data)
			}
			delete(s.nodes, nid)
		case discovery.Get:
			resp := &discovery.GetResponse{}
			s.mutex.Unlock()
			if nodes, err := handleGetNodes(req.Service, req.Params); err == nil {
				resp.Nodes = nodes
			}
			s.mutex.Lock()
			data, err := util.Marshal(resp)
			if err != nil {
				log.Errorf("%v", err)
				return err
			}
			msg.Respond(data)
			return nil
		default:
			log.Warnf("unkonw message: %v", msg.Data)
			return fmt.Errorf("unkonw message: %v", msg.Data)
		}

		data, err := util.Marshal(&resp)
		if err != nil {
			log.Errorf("%v", err)
			return err
		}
		s.nc.Publish(msg.Reply, data)
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
				if err := s.checkExpires(now, handleNodeAction); err != nil {
					log.Warnf("checkExpires err: %v", err)
					return err
				}
			case msg, ok := <-msgCh:
				if ok {
					err := handleNatsMsg(msg)
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
