package registry

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cloudwebrtc/nats-discovery/pkg/discovery"
	"github.com/cloudwebrtc/nats-discovery/pkg/util"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
)

var (
	logger = log.NewLogger(log.InfoLevel, "nats-discovery.Registry")
)

type NodeItem struct {
	subj   string
	expire time.Duration
	node   *discovery.Node
}

type Registry struct {
	nc     *nats.Conn
	ctx    context.Context
	cancel context.CancelFunc
	expire time.Duration
}

func (s *Registry) Close() {
	s.cancel()
}

// NewService create a service instance
func NewRegistry(nc *nats.Conn, expire time.Duration) (*Registry, error) {
	s := &Registry{
		nc:     nc,
		expire: expire,
	}

	if s.expire <= 0 {
		s.expire = discovery.DefaultExpire
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	return s, nil
}

func (s *Registry) checkExpires(nodes map[string]*NodeItem, now time.Duration, handleNodeAction func(action discovery.Action, node discovery.Node) (bool, error)) error {
	for key, item := range nodes {
		if item.expire <= now {
			discoverySubj := strings.ReplaceAll(item.subj, discovery.DefaultPublishPrefix, discovery.DefaultDiscoveryPrefix)
			logger.Infof("node.delete %v, %v", discoverySubj, key)
			d, err := util.Marshal(&discovery.Request{
				Action: discovery.Delete, Node: *item.node,
			})
			if err != nil {
				logger.Errorf("%v", err)
				return err
			}

			if err := s.nc.Publish(discoverySubj, d); err != nil {
				logger.Errorf("node start error: err=%v, id=%s", err, item.node.ID())
				return nil
			}
			handleNodeAction(discovery.Delete, *item.node)
			delete(nodes, key)
		}
	}
	return nil
}

func (s *Registry) Listen(
	handleNodeAction func(action discovery.Action, node discovery.Node) (bool, error),
	handleGetNodes func(service string, params map[string]interface{}) ([]discovery.Node, error)) error {

	if handleNodeAction == nil || handleGetNodes == nil {
		err := fmt.Errorf("Listen callback must be set for Registry.Listen")
		logger.Warnf("Listen: err => %v", err)
		return err
	}

	subj := discovery.DefaultPublishPrefix + ".>"
	msgCh := make(chan *nats.Msg)

	sub, err := s.nc.Subscribe(subj, func(msg *nats.Msg) {
		msgCh <- msg
	})

	if err != nil {
		return err
	}

	logger.Infof("Registry: listen subj prefix => %v", subj)

	nodes := make(map[string]*NodeItem)

	handleNatsMsg := func(msg *nats.Msg) error {
		logger.Debugf("handle storage key: %v", msg.Subject)
		var req discovery.Request
		err := util.Unmarshal(msg.Data, &req)
		if err != nil {
			logger.Errorf("connect: error parsing offer: %v", err)
			return err
		}
		nid := req.Node.ID()

		resp := discovery.Response{
			Success: true,
		}
		switch req.Action {
		case discovery.Save:
			if _, ok := nodes[nid]; !ok {
				logger.Infof("node.save")
				// accept or reject
				if ok, err := handleNodeAction(req.Action, req.Node); !ok {
					logger.Errorf("aciont %v, rejected %v", req.Action, err)
					resp.Success = false
					resp.Reason = fmt.Sprint(err)
					break
				}

				discoverySubj := strings.ReplaceAll(msg.Subject, discovery.DefaultPublishPrefix, discovery.DefaultDiscoveryPrefix)
				s.nc.Publish(discoverySubj, msg.Data)

				nodes[nid] = &NodeItem{
					expire: time.Duration(time.Now().UnixNano()) + s.expire,
					node:   &req.Node,
					subj:   msg.Subject,
				}
			}
		case discovery.Update:
			if node, ok := nodes[nid]; ok {
				logger.Debugf("node.update")
				node.expire = time.Duration(time.Now().UnixNano()) + s.expire
				if ok, err := handleNodeAction(req.Action, req.Node); !ok {
					logger.Errorf("aciont %v, rejected %v", req.Action, err)
					resp.Success = false
					resp.Reason = fmt.Sprint(err)
				}
			} else {
				req.Action = discovery.Save
				if ok, err := handleNodeAction(req.Action, req.Node); !ok {
					logger.Errorf("aciont %v, rejected %v", req.Action, err)
					resp.Success = false
					resp.Reason = fmt.Sprint(err)
					break
				}
				discoverySubj := strings.ReplaceAll(msg.Subject, discovery.DefaultPublishPrefix, discovery.DefaultDiscoveryPrefix)
				s.nc.Publish(discoverySubj, msg.Data)
				nodes[nid] = &NodeItem{
					expire: time.Duration(time.Now().UnixNano()) + s.expire,
					node:   &req.Node,
					subj:   msg.Subject,
				}
			}
		case discovery.Delete:
			if _, ok := nodes[nid]; ok {
				logger.Infof("node.delete")
				if ok, err := handleNodeAction(req.Action, req.Node); !ok {
					logger.Errorf("aciont %v, rejected %v", req.Action, err)
					resp.Success = false
					resp.Reason = fmt.Sprint(err)
					break
				}
				discoverySubj := strings.ReplaceAll(msg.Subject, discovery.DefaultPublishPrefix, discovery.DefaultDiscoveryPrefix)
				s.nc.Publish(discoverySubj, msg.Data)
			}
			delete(nodes, nid)
		case discovery.Get:
			resp := &discovery.GetResponse{}
			if nodes, err := handleGetNodes(req.Service, req.Params); err == nil {
				resp.Nodes = nodes
			}
			data, err := util.Marshal(resp)
			if err != nil {
				logger.Errorf("%v", err)
				return err
			}
			msg.Respond(data)
			return nil
		default:
			logger.Warnf("unkonw message: %v", msg.Data)
			return fmt.Errorf("unkonw message: %v", msg.Data)
		}

		data, err := util.Marshal(&resp)
		if err != nil {
			logger.Errorf("%v", err)
			return err
		}

		s.nc.Publish(msg.Reply, data)
		return nil
	}

	go func() error {

		defer func() {
			sub.Unsubscribe()
			close(msgCh)
		}()

		t := time.NewTicker(s.expire / 2)
		for {
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case <-t.C:
				if err := s.checkExpires(nodes, time.Duration(time.Now().UnixNano()), handleNodeAction); err != nil {
					logger.Warnf("checkExpires err: %v", err)
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
