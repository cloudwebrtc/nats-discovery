package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cloudwebrtc/nats-discovery/pkg/discovery"
	"github.com/cloudwebrtc/nats-discovery/pkg/util"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
)

var (
	logger = log.NewLogger(log.InfoLevel, "nats-discovery.Client")
)

type NodeStateChangeCallback func(state discovery.NodeState, node *discovery.Node)

type Client struct {
	nc       *nats.Conn
	nodes    map[string]*discovery.Node
	nodeLock sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
}

func (c *Client) Close() {
	c.cancel()
}

// NewService create a service instance
func NewClient(nc *nats.Conn) (*Client, error) {

	c := &Client{
		nc:    nc,
		nodes: make(map[string]*discovery.Node),
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c, nil
}

func (c *Client) Get(service string, params map[string]interface{}) (*discovery.GetResponse, error) {
	data, err := util.Marshal(&discovery.Request{
		Action:  discovery.Get,
		Service: service,
		Params:  params,
	})
	if err != nil {
		logger.Errorf("%v", err)
		return nil, err
	}
	subj := discovery.DefaultPublishPrefix + "." + service
	logger.Infof("Get: subj=%v", subj)
	msg, err := c.nc.Request(subj, data, 15*time.Second)
	if err != nil {
		logger.Errorf("Get: service=%v, err=%v", service, err)
		return nil, err
	}

	var resp discovery.GetResponse
	err = util.Unmarshal(msg.Data, &resp)
	if err != nil {
		logger.Errorf("Get: error parsing discovery.GetResponse: %v", err)
		return nil, err
	}

	logger.Infof("nodes %v", resp.Nodes)
	return &resp, nil
}

func (c *Client) handleNatsMsg(msg *nats.Msg, callback NodeStateChangeCallback) error {
	logger.Infof("handle discovery message: %v", msg.Subject)

	c.nodeLock.Lock()
	defer c.nodeLock.Unlock()

	var event discovery.Request
	err := util.Unmarshal(msg.Data, &event)
	if err != nil {
		logger.Errorf("connect: error parsing offer: %v", err)
		return err
	}
	nid := event.Node.ID()
	switch event.Action {
	case discovery.Save:
		if _, ok := c.nodes[nid]; !ok {
			logger.Infof("node.save")
			c.nodes[nid] = &event.Node
		}
		callback(discovery.NodeUp, &event.Node)
	case discovery.Update:
		if _, ok := c.nodes[nid]; ok {
			logger.Infof("node.update")
			c.nodes[nid] = &event.Node
		}
		callback(discovery.NodeKeepalive, &event.Node)
	case discovery.Delete:
		if _, ok := c.nodes[nid]; ok {
			logger.Infof("node.delete")
			delete(c.nodes, nid)
		}
		callback(discovery.NodeDown, &event.Node)
	default:
		err = fmt.Errorf("unkonw message: %v", msg.Data)
		logger.Warnf("handleNatsMsg: err => %v", err)
		return err
	}

	return nil
}

func (c *Client) Watch(service string, handleNodeState NodeStateChangeCallback) error {
	if handleNodeState == nil {
		err := fmt.Errorf("Watch callback must be set for %v", service)
		logger.Warnf("Watch: err => %v", err)
		return err
	}

	subj := discovery.DefaultDiscoveryPrefix + "." + service + ".>"
	msgCh := make(chan *nats.Msg)

	sub, err := c.nc.Subscribe(subj, func(msg *nats.Msg) {
		msgCh <- msg
	})

	if err != nil {
		return err
	}

	go func() error {
		defer sub.Unsubscribe()

		for {
			select {
			case <-c.ctx.Done():
				return c.ctx.Err()
			case msg, ok := <-msgCh:
				if ok {
					err := c.handleNatsMsg(msg, handleNodeState)
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

func (c *Client) KeepAlive(node discovery.Node) error {
	t := time.NewTicker(discovery.DefaultLivecycle)

	defer func() {
		c.sendAction(node, discovery.Delete)
		t.Stop()
	}()

	c.sendAction(node, discovery.Save)

	for {
		select {
		case <-c.ctx.Done():
			err := c.ctx.Err()
			logger.Errorf("keepalive abort: err %v", err)
			return err
		case <-t.C:
			c.sendAction(node, discovery.Update)
		}
	}
}

func (c *Client) sendAction(node discovery.Node, action discovery.Action) error {
	data, err := util.Marshal(&discovery.Request{
		Action: action, Node: node,
	})
	if err != nil {
		logger.Errorf("%v", err)
		return err
	}
	subj := discovery.DefaultPublishPrefix + "." + node.Service + "." + node.ID()
	msg, err := c.nc.Request(subj, data, time.Duration(time.Second*15))
	if err != nil {
		logger.Errorf("node start error: err=%v, id=%v", err, node.ID())
		return nil
	}

	var resp discovery.Response
	err = util.Unmarshal(msg.Data, &resp)
	if err != nil {
		logger.Errorf("sendAction: [%v] parsing discovery.Response error: %v", action, err)
		return err
	}

	if !resp.Success {
		err := fmt.Errorf("[%v] response error %v", action, resp.Reason)
		logger.Errorf("sendAction: error: %v", err)
		return err
	}
	return nil
}
