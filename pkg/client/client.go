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

type NodeStateChangeCallback func(state discovery.NodeState, node *discovery.Node)

type Client struct {
	nc                    *nats.Conn
	sub                   *nats.Subscription
	nodes                 map[string]*discovery.Node
	nodeLock              sync.Mutex
	ctx                   context.Context
	cancel                context.CancelFunc
	handleNodeStateChange NodeStateChangeCallback
}

func (c *Client) Close() {
	c.cancel()
	c.sub.Unsubscribe()
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

func (c *Client) Get(service string) (*discovery.GetResponse, error) {
	data, err := util.Marshal(&discovery.KeepAlive{
		Action: discovery.Get, Node: discovery.Node{},
	})
	if err != nil {
		log.Errorf("%v", err)
		return nil, err
	}
	subj := discovery.DefaultPublishPrefix + "." + service
	log.Infof("Get: subj=%v", subj)
	msg, err := c.nc.Request(subj, data, 15*time.Second)
	if err != nil {
		log.Errorf("Get: service=%v, err=%v", service, err)
		return nil, err
	}

	var resp discovery.GetResponse
	err = util.Unmarshal(msg.Data, &resp)
	if err != nil {
		log.Errorf("Get: error parsing offer: %v", err)
		return nil, err
	}

	log.Infof("nodes %v", resp.Nodes)
	return &resp, nil
}

func (c *Client) handleMsg(msg *nats.Msg) error {
	log.Infof("handle discovery message: %v", msg.Subject)

	c.nodeLock.Lock()
	defer c.nodeLock.Unlock()

	var event discovery.KeepAlive
	err := util.Unmarshal(msg.Data, &event)
	if err != nil {
		log.Errorf("connect: error parsing offer: %v", err)
		return err
	}
	nid := event.Node.ID()
	switch event.Action {
	case discovery.Save:
		if _, ok := c.nodes[nid]; !ok {
			log.Infof("node.save")
			c.nodes[nid] = &event.Node
			c.handleNodeStateChange(discovery.NodeUp, &event.Node)
		}
	case discovery.Update:
		if _, ok := c.nodes[nid]; ok {
			log.Infof("node.update")
			c.handleNodeStateChange(discovery.NodeKeepalive, &event.Node)
		}
	case discovery.Delete:
		if _, ok := c.nodes[nid]; ok {
			log.Infof("node.delete")
			c.handleNodeStateChange(discovery.NodeDown, &event.Node)
		}
		delete(c.nodes, nid)
	default:
		log.Warnf("unkonw message: %v", string(msg.Data))
		return fmt.Errorf("unkonw message: %v", msg.Data)
	}

	return nil
}

func (c *Client) Watch(service string, onStateChange NodeStateChangeCallback) error {
	var err error
	subj := discovery.DefaultDiscoveryPrefix + "." + service + ".>"

	msgCh := make(chan *nats.Msg)

	if c.sub, err = c.nc.ChanSubscribe(subj, msgCh); err != nil {
		return err
	}

	c.handleNodeStateChange = onStateChange

	go func() error {
		for {
			select {
			case <-c.ctx.Done():
				return c.ctx.Err()
			case msg, ok := <-msgCh:
				if ok {
					err := c.handleMsg(msg)
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
		c.SendAction(node, discovery.Delete)
		t.Stop()
	}()

	c.SendAction(node, discovery.Save)

	for {
		select {
		case <-c.ctx.Done():
			err := c.ctx.Err()
			log.Errorf("keepalive abort: err %v", err)
			return err
		case <-t.C:
			c.SendAction(node, discovery.Update)
			break
		}
	}
}

func (c *Client) SendAction(node discovery.Node, action string) error {
	data, err := util.Marshal(&discovery.KeepAlive{
		Action: action, Node: node,
	})
	if err != nil {
		log.Errorf("%v", err)
		return err
	}
	subj := discovery.DefaultPublishPrefix + "." + node.Service + "." + node.ID()
	if err := c.nc.Publish(subj, data); err != nil {
		log.Errorf("node start error: err=%v, id=%v", err, node.ID())
		return nil
	}
	return nil
}
