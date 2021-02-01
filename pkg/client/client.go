package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cloudwebrtc/nats-discovery/pkg/registry"
	"github.com/cloudwebrtc/nats-discovery/pkg/util"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
)

type Client struct {
	nc       *nats.Conn
	sub      *nats.Subscription
	nodes    map[string]*registry.Node
	nodeLock sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
}

func (c *Client) Close() {
	c.cancel()
	c.nc.Close()
}

// NewService create a service instance
func NewClient(natsURL string) (*Client, error) {

	opts := []nats.Option{nats.Name("nats-grpc echo client")}
	// Connect to the NATS server.
	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		log.Errorf("%v", err)
		return nil, err
	}

	c := &Client{
		nc:    nc,
		nodes: make(map[string]*registry.Node),
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c, nil
}

func (c *Client) Get(service string) error {
	data, err := util.Marshal(&registry.KeepAlive{
		Action: registry.Get, Node: registry.Node{},
	})
	if err != nil {
		log.Errorf("%v", err)
		return err
	}
	subj := registry.DefaultPublishPrefix + "."
	if msg, err2 := c.nc.Request(subj, data, 15*time.Second); err2 != nil {
		log.Errorf("Get: service=%v, err=%v", service, err2)
		return nil
	} else {
		log.Infof("msg %v", string(msg.Data))
	}
	return nil
}

func (c *Client) Watch(service string) error {
	var err error
	subj := registry.DefaultDiscoveryPrefix + "." + service + ".>"

	msgCh := make(chan *nats.Msg)

	if c.sub, err = c.nc.ChanSubscribe(subj, msgCh); err != nil {
		return err
	}

	handleMsg := func(msg *nats.Msg) error {
		log.Infof("handle discovery message: %v", msg.Subject)

		c.nodeLock.Lock()
		defer c.nodeLock.Unlock()

		var event registry.KeepAlive
		err := util.Unmarshal(msg.Data, &event)
		if err != nil {
			log.Errorf("connect: error parsing offer: %v", err)
			return err
		}
		nid := event.Node.ID()
		switch event.Action {
		case registry.Save:
			if _, ok := c.nodes[nid]; !ok {
				log.Infof("app.save")
				c.nodes[nid] = &event.Node
			}
		case registry.Update:
			if _, ok := c.nodes[nid]; ok {
				log.Infof("app.update")
			}
		case registry.Delete:
			if _, ok := c.nodes[nid]; ok {
				log.Infof("app.delete")
			}
			delete(c.nodes, nid)
		default:
			log.Warnf("unkonw message: %v", string(msg.Data))
			return fmt.Errorf("unkonw message: %v", msg.Data)
		}

		return nil
	}

	go func() error {
		for {
			select {
			case <-c.ctx.Done():
				return c.ctx.Err()
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

func (c *Client) KeepAlive(node registry.Node) error {
	t := time.NewTicker(registry.DefaultLivecycle)
	defer func() {
		c.SendAction(node, registry.Delete)
		t.Stop()
	}()

	c.SendAction(node, registry.Save)
	for {
		select {
		case <-c.ctx.Done():
			err := c.ctx.Err()
			log.Errorf("keepalive: err %v", err)
			return err
		case <-t.C:
			c.SendAction(node, registry.Update)
			break
		}
	}
}

func (c *Client) SendAction(node registry.Node, action string) error {
	data, err := util.Marshal(&registry.KeepAlive{
		Action: action, Node: node,
	})
	if err != nil {
		log.Errorf("%v", err)
		return err
	}
	subj := registry.DefaultPublishPrefix + "." + node.Service + "." + node.ID()
	if err := c.nc.Publish(subj, data); err != nil {
		log.Errorf("node start error: err=%v, id=%v", err, node.ID())
		return nil
	}
	return nil
}
