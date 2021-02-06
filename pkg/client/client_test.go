package client

import (
	"sync"
	"testing"

	"github.com/cloudwebrtc/nats-discovery/pkg/discovery"
	"github.com/cloudwebrtc/nats-discovery/pkg/util"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	"github.com/tj/assert"
)

const (
	nodeName = "sfu"
)

func init() {
	log.Init("info", []string{"asm_amd64.s", "proc.go"}, []string{})
}

func TestWatch(t *testing.T) {
	var wg sync.WaitGroup

	natsURL := nats.DefaultURL
	opts := []nats.Option{nats.Name("nats-discovery client")}
	// Connect to the NATS server.
	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		log.Errorf("%v", err)
		t.Error(err)
	}

	s, err := NewClient(nc)
	assert.NoError(t, err)

	node := discovery.Node{
		DC:      "dc1",
		Service: "sfu",
		NID:     util.RandomString(12),
		RPC: discovery.RPC{
			Protocol: discovery.GRPC,
			Addr:     "sfu:5551",
			Params:   map[string]string{"username": "foo", "password": "bar"},
		},
	}

	s.Watch("sfu", func(state discovery.NodeState, n *discovery.Node) {
		if state == discovery.NodeUp {
			log.Infof("NodeUp => %v", *n)
			assert.Equal(t, node, *n)
			assert.Equal(t, node.RPC, n.RPC)
			wg.Done()
		} else if state == discovery.NodeDown {
			log.Infof("NodeDown => %v", *n)
			assert.Equal(t, node.ID(), n.ID())
			wg.Done()
		}
	})

	wg.Add(1)

	go s.KeepAlive(node)
	wg.Wait()

	res, err := s.Get("sfu")
	log.Infof("nodes => %v", res.Nodes)

	assert.Equal(t, node.RPC, res.Nodes[0].RPC)

	wg.Add(1)
	s.SendAction(node, discovery.Delete)
	wg.Wait()
}
