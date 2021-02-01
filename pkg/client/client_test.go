package client

import (
	"sync"
	"testing"
	"time"

	"github.com/cloudwebrtc/nats-discovery/pkg/registry"
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

	s.Watch("sfu")
	/*
		s.Watch(proto.ServiceSFU, func(state NodeState, id string, node *Node) {
			if state == NodeStateUp {
				assert.Equal(t, s.node, *node)
				wg.Done()
			} else if state == NodeStateDown {
				assert.Equal(t, s.node.ID(), id)
				wg.Done()
			}
		})
	*/

	wg.Add(1)

	node := registry.Node{
		DC:      "dc1",
		Service: "sfu",
		NID:     "sfu" + "-" + util.RandomString(12),
	}
	go s.KeepAlive(node)
	//wg.Wait()
	time.Sleep(5 * time.Second)
	s.Get("sfu")
	//s.SendAction(node, registry.Delete)
	//s.Close()
	wg.Wait()
}
