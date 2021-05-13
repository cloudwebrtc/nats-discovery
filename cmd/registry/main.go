package main

import (
	"sync"
	"time"

	"github.com/cloudwebrtc/nats-discovery/pkg/discovery"
	"github.com/cloudwebrtc/nats-discovery/pkg/registry"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
)

func init() {
	log.Init("info")
}

//setupConnOptions default conn opts.
func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second
	connectTimeout := 5 * time.Second

	opts = append(opts, nats.Timeout(connectTimeout))
	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		if !nc.IsClosed() {
			log.Infof("Disconnected, will attempt reconnects for %.0fm", totalWait.Minutes())
		}
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Infof("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		if !nc.IsClosed() {
			log.Errorf("Exiting: no servers available")
		} else {
			log.Errorf("Exiting")
		}
	}))
	return opts
}

func main() {
	natsURL := nats.DefaultURL

	opts := []nats.Option{nats.Name("nats-discovery discovery server")}
	opts = setupConnOptions(opts)
	// Connect to the NATS server.
	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		log.Errorf("%v", err)
		return
	}

	reg, err := registry.NewRegistry(nc, discovery.DefaultExpire)
	if err != nil {
		log.Errorf("%v", err)
		return
	}

	nodes := make(map[string]discovery.Node)
	mutex := sync.Mutex{}

	reg.Listen(
		// handleNodeAction
		func(action discovery.Action, node discovery.Node) (bool, error) {
			log.Infof("handleNodeAction: action %v, node %v", action, node)

			/*
				You can store node info in db like redis here.
			*/

			mutex.Lock()
			defer mutex.Unlock()

			switch action {
			case discovery.Save:
				fallthrough
			case discovery.Update:
				nodes[node.ID()] = node
			case discovery.Delete:
				delete(nodes, node.ID())
			}

			return true, nil
		},
		//handleGetNodes
		func(service string, params map[string]interface{}) ([]discovery.Node, error) {
			log.Infof("handleGetNodes: service %v, params %v", service, params)

			/*
				You can read the global node information from redis here.
			*/

			mutex.Lock()
			defer mutex.Unlock()

			nodesResp := []discovery.Node{}
			for _, item := range nodes {
				if item.Service == service || service == "*" {
					nodesResp = append(nodesResp, item)
				}
			}

			return nodesResp, nil
		})

	select {}
}
