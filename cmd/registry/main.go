package main

import (
	"time"

	"github.com/cloudwebrtc/nats-discovery/pkg/discovery"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
)

func init() {
	log.Init("info", []string{"asm_amd64.s", "proc.go"}, []string{})
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

	reg, err := discovery.NewRegistry(nc)
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	reg.Listen()

	select {}
}
