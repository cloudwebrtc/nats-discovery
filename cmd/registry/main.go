package main

import (
	"github.com/cloudwebrtc/nats-discovery/pkg/registry"
	"github.com/nats-io/go-nats"
	log "github.com/pion/ion-log"
)

func init() {
	log.Init("info", []string{"asm_amd64.s", "proc.go"}, []string{})
}

func main() {
	natsURL := nats.DefaultURL
	reg, err := registry.NewRegistry(natsURL)
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	reg.Listen()

	select {}
}
