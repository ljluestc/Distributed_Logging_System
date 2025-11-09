package main

import (
	"time"

	"distributed_logging_system/golang/microservices/config"
	"distributed_logging_system/golang/microservices/node"
)

func main() {
	cfg := config.Default()
	n, err := node.New("OrderService", cfg)
	if err != nil {
		panic(err)
	}
	defer n.Close()

	n.Register()
	n.StartHeartbeat(5 * time.Second)
	n.StartLogGeneration(3 * time.Second)

	select {}
}



