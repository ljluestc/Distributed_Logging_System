package tracker

import (
	"sync"
	"time"

	"github.com/fatih/color"
)

type NodeInfo struct {
	ServiceName   string
	LastHeartbeat time.Time
	Status        string
}

type NodeTracker struct {
	nodes            map[string]*NodeInfo
	mu               sync.Mutex
	heartbeatTimeout time.Duration
}

func New(timeout time.Duration) *NodeTracker {
	t := &NodeTracker{
		nodes:            make(map[string]*NodeInfo),
		heartbeatTimeout: timeout,
	}
	go t.loop()
	return t
}

func (t *NodeTracker) loop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		t.check()
	}
}

func (t *NodeTracker) check() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	for id, info := range t.nodes {
		if now.Sub(info.LastHeartbeat) > t.heartbeatTimeout {
			if info.Status == "UP" {
				// transition to DOWN
				info.Status = "DOWN"
				border := "=================================================="
				msg := color.New(color.BgRed, color.FgBlack).SprintFunc()
				color.New(color.BgRed, color.FgBlack).Printf("\n%s\n", border)
				color.New(color.BgRed, color.FgBlack).Printf("%s\n", msg("  Node Disconnected: "+info.ServiceName+" (Node ID: "+id[:8]+")  "))
				color.New(color.BgRed, color.FgBlack).Printf("%s\n\n", border)
			}
		}
	}
}

func (t *NodeTracker) UpdateHeartbeat(nodeID, serviceName, status string, ts time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, exists := t.nodes[nodeID]
	if !exists {
		border := "=================================================="
		msg := color.New(color.BgGreen, color.FgBlack).SprintFunc()
		color.New(color.BgGreen, color.FgBlack).Printf("\n%s\n", border)
		color.New(color.BgGreen, color.FgBlack).Printf("%s\n", msg("  New Node Registered: "+serviceName+" (Node ID: "+nodeID[:8]+")  "))
		color.New(color.BgGreen, color.FgBlack).Printf("%s\n\n", border)
	}
	t.nodes[nodeID] = &NodeInfo{
		ServiceName:   serviceName,
		LastHeartbeat: ts,
		Status:        status,
	}
}



