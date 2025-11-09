package node

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"distributed_logging_system/microservices/config"
	"distributed_logging_system/pkg/fluent"
	"distributed_logging_system/pkg/kafka"

	"github.com/fatih/color"
	"github.com/google/uuid"
)

type Node struct {
	id           string
	serviceName  string
	status       string
	cfg          config.Config
	kafkaClient  *kafka.Client
	fluentClient *fluent.Client
	ctx          context.Context
	cancel       context.CancelFunc
	printMu      sync.Mutex
	registered   bool
}

func New(serviceName string, cfg config.Config) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())
	fluentClient, err := fluent.New(cfg.FluentdHost, cfg.FluentdPort)
	if err != nil {
		cancel()
		return nil, err
	}
	return &Node{
		id:           uuid.New().String(),
		serviceName:  serviceName,
		status:       "UP",
		cfg:          cfg,
		kafkaClient:  kafka.NewClient(cfg.KafkaBrokers),
		fluentClient: fluentClient,
		ctx:          ctx,
		cancel:       cancel,
	}, nil
}

func (n *Node) printMessage(messageType string, content map[string]any) {
	n.printMu.Lock()
	defer n.printMu.Unlock()
	var colorizer func(a ...any) string
	switch messageType {
	case "registration":
		colorizer = color.New(color.FgCyan).SprintFunc()
	case "heartbeat":
		colorizer = color.New(color.FgRed).SprintFunc()
	default:
		colorizer = color.New(color.FgGreen).SprintFunc()
	}
	b, _ := json.Marshal(content)
	color.New(color.FgWhite).Printf("%s: %s\n", colorizer(messageType), string(b))
}

func (n *Node) sendToFluentd(tag string, message map[string]any) {
	_ = n.fluentClient.Emit(tag, message)
}

func (n *Node) sendToKafka(topic string, message map[string]any) {
	_ = n.kafkaClient.SendMessage(n.ctx, topic, message)
}

func (n *Node) Register() {
	if n.registered {
		return
	}
	reg := map[string]any{
		"node_id":      n.id,
		"message_type": "registration",
		"service_name": n.serviceName,
		"timestamp":    time.Now().Format(time.RFC3339),
	}
	n.printMessage("registration", reg)
	n.sendToKafka("microservice_registration", reg)
	n.registered = true
}

func (n *Node) generateLog(level, message string, extra map[string]any) {
	log := map[string]any{
		"log_id":       uuid.New().String(),
		"node_id":      n.id,
		"log_level":    level,
		"message_type": "LOG",
		"message":      message,
		"service_name": n.serviceName,
		"timestamp":    time.Now().Format(time.RFC3339),
	}
	for k, v := range extra {
		log[k] = v
	}
	n.printMessage("Log", log)
	n.sendToFluentd("log."+lower(level), log)
	n.sendToKafka("microservice_logs", log)
}

func lower(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	for i := range r {
		if 'A' <= r[i] && r[i] <= 'Z' {
			r[i] += 'a' - 'A'
		}
	}
	return string(r)
}

func (n *Node) sendHeartbeat() {
	hb := map[string]any{
		"node_id":      n.id,
		"message_type": "HEARTBEAT",
		"service_name": n.serviceName,
		"status":       n.status,
		"timestamp":    time.Now().Format(time.RFC3339),
	}
	n.printMessage("heartbeat", hb)
	n.sendToFluentd("heartbeat", hb)
	n.sendToKafka("microservice_heartbeats", hb)
}

func (n *Node) StartHeartbeat(interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-n.ctx.Done():
				return
			case <-t.C:
				if n.status == "UP" {
					n.sendHeartbeat()
				}
			}
		}
	}()
}

func (n *Node) StartLogGeneration(interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		levels := []string{"INFO", "WARN", "ERROR"}
		for {
			select {
			case <-n.ctx.Done():
				return
			case <-t.C:
				l := levels[rand.Intn(len(levels))]
				switch l {
				case "INFO":
					n.generateLog("INFO", "This is an info log.", map[string]any{})
				case "WARN":
					n.generateLog("WARN", "This is a warning log.", map[string]any{
						"response_time_ms":   rand.Intn(401) + 100,
						"threshold_limit_ms": 300,
					})
				case "ERROR":
					n.generateLog("ERROR", "This is an error log.", map[string]any{
						"error_details": map[string]any{
							"error_code":    "500",
							"error_message": "Internal Server Error",
						},
					})
				}
			}
		}
	}()
}

func (n *Node) Close() {
	n.cancel()
	_ = n.fluentClient.Close()
}



