package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"bytes"

	"distributed_logging_system/central/internal/tracker"
	"distributed_logging_system/pkg/kafka"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/fatih/color"
)

func mustIndex(es *elasticsearch.Client, index string, doc map[string]any) {
	b, _ := json.Marshal(doc)
	req := esapi.IndexRequest{Index: index, Body: bytes.NewReader(b)}
	res, err := req.Do(context.Background(), es)
	if err == nil && res != nil {
		defer res.Body.Close()
	}
}

func main() {
	bootstrap := "192.168.222.127:9092"
	esHost := "http://localhost:9200"
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{esHost},
	})
	if err != nil {
		panic(err)
	}

	k := kafka.NewClient([]string{bootstrap})
	tr := tracker.New(10 * time.Second)
	logIndex := "microservice_logs"
	ctx := context.Background()

	// Logs
	_, _ = k.StartConsumer(ctx, "microservice_logs", "log-consumer", func(m map[string]any) {
		// Store to Elasticsearch
		mustIndex(es, logIndex, m)
		level := getString(m, "log_level")
		ts := getString(m, "timestamp")
		service := getString(m, "service_name")
		nodeID := abbreviate(getString(m, "node_id"))
		msg := getString(m, "message")

		var colorizer func(a ...any) string
		switch level {
		case "INFO":
			colorizer = color.New(color.FgGreen).SprintFunc()
		case "WARN":
			colorizer = color.New(color.FgYellow).SprintFunc()
		case "ERROR":
			colorizer = color.New(color.FgRed).SprintFunc()
		default:
			colorizer = color.New(color.FgWhite).SprintFunc()
		}

		extra := ""
		if level == "WARN" {
			rt := getAny(m, "response_time_ms")
			th := getAny(m, "threshold_limit_ms")
			if rt != nil && th != nil {
				extra = fmt.Sprintf(" [Response: %vms, Threshold: %vms]", rt, th)
			}
		}
		if level == "ERROR" {
			if ed, ok := m["error_details"].(map[string]any); ok {
				extra = fmt.Sprintf(" [Code: %v, Details: %v]", ed["error_code"], ed["error_message"])
			}
		}

		color.New(color.FgWhite).Printf("%s [%s] %s (%s): %s%s\n",
			colorizer("["+level+"]"), ts, service, nodeID, msg, extra)
	})

	// Heartbeats
	_, _ = k.StartConsumer(ctx, "microservice_heartbeats", "heartbeat-consumer", func(m map[string]any) {
		ts := parseTime(getString(m, "timestamp"))
		service := getString(m, "service_name")
		nodeID := getString(m, "node_id")
		status := getString(m, "status")
		tr.UpdateHeartbeat(nodeID, service, status, ts)

		col := color.New(color.FgGreen)
		if status != "UP" {
			col = color.New(color.FgRed)
		}
		col.Printf("[%s] [HEARTBEAT] %s (%s): Status: %s\n", getString(m, "timestamp"), service, abbreviate(nodeID), status)
	})

	// Registration
	_, _ = k.StartConsumer(ctx, "microservice_registration", "registration-consumer", func(m map[string]any) {
		ts := getString(m, "timestamp")
		service := getString(m, "service_name")
		nodeID := getString(m, "node_id")
		tr.UpdateHeartbeat(nodeID, service, "UP", parseTime(ts))
		color.New(color.FgMagenta).Printf("[%s] [REGISTRATION] New service registered: %s (%s)\n", ts, service, abbreviate(nodeID))
	})

	color.New(color.FgWhite).Println("Started consuming messages from all topics")
	select {}
}

func getString(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getAny(m map[string]any, key string) any {
	if v, ok := m[key]; ok {
		return v
	}
	return nil
}

func abbreviate(s string) string {
	if len(s) >= 8 {
		return s[:8]
	}
	return s
}

func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Now()
	}
	return t
}



