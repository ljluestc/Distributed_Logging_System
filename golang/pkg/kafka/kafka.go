package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Client struct {
	brokers []string
}

func NewClient(brokers []string) *Client {
	return &Client{
		brokers: brokers,
	}
}

func (c *Client) SendMessage(ctx context.Context, topic string, message map[string]any) error {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(c.brokers...),
		Topic:        topic,
		RequiredAcks: kafka.RequireOne,
		Balancer:     &kafka.LeastBytes{},
		Async:        false,
	}
	defer writer.Close()

	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(time.Now().Format(time.RFC3339Nano)),
		Value: payload,
		Time:  time.Now(),
	}
	return writer.WriteMessages(ctx, msg)
}

type Handler func(message map[string]any)

type Consumer struct {
	reader *kafka.Reader
}

func (c *Client) StartConsumer(ctx context.Context, topic, groupID string, handler Handler) (*Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,               // 1B
		MaxBytes:       10e6,            // 10MB
		CommitInterval: time.Second * 1, // commit regularly
		StartOffset:    kafka.LastOffset,
	})

	consumer := &Consumer{reader: reader}
	go func() {
		for {
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				// reader returns when context is cancelled or on fatal error
				return
			}
			var value map[string]any
			if err := json.Unmarshal(m.Value, &value); err != nil {
				// skip malformed message
				continue
			}
			handler(value)
		}
	}()
	return consumer, nil
}

func (c *Consumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}



