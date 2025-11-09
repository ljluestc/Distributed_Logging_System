package fluent

import (
	"fmt"

	"github.com/fluent/fluent-logger-golang/fluent"
)

type Client struct {
	logger *fluent.Fluent
}

func New(host string, port int) (*Client, error) {
	cfg := fluent.Config{
		FluentHost: host,
		FluentPort: port,
		Async:      true,
	}
	l, err := fluent.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("init fluentd: %w", err)
	}
	return &Client{logger: l}, nil
}

func (c *Client) Emit(tag string, data map[string]any) error {
	return c.logger.Post(tag, data)
}

func (c *Client) Close() error {
	if c.logger != nil {
		return c.logger.Close()
	}
	return nil
}



