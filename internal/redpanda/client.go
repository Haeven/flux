package redpanda

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Client struct {
	client *kgo.Client
}

func NewClient() (*Client, error) {
	// TODO: Configure these options based on your Redpanda setup
	opts := []kgo.Opt{
		kgo.SeedBrokers("localhost:9092"),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redpanda client: %w", err)
	}

	return &Client{client: client}, nil
}

func (c *Client) Close() error {
	c.client.Close()
	return nil
}

func (c *Client) SendMessage(topic string, key, value []byte) error {
	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	results := c.client.ProduceSync(context.Background(), record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

func (c *Client) ReceiveMessages(topic string, handler func([]byte, []byte) error) error {
	err := c.client.ConsumePartitions(context.Background(), topic, 0)
	if err != nil {
		return fmt.Errorf("failed to consume partitions: %w", err)
	}

	for {
		fetches := c.client.PollFetches(context.Background())
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			if err := handler(record.Key, record.Value); err != nil {
				return fmt.Errorf("error handling message: %w", err)
			}
		}
	}
}