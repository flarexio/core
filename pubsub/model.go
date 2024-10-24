package pubsub

import (
	"encoding/json"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3"
)

type ConsumerStreamPair struct {
	Consumer string
	Stream   string
}

type ConsumerCtx struct {
	consumer jetstream.Consumer
	ctxs     []jetstream.ConsumeContext
	sync.Mutex
}

type StreamConsumer struct {
	Stream   Stream
	Consumer Consumer
}

type Stream struct {
	Name   string
	Config jetstream.StreamConfig
	Raw    json.RawMessage
}

func (s *Stream) UnmarshalYAML(value *yaml.Node) error {
	var raw struct {
		Name   string
		Config json.RawMessage
	}

	if err := value.Decode(&raw); err != nil {
		return err
	}

	s.Name = raw.Name
	s.Raw = raw.Config

	var cfg jetstream.StreamConfig
	if err := json.Unmarshal(raw.Config, &cfg); err != nil {
		return err
	}

	cfg.Name = raw.Name

	s.Config = cfg

	return nil
}

type Consumer struct {
	Name   string
	Stream string
	Config jetstream.ConsumerConfig
	Raw    json.RawMessage
}

func (c *Consumer) UnmarshalYAML(value *yaml.Node) error {
	var raw struct {
		Name   string
		Stream string
		Config json.RawMessage
	}

	if err := value.Decode(&raw); err != nil {
		return err
	}

	c.Name = raw.Name
	c.Stream = raw.Stream
	c.Raw = raw.Config

	var cfg jetstream.ConsumerConfig
	if err := json.Unmarshal(raw.Config, &cfg); err != nil {
		return err
	}

	cfg.Durable = raw.Name

	c.Config = cfg

	return nil
}
