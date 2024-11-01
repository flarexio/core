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
		Config string
	}

	if err := value.Decode(&raw); err != nil {
		return err
	}

	s.Name = raw.Name
	s.Raw = json.RawMessage(raw.Config)

	var cfg jetstream.StreamConfig
	if err := json.Unmarshal(s.Raw, &cfg); err != nil {
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
		Config string
	}

	if err := value.Decode(&raw); err != nil {
		return err
	}

	c.Name = raw.Name
	c.Stream = raw.Stream
	c.Raw = json.RawMessage(raw.Config)

	var cfg jetstream.ConsumerConfig
	if err := json.Unmarshal(c.Raw, &cfg); err != nil {
		return err
	}

	cfg.Durable = raw.Name

	c.Config = cfg

	return nil
}
