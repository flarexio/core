package pubsub

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

type NATSPubSub interface {
	PubSub
	AddService(cfg micro.Config) (micro.Service, error)
	AddJetStream(opts ...jetstream.JetStreamOpt) (jetstream.JetStream, error)
}

func NewNATSPubSub(url string, name string, creds string) (NATSPubSub, error) {
	nc, err := nats.Connect(url,
		nats.Name(name),
		nats.UserCredentials(creds),
	)

	if err != nil {
		return nil, err
	}

	return &natsPubSub{
		nc:            nc,
		subscriptions: make(map[string]*nats.Subscription),
	}, nil
}

type natsPubSub struct {
	nc            *nats.Conn
	subscriptions map[string]*nats.Subscription
	sync.Mutex
}

func (ps *natsPubSub) Publish(topic string, data []byte) error {
	return ps.nc.Publish(topic, data)
}

func (ps *natsPubSub) Subscribe(topic string, callback MessageHandler) error {
	topic = strings.ReplaceAll(topic, `#`, `>`)

	sub, err := ps.nc.Subscribe(topic, func(m *nats.Msg) {
		ctx := context.Background()
		msg := &Message{
			Topic:    m.Subject,
			Data:     m.Data,
			Response: m.Respond,
		}

		callback(ctx, msg)
	})

	if err != nil {
		return err
	}

	ps.Lock()
	ps.subscriptions[topic] = sub
	ps.Unlock()
	return nil
}

func (ps *natsPubSub) Close() error {
	ps.Lock()
	defer ps.Unlock()

	for _, sub := range ps.subscriptions {
		sub.Unsubscribe()
		sub.Drain()
	}

	return ps.nc.Drain()
}

func (ps *natsPubSub) AddService(cfg micro.Config) (micro.Service, error) {
	if ps.nc == nil {
		return nil, errors.New("connection not found")
	}

	return micro.AddService(ps.nc, cfg)
}

func (ps *natsPubSub) AddJetStream(opts ...jetstream.JetStreamOpt) (jetstream.JetStream, error) {
	if ps.nc == nil {
		return nil, errors.New("connection not found")
	}

	return jetstream.New(ps.nc, opts...)
}
