package pubsub

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
	"go.uber.org/zap"
)

type NATSPubSub interface {
	PubSub
	AddService(cfg micro.Config) (micro.Service, error)
	AddJetStream(opts ...jetstream.JetStreamOpt) error
	AddStreamAndConsumer(ctx context.Context, cfg StreamConsumer) error
	PullConsume(consumer ConsumerStreamPair, handler MessageHandler) error
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
		log: zap.L().With(
			zap.String("infra", "pubsub"),
			zap.String("address", url),
		),
		nc:            nc,
		subscriptions: make(map[string]*nats.Subscription),
		consumers:     make(map[ConsumerStreamPair]*ConsumerCtx),
	}, nil
}

type natsPubSub struct {
	log           *zap.Logger
	nc            *nats.Conn
	js            jetstream.JetStream
	subscriptions map[string]*nats.Subscription
	consumers     map[ConsumerStreamPair]*ConsumerCtx
	sync.RWMutex
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

	if ps.js != nil {
		for _, consumer := range ps.consumers {
			consumer.Lock()
			for _, ctx := range consumer.ctxs {
				ctx.Stop()
			}
			consumer.Unlock()
		}
	}

	return ps.nc.Drain()
}

func (ps *natsPubSub) AddService(cfg micro.Config) (micro.Service, error) {
	if ps.nc == nil {
		return nil, errors.New("connection not found")
	}

	return micro.AddService(ps.nc, cfg)
}

func (ps *natsPubSub) AddJetStream(opts ...jetstream.JetStreamOpt) error {
	if ps.nc == nil {
		return errors.New("connection not found")
	}

	js, err := jetstream.New(ps.nc, opts...)
	if err != nil {
		return err
	}

	ps.js = js

	return nil
}

func (ps *natsPubSub) AddStreamAndConsumer(ctx context.Context, cfg StreamConsumer) error {
	if ps.js == nil {
		return errors.New("jetstream not found")
	}

	stream, err := ps.js.Stream(ctx, cfg.Consumer.Stream)
	if err != nil {
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return err
		}

		if cfg.Consumer.Stream != cfg.Stream.Name {
			return err
		}

		s, err := ps.js.CreateStream(ctx, cfg.Stream.Config)
		if err != nil {
			return err
		}

		stream = s
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, cfg.Consumer.Config)
	if err != nil {
		return err
	}

	pair := ConsumerStreamPair{
		Consumer: cfg.Consumer.Name,
		Stream:   cfg.Consumer.Stream,
	}

	ps.Lock()
	ps.consumers[pair] = &ConsumerCtx{
		consumer: consumer,
		ctxs:     make([]jetstream.ConsumeContext, 0),
	}
	ps.Unlock()

	return nil
}

func (ps *natsPubSub) PullConsume(consumer ConsumerStreamPair, handler MessageHandler) error {
	log := ps.log.With(
		zap.String("action", "pull_consume"),
		zap.String("stream", consumer.Stream),
		zap.String("consumer", consumer.Consumer),
	)

	ps.RLock()
	defer ps.RUnlock()

	c, ok := ps.consumers[consumer]
	if !ok {
		return errors.New("consumer not found")
	}

	cc, err := c.consumer.Consume(func(msg jetstream.Msg) {
		m := &Message{
			Topic: msg.Subject(),
			Data:  msg.Data(),
		}

		ctx := context.Background()
		err := handler(ctx, m)
		if err != nil {
			log.Error(err.Error())
			return
		}

		msg.Ack()
	})

	if err != nil {
		return err
	}

	c.Lock()
	c.ctxs = append(c.ctxs, cc)
	c.Unlock()

	return nil
}
