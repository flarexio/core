package events

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/flarexio/core/pubsub"
)

type DomainEvent interface {
	EventName() string
	Topic() string
}

type EventStore interface {
	AddEvent(...DomainEvent)
	Notify() error
	Events() []DomainEvent
}

type eventStore struct {
	pubsub GlobalPubSub
	events []DomainEvent
	sync.Mutex
}

func NewEventStore() EventStore {
	return &eventStore{
		pubsub: func() (pubsub.PubSub, error) {
			if instance == nil {
				return nil, errors.New("pubsub not found")
			}

			return instance, nil
		},
		events: make([]DomainEvent, 0),
	}
}

func (s *eventStore) AddEvent(e ...DomainEvent) {
	s.Lock()
	s.events = append(s.events, e...)
	s.Unlock()
}

func (s *eventStore) Notify() error {
	var ps pubsub.PubSub

	count := 1
	for {
		instance, err := s.pubsub()
		if err == nil {
			ps = instance
			break
		}

		count++
		if count > 3 {
			return err
		}

		time.Sleep(1000 * time.Millisecond)
	}

	s.Lock()
	defer s.Unlock()

	for _, e := range s.events {
		data, err := json.Marshal(&e)
		if err != nil {
			return err
		}

		if err := ps.Publish(e.Topic(), data); err != nil {
			return err
		}
	}

	s.events = make([]DomainEvent, 0)
	return nil
}

func (s *eventStore) Events() []DomainEvent {
	return s.events
}
