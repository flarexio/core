package pubsub

import (
	"context"
	"regexp"
	"strings"
	"sync"
)

func NewSimplePubSub() PubSub {
	return &simplePubSub{
		subscribers: make(map[string]*subscription),
	}
}

type subscription struct {
	pattern  *regexp.Regexp
	handlers []MessageHandler
}

type simplePubSub struct {
	subscribers map[string]*subscription
	sync.RWMutex
}

func (ps *simplePubSub) Publish(topic string, data []byte) error {
	ps.RLock()
	defer ps.RUnlock()

	msg := &Message{
		Topic: topic,
		Data:  data,
	}

	ctx := context.Background()

	for _, sub := range ps.subscribers {
		if sub.pattern.MatchString(topic) {
			for _, handler := range sub.handlers {
				go handler(ctx, msg)
			}
		}
	}

	return nil
}

func (ps *simplePubSub) Subscribe(topic string, callback MessageHandler) error {
	ps.Lock()
	defer ps.Unlock()

	sub, ok := ps.subscribers[topic]
	if !ok {
		pattern, err := compilePattern(topic)
		if err != nil {
			return err
		}

		sub = &subscription{
			pattern:  pattern,
			handlers: make([]MessageHandler, 0),
		}
		ps.subscribers[topic] = sub
	}

	sub.handlers = append(sub.handlers, callback)

	return nil
}

func (ps *simplePubSub) Close() error {
	ps.Lock()
	defer ps.Unlock()

	ps.subscribers = make(map[string]*subscription)

	return nil
}

// compilePattern compiles a topic pattern with wildcards into a regexp
// * matches exactly one word
// # matches zero or more words
func compilePattern(pattern string) (*regexp.Regexp, error) {
	// Escape dots first
	regexPattern := strings.ReplaceAll(pattern, `.`, `\.`)
	// * matches exactly one word (no dots)
	regexPattern = strings.ReplaceAll(regexPattern, `*`, `[^.]+`)
	// # matches zero or more words (including dots)
	regexPattern = strings.ReplaceAll(regexPattern, `#`, `.*`)

	// Anchor the pattern
	regexPattern = "^" + regexPattern + "$"

	return regexp.Compile(regexPattern)
}
