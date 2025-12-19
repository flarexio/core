package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type simplePubSubTestSuite struct {
	suite.Suite
	ps PubSub
}

func (suite *simplePubSubTestSuite) SetupTest() {
	suite.ps = NewSimplePubSub()
}

func (suite *simplePubSubTestSuite) TearDownTest() {
	suite.ps.Close()
}

func (suite *simplePubSubTestSuite) TestPublishSubscribe() {
	var received []byte
	var wg sync.WaitGroup
	wg.Add(1)

	err := suite.ps.Subscribe("test.topic", func(ctx context.Context, msg *Message) error {
		received = msg.Data
		wg.Done()
		return nil
	})
	suite.Require().NoError(err)

	err = suite.ps.Publish("test.topic", []byte("hello"))
	suite.Require().NoError(err)

	wg.Wait()

	suite.Equal("hello", string(received))
}

func (suite *simplePubSubTestSuite) TestWildcardStar() {
	var received string
	var wg sync.WaitGroup
	wg.Add(1)

	err := suite.ps.Subscribe("test.*.event", func(ctx context.Context, msg *Message) error {
		received = msg.Topic
		wg.Done()
		return nil
	})
	suite.Require().NoError(err)

	err = suite.ps.Publish("test.user.event", []byte("data"))
	suite.Require().NoError(err)

	wg.Wait()

	suite.Equal("test.user.event", received)
}

func (suite *simplePubSubTestSuite) TestWildcardStarNoMatch() {
	called := false

	err := suite.ps.Subscribe("test.*.event", func(ctx context.Context, msg *Message) error {
		called = true
		return nil
	})
	suite.Require().NoError(err)

	// Should not match: * only matches exactly one word
	suite.ps.Publish("test.user.foo.event", []byte("data"))

	time.Sleep(50 * time.Millisecond)

	suite.False(called, "handler should not be called for non-matching topic")
}

func (suite *simplePubSubTestSuite) TestWildcardHash() {
	var mu sync.Mutex
	var topics []string

	err := suite.ps.Subscribe("test.#", func(ctx context.Context, msg *Message) error {
		mu.Lock()
		topics = append(topics, msg.Topic)
		mu.Unlock()
		return nil
	})
	suite.Require().NoError(err)

	suite.ps.Publish("test.a", []byte("data1"))
	suite.ps.Publish("test.a.b.c", []byte("data2"))

	suite.Eventually(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(topics) == 2
	}, 1*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	suite.Contains(topics, "test.a")
	suite.Contains(topics, "test.a.b.c")
}

func (suite *simplePubSubTestSuite) TestWildcardHashMatchesZeroWords() {
	var received string
	var wg sync.WaitGroup
	wg.Add(1)

	err := suite.ps.Subscribe("test.#", func(ctx context.Context, msg *Message) error {
		received = msg.Topic
		wg.Done()
		return nil
	})
	suite.Require().NoError(err)

	// # should match zero words too
	err = suite.ps.Publish("test.", []byte("data"))
	suite.Require().NoError(err)

	wg.Wait()

	suite.Equal("test.", received)
}

func (suite *simplePubSubTestSuite) TestNoMatch() {
	called := false

	err := suite.ps.Subscribe("test.topic", func(ctx context.Context, msg *Message) error {
		called = true
		return nil
	})
	suite.Require().NoError(err)

	suite.ps.Publish("other.topic", []byte("data"))

	time.Sleep(50 * time.Millisecond)

	suite.False(called, "handler should not be called for non-matching topic")
}

func (suite *simplePubSubTestSuite) TestMultipleHandlers() {
	var count int
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(2)

	handler := func(ctx context.Context, msg *Message) error {
		mu.Lock()
		count++
		mu.Unlock()
		wg.Done()
		return nil
	}

	suite.ps.Subscribe("test.topic", handler)
	suite.ps.Subscribe("test.topic", handler)

	suite.ps.Publish("test.topic", []byte("data"))

	wg.Wait()

	suite.Equal(2, count)
}

func (suite *simplePubSubTestSuite) TestMultipleSubscriptions() {
	var mu sync.Mutex
	var topics []string
	var wg sync.WaitGroup
	wg.Add(2)

	handler := func(ctx context.Context, msg *Message) error {
		mu.Lock()
		topics = append(topics, msg.Topic)
		mu.Unlock()
		wg.Done()
		return nil
	}

	suite.ps.Subscribe("test.a", handler)
	suite.ps.Subscribe("test.b", handler)

	suite.ps.Publish("test.a", []byte("data"))
	suite.ps.Publish("test.b", []byte("data"))

	wg.Wait()

	suite.Len(topics, 2)
	suite.Contains(topics, "test.a")
	suite.Contains(topics, "test.b")
}

func (suite *simplePubSubTestSuite) TestMessageContainsTopic() {
	var receivedTopic string
	var wg sync.WaitGroup
	wg.Add(1)

	err := suite.ps.Subscribe("test.*", func(ctx context.Context, msg *Message) error {
		receivedTopic = msg.Topic
		wg.Done()
		return nil
	})
	suite.Require().NoError(err)

	suite.ps.Publish("test.event", []byte("data"))

	wg.Wait()

	suite.Equal("test.event", receivedTopic)
}

func (suite *simplePubSubTestSuite) TestClose() {
	var count int
	var mu sync.Mutex

	suite.ps.Subscribe("test.topic", func(ctx context.Context, msg *Message) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	})

	err := suite.ps.Close()
	suite.Require().NoError(err)

	// After close, publish should not trigger old handlers
	suite.ps.Publish("test.topic", []byte("data"))

	time.Sleep(50 * time.Millisecond)

	suite.Equal(0, count, "handler should not be called after close")
}

func (suite *simplePubSubTestSuite) TestCompilePattern() {
	tests := []struct {
		pattern string
		topic   string
		match   bool
	}{
		// Exact match
		{"test.topic", "test.topic", true},
		{"test.topic", "test.other", false},

		// Star wildcard - matches exactly one word
		{"test.*.event", "test.user.event", true},
		{"test.*.event", "test.user.foo.event", false},
		{"test.*", "test.a", true},
		{"*.topic", "foo.topic", true},

		// Hash wildcard - matches zero or more words
		{"test.#", "test.a", true},
		{"test.#", "test.a.b.c", true},
		{"test.#", "test.", true},
		{"#", "anything.here", true},
	}

	for _, tt := range tests {
		pattern, err := compilePattern(tt.pattern)
		suite.Require().NoError(err, "pattern: %s", tt.pattern)

		result := pattern.MatchString(tt.topic)
		suite.Equal(tt.match, result, "pattern: %s, topic: %s, regex: %s", tt.pattern, tt.topic, pattern.String())
	}
}

func TestSimplePubSubSuite(t *testing.T) {
	suite.Run(t, new(simplePubSubTestSuite))
}
