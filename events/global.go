package events

import "github.com/flarexio/core/pubsub"

var instance pubsub.PubSub

func ReplaceGlobals(pb pubsub.PubSub) {
	instance = pb
}

type GlobalPubSub func() (pubsub.PubSub, error)
