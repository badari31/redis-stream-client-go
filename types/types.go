package types

import (
	"bburli/redis-stream-client-go/notifs"
	"context"
)

// RedisStreamClient is an interface for a Redis Stream client
// This is the main interface for the Redis Stream client
type RedisStreamClient interface {
	// ID returns consumerID which uniquely identifies the consumer
	ID() string
	// Init Initialize the client
	//
	// Returns the load balanced stream (LBS) channel. This channel should be used by consumers to find out which new data stream has been added for processing. Equivalent to kafka's topic.
	// 		   the key space notifications (ksp) channel. This channel should be used by consumers to find out if any of the streams has expired. All notifications will come to kspchan.
	// 		   error if there is any in initialization
	Init(ctx context.Context) (outputChan <-chan notifs.RecoverableRedisNotification[any], err error)
	// Claim allows for a consumer to claim data stream from another failed consumer
	//
	// should be called once a consumer receives a message on kspchan
	Claim(ctx context.Context, kspNotification string) error
	// Done marks the end of processing the stream
	//
	// should be called when consumer is done processing the data stream.
	Done() error
}
