package types

import (
	"context"

	"github.com/redis/go-redis/v9"
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
	Init(ctx context.Context) (lbsChan <-chan *redis.XMessage, kspChan <-chan *redis.Message, streamDisownedChan <-chan string, err error)
	// Claim allows for a consumer to claim data stream from another failed consumer
	//
	// should be called once a consumer receives a message on kspchan
	Claim(ctx context.Context, kspNotification string) error
	// StreamsOwned provides a list of string of the data stream names that are being processed by consumer.
	StreamsOwned() (streamsOwned []string)
	// DoneDataStream allows to mark end of processing and ownership for a particular data stream
	DoneDataStream(ctx context.Context, dataStreamName string) error
	// Done marks the end of processing the stream
	//
	// should be called when consumer is done processing the data stream.
	Done()
}
