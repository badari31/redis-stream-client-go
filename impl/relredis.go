package impl

import (
	"bburli/redis-stream-client-go/notifs"
	"bburli/redis-stream-client-go/types"
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

// RecoverableRedisStreamClient is an implementation of the RedisStreamClient interface
type RecoverableRedisStreamClient struct {
	// underlying redis client used to interact with redis
	redisClient redis.UniversalClient
	// consumerID is the unique identifier for the consumer
	consumerID string
	// kspChan is the channel to read keyspace notifications
	kspChan <-chan *redis.Message
	// lbsCtxCancelFunc is used to control when to kill go routines spwaned as part of lbs
	lbsCtxCancelFunc context.CancelFunc
	// hbInterval is the interval at which the client sends heartbeats
	hbInterval time.Duration
	// streamLocks is a map of stream name to LBSInfo for locking
	streamLocks map[string]*lbsInfo
	// serviceName is the name of the service
	serviceName string
	// redis pub sub subscription
	pubSub *redis.PubSub
	// outputChan is the channel exposed to clients on which we relay all messages
	outputChan chan notifs.RecoverableRedisNotification[any]
	// outputChanClosed tracks if the outputChan is still being read by client
	outputChanClosed atomic.Bool
}

// NewRedisStreamClient creates a new RedisStreamClient
//
// This function creates a new RedisStreamClient with the given redis client and stream name
// Stream is the name of the stream to read from where actual data is transmitted
func NewRedisStreamClient(redisClient redis.UniversalClient, serviceName string) types.RedisStreamClient {
	// obtain consumer name via kubernetes downward api
	podName := os.Getenv(types.PodName)
	podIP := os.Getenv(types.PodIP)

	if podName == "" && podIP == "" {
		panic("POD_NAME or POD_IP not set")
	}

	var consumerID string

	if len(podName) > 0 {
		consumerID = types.RedisConsumerPrefix + podName
	} else {
		consumerID = types.RedisConsumerPrefix + podIP
	}

	// Based on experiements we use a default heartbeat of 2 seconds because when we get a distributed lock
	// we extend key every heartbeatInterval / 2 times so that means we send heartbeat every second
	// Simillarly, when claiming a stale stream, we wait for 1 heartbeat interval duration which means the key
	// extension failed two consecutive times
	defaultHBInterval := 2 * time.Second

	return &RecoverableRedisStreamClient{
		redisClient:      redisClient,
		consumerID:       consumerID,
		kspChan:          make(<-chan *redis.Message, 500),
		hbInterval:       defaultHBInterval,
		streamLocks:      make(map[string]*lbsInfo),
		serviceName:      serviceName,
		outputChan:       make(chan notifs.RecoverableRedisNotification[any], 500),
		outputChanClosed: atomic.Bool{},
	}
}

// ID returns the consumer name that uniquely identifies the consumer
func (r *RecoverableRedisStreamClient) ID() string {
	return r.consumerID
}

// Init initializes the RedisStreamClient
//
// This function initializes the RedisStreamClient by enabling keyspace notifications for expired events,
// subscribing to expired events, and starting a blocking read on the LBS stream
// Returns a channel to read messages from the LBS stream. The client should read from this channel and process the messages.
// Returns a channel to read keyspace notifications. The client should read from this channel and process the notifications.
func (r *RecoverableRedisStreamClient) Init(ctx context.Context) (<-chan notifs.RecoverableRedisNotification[any], error) {
	if r.checkErr(ctx, r.enableKeyspaceNotifsForExpiredEvents).
		checkErr(ctx, r.subscribeToExpiredEvents) == nil {
		return nil, fmt.Errorf("error initializing the client")
	}

	lbsCtx, cancelFunc := context.WithCancel(ctx)
	r.lbsCtxCancelFunc = cancelFunc

	// start blocking read on LBS stream
	go r.readLBSStream(lbsCtx)

	// listen to ksp chan
	go r.listenKsp()

	return r.outputChan, nil
}

// Claim claims pending messages from a stream
func (r *RecoverableRedisStreamClient) Claim(ctx context.Context, mutexKey string) error {
	lbsInfo, err := createByMutexKey(mutexKey)
	if err != nil {
		return err
	}

	// Claim the stream
	res := r.redisClient.XClaim(ctx, &redis.XClaimArgs{
		Stream:   r.lbsName(),
		Group:    r.lbsGroupName(),
		Consumer: r.consumerID,
		MinIdle:  r.hbInterval, // one heartbeat interval has elapsed
		Messages: []string{lbsInfo.IDInLBS},
	})

	if res.Err() != nil {
		return res.Err()
	}

	claimed, err := res.Result()
	if err != nil {
		return err
	}

	if len(claimed) == 0 {
		return fmt.Errorf("already claimed")
	}

	pool := goredis.NewPool(r.redisClient)
	rs := redsync.New(pool)

	mutex := rs.NewMutex(lbsInfo.getMutexKey(),
		redsync.WithExpiry(r.hbInterval),
		redsync.WithFailFast(true),
		redsync.WithRetryDelay(10*time.Millisecond),
		redsync.WithSetNXOnExtend(),
		redsync.WithGenValueFunc(func() (string, error) {
			return r.consumerID, nil
		}))

	// lock once
	if err := mutex.Lock(); err != nil {
		return err
	}

	go r.startExtendingKey(ctx, mutex, lbsInfo.DataStreamName)

	// seed the mutex
	lbsInfo.Mutex = mutex
	r.streamLocks[lbsInfo.DataStreamName] = lbsInfo

	return nil
}

// DoneDataStream marks end of processing for a particular stream
func (r *RecoverableRedisStreamClient) doneDataStream(ctx context.Context, dataStreamName string) error {
	lbsInfo, ok := r.streamLocks[dataStreamName]
	if !ok {
		return fmt.Errorf("stream not found")
	}

	// delete volatile key from streamLocks
	if ok {
		delete(r.streamLocks, dataStreamName)
	}

	// unlock the stream
	_, err := lbsInfo.Mutex.Unlock()
	if err != nil && !errors.Is(errors.Unwrap(err), redsync.ErrLockAlreadyExpired) {
		return err
	}

	// Acknowledge the message
	res := r.redisClient.XAck(ctx, r.lbsName(), r.lbsGroupName(), lbsInfo.IDInLBS)
	if res.Err() != nil {
		return res.Err()
	}

	return nil
}

// Done marks the end of processing for a client
func (r *RecoverableRedisStreamClient) Done() error {
	ctx := context.Background()

	for streamName := range r.streamLocks {
		if err := r.doneDataStream(ctx, streamName); err != nil {
			return err
		}
	}

	// release resources
	if err := r.cleanup(); err != nil {
		return err
	}

	return nil
}

func (r *RecoverableRedisStreamClient) cleanup() error {
	if err := r.pubSub.Close(); err != nil {
		return err
	}

	// drain kspchan and ignore expired notifications
	// since client has called Done and thus are no longer interested in expired notifications
	for len(r.kspChan) > 0 {
		<-r.kspChan
	}

	// cancel LBS context
	r.lbsCtxCancelFunc()

	// close the ouptut channe
	if r.outputChanClosed.CompareAndSwap(false, true) {
		close(r.outputChan)
	}

	return nil
}
