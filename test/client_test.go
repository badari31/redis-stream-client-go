package test

import (
	"bburli/redis-stream-client-go/impl"
	"bburli/redis-stream-client-go/types"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	redisgo "github.com/redis/go-redis/v9"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

func newRedisClient(redisContainer *redis.RedisContainer) redisgo.UniversalClient {
	connString, err := redisContainer.ConnectionString(context.Background())
	if err != nil {
		panic(err)
	}

	connString = connString[8:] // remove redis:// prefix

	return redisgo.NewUniversalClient(&redisgo.UniversalOptions{
		Addrs: []string{connString},
		DB:    0,
	})
}

func setupSuite(t *testing.T) *redis.RedisContainer {
	redisContainer, err := redis.Run(context.Background(), "redis:7.2.3")
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}
	require.True(t, redisContainer != nil)
	require.True(t, redisContainer.IsRunning())

	connString, err := redisContainer.ConnectionString(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, connString)

	return redisContainer
}

func TestLBS(t *testing.T) {
	ctx := context.TODO()
	redisContainer := setupSuite(t)

	redisClient := newRedisClient(redisContainer)
	res := redisClient.ConfigSet(ctx, types.NotifyKeyspaceEventsCmd, types.KeyspacePatternForExpiredEvents)
	require.NoError(t, res.Err())

	// create consumer1 client
	consumer1 := createConsumer("111", redisContainer)
	require.NotNil(t, consumer1)
	lbsChan1, kspChan1, err := consumer1.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, lbsChan1)
	require.NotNil(t, kspChan1)

	// create consumer2 client
	consumer2 := createConsumer("222", redisContainer)
	require.NotNil(t, consumer2)
	lbsChan2, kspChan2, err := consumer2.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, lbsChan2)
	require.NotNil(t, kspChan2)

	lbsChan1, _, err = consumer1.Init(ctx)
	require.NoError(t, err)

	lbsChan2, _, err = consumer2.Init(ctx)
	require.NoError(t, err)

	addTwoStreamsToLBS(t, redisContainer)

	// load balanced stream distributes messages to different consumers in a load balanced way
	// so we keep track of which stream was given to consumer1 so that we can check if consumer2 gets another one
	var expectedMsgConsumer2 string
	var expectedMsgConsumer1 string

	for i := range 2 {
		log.Println("iteration: ", i)
		select {
		case msg, ok := <-lbsChan1:
			require.True(t, ok)
			require.NotNil(t, msg)
			var lbsMessage types.LBSMessage
			require.NoError(t, json.Unmarshal([]byte(msg.Values[types.LBSInput].(string)), &lbsMessage))
			require.NotNil(t, lbsMessage)

			if expectedMsgConsumer1 != "" {
				require.Equal(t, lbsMessage.DataStreamName, expectedMsgConsumer1)
			} else {
				if lbsMessage.DataStreamName == "session1" {
					expectedMsgConsumer2 = "session2"
					require.Equal(t, lbsMessage.Info["key1"], "value1")
				} else {
					expectedMsgConsumer2 = "session1"
					require.Equal(t, lbsMessage.Info["key2"], "value2")
				}
			}
		case msg, ok := <-lbsChan2:
			require.True(t, ok)
			require.NotNil(t, msg)
			var lbsMessage types.LBSMessage
			require.NoError(t, json.Unmarshal([]byte(msg.Values[types.LBSInput].(string)), &lbsMessage))
			require.NotNil(t, lbsMessage)
			if expectedMsgConsumer2 != "" {
				require.Equal(t, lbsMessage.DataStreamName, expectedMsgConsumer2)
			} else {
				if lbsMessage.DataStreamName == "session1" {
					expectedMsgConsumer1 = "session2"
					require.Equal(t, lbsMessage.Info["key1"], "value1")
				} else {
					expectedMsgConsumer1 = "session1"
					require.Equal(t, lbsMessage.Info["key2"], "value2")
				}
			}
		}
	}

	consumer1.Done()
	consumer2.Done()

	_, ok := <-lbsChan1
	require.False(t, ok)
	_, ok = <-lbsChan2
	require.False(t, ok)
}

func TestClaimWorksOnlyOnce(t *testing.T) {
	ctxWCancel, cancelFunc := context.WithCancel(context.Background())
	ctxWOCancel := context.Background()

	redisContainer := setupSuite(t)

	redisClient := newRedisClient(redisContainer)
	res := redisClient.ConfigSet(ctxWOCancel, types.NotifyKeyspaceEventsCmd, types.KeyspacePatternForExpiredEvents)
	require.NoError(t, res.Err())

	// create consumer1 client
	consumer1 := createConsumer("111", redisContainer)
	require.NotNil(t, consumer1)
	lbsChan1, kspChan1, err := consumer1.Init(ctxWCancel)
	require.NoError(t, err)
	require.NotNil(t, lbsChan1)
	require.NotNil(t, kspChan1)

	// create consumer2 client
	consumer2 := createConsumer("222", redisContainer)
	require.NotNil(t, consumer2)
	lbsChan2, kspChan2, err := consumer2.Init(ctxWOCancel)
	require.NoError(t, err)
	require.NotNil(t, lbsChan2)
	require.NotNil(t, kspChan2)

	addTwoStreamsToLBS(t, redisContainer)

	// create consumer3 client
	consumer3 := createConsumer("333", redisContainer)
	require.NotNil(t, consumer3)
	lbsChan3, kspChan3, err := consumer3.Init(ctxWOCancel)
	require.NoError(t, err)
	require.NotNil(t, lbsChan3)
	require.NotNil(t, kspChan3)

	// get streams owned by consumer1
	streamsOwnedByConsumer1 := consumer1.StreamsOwned()
	// kill consumer1
	cancelFunc()

	// consumer2 and consumer3 try to claim at the same time
	consumer2.Claim(ctxWOCancel, streamsOwnedByConsumer1[0]+":0")
	err = consumer3.Claim(ctxWOCancel, streamsOwnedByConsumer1[0]+":0")
	require.Error(t, err)
	require.Equal(t, err, fmt.Errorf("already claimed"))

	// Done is not called on consumer1 as it's crashed

	consumer2.Done()
	consumer3.Done()
}

func TestKspNotifs(t *testing.T) {
	ctx := context.TODO()
	redisContainer := setupSuite(t)

	redisClient := newRedisClient(redisContainer)
	res := redisClient.ConfigSet(ctx, types.NotifyKeyspaceEventsCmd, types.KeyspacePatternForExpiredEvents)
	require.NoError(t, res.Err())

	pubsub := redisClient.PSubscribe(ctx, types.ExpiredEventPattern)
	kspChan := pubsub.Channel(redisgo.WithChannelHealthCheckInterval(1*time.Second), redisgo.WithChannelSendTimeout(10*time.Minute))

	// now add a key and check if it times out
	redisClient.Set(ctx, "key1", "value1", time.Second)

	success := false

	for {
		select {
		case notif, ok := <-kspChan:
			require.True(t, ok)
			require.NotNil(t, notif)
			require.NotNil(t, notif.Payload)
			require.Equal(t, notif.Payload, "key1")
			success = true
		case <-time.After(time.Millisecond * 500):
		}

		if success {
			break
		}
	}

	require.True(t, success)
	require.NoError(t, pubsub.Close())
	// read from closed channel
	_, ok := <-kspChan
	require.False(t, ok)
	require.NoError(t, redisClient.Close())
}

func TestMainFlow(t *testing.T) {
	// Main flow:
	// there is one producer and two consumers: consumer1 and consumer2
	// producer adds messages and consumers consume.
	// consumer1 crashes
	// consumer2 is notified via ksp and it claims the stream

	// redis container
	// defer goleak.VerifyNone(t)
	redisContainer := setupSuite(t)

	// context based flow
	ctxWithCancel := context.TODO()
	consumer1Ctx, consumer1CancelFunc := context.WithCancel(ctxWithCancel)

	ctxWithCancel = context.TODO()
	consumer2Ctx, consumer2CancelFunc := context.WithCancel(ctxWithCancel)

	// create consumer1 client
	consumer1 := createConsumer("111", redisContainer)
	require.NotNil(t, consumer1)
	lbsChan1, kspChan1, err := consumer1.Init(consumer1Ctx)
	require.NoError(t, err)
	require.NotNil(t, lbsChan1)
	require.NotNil(t, kspChan1)

	// create consumer2 client
	consumer2 := createConsumer("222", redisContainer)
	require.NotNil(t, consumer2)
	lbsChan2, kspChan2, err := consumer2.Init(consumer2Ctx)
	require.NoError(t, err)
	require.NotNil(t, lbsChan2)
	require.NotNil(t, kspChan2)

	addTwoStreamsToLBS(t, redisContainer)

	simpleRedisClient := newRedisClient(redisContainer)

	// read from lbsChan
	streamsPickedup := 0
	consumer1Crashed := false
	gotNotification := false

	i := 0

	for {

		if i == 10 {
			break
		}

		if streamsPickedup == 2 && !consumer1Crashed {
			// kill consumer1
			log.Println("killing consumer1")
			require.Len(t, consumer1.StreamsOwned(), 1)
			consumer1CancelFunc()
			consumer1Crashed = true
		}

		select {
		case notif, ok := <-kspChan2:
			gotNotification = true
			require.True(t, consumer1Crashed)
			require.True(t, ok)
			require.NotNil(t, notif)
			require.NotNil(t, notif.Payload)
			require.Contains(t, notif.Payload, "session")
			err = consumer2.Claim(consumer2Ctx, notif.Payload)
			require.NoError(t, err)
			res := simpleRedisClient.XInfoStreamFull(context.Background(), "consumer-input", 100)
			require.NotNil(t, res)
			require.NotNil(t, res.Val())
			grpInfo := res.Val().Groups
			require.NotEmpty(t, grpInfo)
			// there's only one group
			require.Len(t, grpInfo, 1)
			// there are two consumers
			require.Len(t, grpInfo[0].Consumers, 2)
			var c1, c2 *redisgo.XInfoStreamConsumer
			for _, c := range grpInfo[0].Consumers {
				if c.Name == "redis-consumer-111" {
					c1 = &c
				} else if c.Name == "redis-consumer-222" {
					c2 = &c
				}

				if c1 != nil && c2 != nil {
					break
				}
			}

			require.True(t, c1.ActiveTime.Before(c2.ActiveTime))
			require.True(t, c1.SeenTime.Before(c2.SeenTime))

		case msg, ok := <-lbsChan1:
			if ok {
				require.NotNil(t, msg)
				var lbsMessage types.LBSMessage
				require.NoError(t, json.Unmarshal([]byte(msg.Values[types.LBSInput].(string)), &lbsMessage))
				require.NotNil(t, lbsMessage)
				require.Contains(t, lbsMessage.DataStreamName, "session")
				require.Contains(t, lbsMessage.Info["key1"], "value")
				streamsPickedup++
			}
		case msg, ok := <-lbsChan2:
			if ok {
				require.NotNil(t, msg)
				var lbsMessage types.LBSMessage
				require.NoError(t, json.Unmarshal([]byte(msg.Values[types.LBSInput].(string)), &lbsMessage))
				require.NotNil(t, lbsMessage)
				require.Contains(t, lbsMessage.DataStreamName, "session")
				require.Contains(t, lbsMessage.Info["key2"], "value")
				streamsPickedup++
			}
		case <-time.After(time.Second):
		}

		i++
	}

	require.True(t, gotNotification)
	consumer2.Done()
	// no Done is called on consumer1 because it crashed
	// either Done is called or context is cancelled

	// cancel the context
	consumer2CancelFunc()
	// consumer1 cancel should have been called
	// calling here to shut up the ctx leak error message
	consumer1CancelFunc()

	// kspchan may contain values as consumer1 crashes
	v, ok := <-kspChan2
	require.Nil(t, v)
	require.False(t, ok)
}

func addTwoStreamsToLBS(t *testing.T, redisContainer *redis.RedisContainer) {
	lbsMsg1, _ := json.Marshal(types.LBSMessage{
		DataStreamName: "session1",
		Info: map[string]interface{}{
			"key1": "value1",
		},
	})

	lbsMsg2, _ := json.Marshal(types.LBSMessage{
		DataStreamName: "session2",
		Info: map[string]interface{}{
			"key2": "value2",
		},
	})

	producer := newRedisClient(redisContainer)
	defer producer.Close()

	_, err := producer.XAdd(context.Background(), &redisgo.XAddArgs{
		Stream: "consumer-input",
		Values: map[string]any{
			types.LBSInput: string(lbsMsg1),
		},
	}).Result()
	require.NoError(t, err)

	_, err = producer.XAdd(context.Background(), &redisgo.XAddArgs{
		Stream: "consumer-input",
		Values: map[string]any{
			types.LBSInput: string(lbsMsg2),
		},
	}).Result()
	require.NoError(t, err)
}

func createConsumer(name string, redisContainer *redis.RedisContainer) types.RedisStreamClient {
	_ = os.Setenv("POD_NAME", name)
	// create a new redis client
	return impl.NewRedisStreamClient(newRedisClient(redisContainer), time.Millisecond*20, "consumer")
}
