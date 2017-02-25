package kcl

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	streamConsumerUpdate    = time.Second * 2
	restartConsumerInterval = time.Duration(60) * time.Second
)

type SharedReader struct {
	client *Client

	streamName string
	clientName string

	err error

	recordsChan chan *kinesis.Record
	closed      bool
	closedMu    sync.Mutex

	consumers   []*LockedReader
	consumersMu sync.Mutex
	consumerWg  *sync.WaitGroup

	runningConsumers int64
}

func (c *Client) NewSharedReader(streamName string, clientName string) (*SharedReader, error) {
	if c.distlock == nil {
		return nil, ErrMissingLocker
	}
	if c.checkpoint == nil {
		return nil, ErrMissingCheckpointer
	}

	r := &SharedReader{
		client:     c,
		streamName: streamName,
		clientName: clientName,

		recordsChan: make(chan *kinesis.Record),

		consumers:  []*LockedReader{},
		consumerWg: &sync.WaitGroup{},
	}

	return r, nil
}

func (sr *SharedReader) Records() chan *kinesis.Record {
	go sr.consumeRecords()
	return sr.recordsChan
}

func (sr *SharedReader) consumeShard(lockedReader *LockedReader) {
	sr.consumerWg.Add(1)
	atomic.AddInt64(&sr.runningConsumers, 1)
	defer sr.consumerWg.Done()
	defer atomic.AddInt64(&sr.runningConsumers, -1)

	Logger.Printf("Consuming shard: %s", lockedReader.shardId)

	go func() {
		<-time.After(restartConsumerInterval)

		if err := lockedReader.Close(); err != nil {
			sr.err = err
			sr.Close()
		}
	}()

	for record := range lockedReader.Records() {
		Logger.Printf("Shard %s | Message: %s", lockedReader.shardId, string(record.Data))

		sr.recordsChan <- record
	}
	if err := lockedReader.Close(); err != nil {
		sr.err = err
		sr.Close()
	}

	Logger.Printf("Stopped consuming shard: %s", lockedReader.shardId)
}

func (sr *SharedReader) consumeRecords() {
	for range time.Tick(streamConsumerUpdate) {
		if sr.closed {
			return
		}

		streamDescription, err := sr.client.StreamDescription(sr.streamName)
		if err != nil {
			sr.err = err
			sr.Close()
			return
		}

		for _, shard := range streamDescription.Shards {
			if !sr.client.elections.CheckAndAdd(GetStreamKey(sr.streamName, *shard.ShardId, sr.clientName)) {
				continue
			}
			lockedReader, err := sr.client.NewLockedShardReader(sr.streamName, *shard.ShardId, sr.clientName)
			if err == ErrShardLocked {
				continue
			} else if err != nil {
				sr.err = err
				sr.Close()
				return
			}

			sr.consumersMu.Lock()
			sr.consumers = append(sr.consumers, lockedReader)
			sr.consumersMu.Unlock()

			go sr.consumeShard(lockedReader)

			break // one shard per interval
		}
	}
}

func (sr *SharedReader) Close() error {
	sr.closedMu.Lock()
	defer sr.closedMu.Unlock()

	if !sr.closed {
		sr.closed = true
		go func() {
			sr.consumerWg.Wait()
			close(sr.recordsChan)
		}()
	}

	return sr.err
}

func (sr *SharedReader) UpdateCheckpoint() error {
	sr.consumersMu.Lock()
	defer sr.consumersMu.Unlock()

	newConsumers := sr.consumers[0:0]

	for _, c := range sr.consumers {
		closed := c.IsClosed()
		err := c.UpdateCheckpoint()
		if err != nil {
			return err
		}
		if !closed {
			newConsumers = append(newConsumers, c)
		}
	}

	sr.consumers = newConsumers

	return nil
}
