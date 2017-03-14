package kcl

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	streamConsumerUpdate = time.Second * 2
)

type SharedReader struct {
	client *Client

	streamName         string
	clientName         string
	streamReadInterval time.Duration
	readBatchSize      int
	channelBufferSize  int

	err error

	recordsChan chan *kinesis.Record
	closed      bool
	closedMu    sync.Mutex

	consumers   []*LockedReader
	consumersMu sync.Mutex
	consumerWg  *sync.WaitGroup
}

type shardConsumer struct {
	lockedReader *LockedReader
	running      bool
}

func (c *Client) NewSharedReader(streamName string, clientName string) (*SharedReader, error) {
	return c.NewSharedReaderWithParameters(streamName, clientName, defaultReadInterval, defaultBatchSize, defaultChannelSize)
}

func (c *Client) NewSharedReaderWithParameters(streamName string, clientName string, streamReadInterval time.Duration, readBatchSize int, channelBufferSize int) (*SharedReader, error) {
	if c.distlock == nil {
		return nil, ErrMissingLocker
	}
	if c.checkpoint == nil {
		return nil, ErrMissingCheckpointer
	}
	if c.snitch == nil {
		return nil, ErrMissingSnitcher
	}

	r := &SharedReader{
		client:             c,
		streamName:         streamName,
		clientName:         clientName,
		streamReadInterval: streamReadInterval,
		readBatchSize:      readBatchSize,
		channelBufferSize:  channelBufferSize,

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

func (sr *SharedReader) consumeShard(sc *shardConsumer) {
	sr.consumerWg.Add(1)
	defer sr.consumerWg.Done()

	Logger.Printf("Consuming shard: %s", sc.lockedReader.shardId)

	for record := range sc.lockedReader.Records() {
		sr.recordsChan <- record
	}
	if err := sc.lockedReader.Close(); err != nil {
		sr.err = err
		sr.Close()
	}
	sc.running = false

	Logger.Printf("Stopped consuming shard: %s", sc.lockedReader.shardId)
}

func (sr *SharedReader) consumeRecords() {
	runningConsumers := map[string]*shardConsumer{}

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
			key := GetStreamKey(sr.streamName, *shard.ShardId, sr.clientName)
			sc := runningConsumers[key]

			// TODO async shard updater
			sr.client.snitch.RegisterKey(key)

			if !sr.client.snitch.CheckOwnership(key) {
				if sc != nil && sc.running {
					sc.lockedReader.Close()
				}
				continue
			}

			if sc != nil && sc.running {
				continue
			}

			lockedReader, err := sr.client.NewLockedReaderWithParameters(sr.streamName, *shard.ShardId, sr.clientName, sr.streamReadInterval, sr.readBatchSize, sr.channelBufferSize)
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

			sc = &shardConsumer{
				lockedReader: lockedReader,
				running:      true,
			}
			runningConsumers[key] = sc

			go sr.consumeShard(sc)
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
