package kcl

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	streamConsumerUpdate = time.Second * 5
)

type SharedReader struct {
	client *Client

	streamName string
	clientName string

	err error

	recordsChan chan *kinesis.Record
	closed      bool
	closedMu    sync.Mutex

	consumers  []*LockedReader
	consumerWg *sync.WaitGroup
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
	go func() {
		sr.consumeRecords()
		// TODO stop consumers
		sr.consumerWg.Wait()
		sr.Close()
	}()
	return sr.recordsChan
}

func (sr *SharedReader) consumeShard(lockedReader *LockedReader) {
	sr.consumerWg.Add(1)
	defer sr.consumerWg.Done()

	Logger.Printf("Consuming shard: %s", lockedReader.shardId)

	for record := range lockedReader.Records() {
		Logger.Printf("Shard %s | Message: %s", lockedReader.shardId, *record.SequenceNumber)

		sr.recordsChan <- record
	}
	if err := lockedReader.Close(); err != nil {
		sr.err = err
		sr.Close()
	}
}

func (sr *SharedReader) consumeRecords() {
	interval := time.NewTicker(streamConsumerUpdate)
	for range interval.C {
		if sr.closed {
			return
		}

		streamDescription, err := sr.client.StreamDescription(sr.streamName)
		if err != nil {
			sr.err = err
			return
		}

		for _, shard := range streamDescription.Shards {
			lockedReader, err := sr.client.NewLockedShardReader(sr.streamName, *shard.ShardId, sr.clientName)
			if err == ErrShardLocked {
				continue
			} else if err != nil {
				sr.err = err
				return
			}

			sr.consumers = append(sr.consumers, lockedReader)
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
	for _, c := range sr.consumers {
		err := c.UpdateCheckpoint()
		if err != nil {
			return err
		}
	}

	return nil
}
