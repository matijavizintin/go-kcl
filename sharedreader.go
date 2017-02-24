package kcl

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	streamConsumerUpdate = time.Second
)

type SharedReader struct {
	client *Client

	streamName string
	clientName string

	err        error
	stop       chan bool
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
		client:         c,
		streamName:     streamName,
		checkpointName: checkpointName,

		stop:       make(chan bool),
		consumerWg: &sync.WaitGroup{},
	}

	return r, nil
}

func (sr *SharedReader) Records() chan *kinesis.Record {
	c := make(chan *kinesis.Record)
	go func() {
		err := sr.consumeRecords(c)
		if err != nil {
			sr.err = err
			sr.consumerWg.Wait()
			close(c)
		}
	}()
	return c
}

func (sr *SharedReader) tryConsumeShard(shard *kinesis.Shard, c chan *kinesis.Record) error {
	sr.consumerWg.Add(1)
	defer sr.consumerWg.Done()

	lockerReader, err := NewLockedShardReader(sr.streamName, shard.GoString(), sr.checkpointName)
	if err != nil {
		return err
	}

	for _, record := range lockerReader.Records() {
		c <- record
	}

	return lockerReader.Err()
}

func (sr *SharedReader) consumeRecords(c chan *kinesis.Record) error {
	interval := time.NewTicker(streamConsumerUpdate)
	for range interval.C {
		streamDescription, err := sr.client.StreamDescription(sr.streamName)
		if err != nil {
			return err
		}

		for _, shard := range streamDescription.Shards {
			err := sr.tryConsumeShard(shard, c)
			if err == ErrShardLocked {
				continue
			} else if err != nil {
				return err
			}
		}
	}

	return nil
}

func (sr *SharedReader) Err() error {
	return sr.err
}

func (sr *SharedReader) UpdateCheckpoint() error {
	// TODO
	return sr.client.checkpoint.SetCheckpoint("shard", sr.clientName, "value")
}
