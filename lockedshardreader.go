package kcl

import (
	"sync"

	"b1/services/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type LockedReader struct {
	client   *Client
	releaser Releaser

	streamName string
	shardId    string
	clientName string

	checkpoint *string

	err    error
	closed bool
	wg     *sync.WaitGroup
}

func (c *Client) NewLockedShardReader(streamName string, shardId string, clientName string) (*LockedReader, error) {
	if c.distlock == nil {
		return nil, ErrMissingLocker
	}
	if c.checkpoint == nil {
		return nil, ErrMissingCheckpointer
	}

	releaser, success, err := c.distlock.Lock(GetStreamKey(streamName, shardId, clientName))
	if err != nil {
		return nil, err
	}
	if !success {
		return nil, ErrShardLocked
	}

	r := &LockedReader{
		client:     c,
		releaser:   releaser,
		streamName: streamName,
		shardId:    shardId,
		clientName: clientName,
		wg:         &sync.WaitGroup{},
	}
	return r, nil
}

func (lr *LockedReader) Records() chan *kinesis.Record {
	ch := make(chan *kinesis.Record)

	checkpoint, err := lr.client.checkpoint.GetCheckpoint(GetStreamKey(lr.streamName, lr.shardId, lr.clientName))
	if err != nil {
		lr.err = err
		close(ch)
		return ch
	}

	var shardIteratorType *string
	if checkpoint == "" {
		shardIteratorType = aws.String(kinesis.ShardIteratorTypeTrimHorizon)
	} else {
		shardIteratorType = aws.String(kinesis.ShardIteratorTypeAtSequenceNumber)
	}

	iterator, err := lr.client.kinesis.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:             aws.String(lr.streamName),
		ShardId:                aws.String(lr.shardId),
		ShardIteratorType:      shardIteratorType,
		StartingSequenceNumber: aws.String(checkpoint),
	})
	if err != nil {
		lr.err = err
		close(ch)
		return ch
	}

	lr.wg.Add(1)
	go lr.consumeStream(ch, iterator.ShardIterator)
	return ch
}

func (lr *LockedReader) UpdateCheckpoint() error {
	if lr.checkpoint == nil {
		return nil
	}

	return lr.client.checkpoint.SetCheckpoint(GetStreamKey(lr.streamName, lr.shardId, lr.clientName), *lr.checkpoint)
}

func (lr *LockedReader) Close() error {
	if lr.closed {
		return nil
	}

	lr.closed = true
	lr.wg.Wait()
	err := lr.releaser.Release()
	if err != nil {
		lr.err = err
	}

	return lr.err
}

func (lr *LockedReader) consumeStream(ch chan *kinesis.Record, shardIterator *string) {
	for !lr.closed {
		out, err := lr.client.kinesis.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})
		if err != nil {
			lr.err = err
			close(ch)
			return
		}

		shardIterator = out.NextShardIterator

		if len(out.Records) == 0 {
			continue
		}

		for _, record := range out.Records {
			ch <- record
			lr.checkpoint = record.SequenceNumber
		}
	}

	close(ch)
	lr.wg.Done()
}
