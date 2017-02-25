package kcl

import (
	"sync"
	"time"

	"b1/services/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const sleepTime = 100 * time.Microsecond

var batchSize int64 = 100

type Reader struct {
	client *Client

	streamName string
	shardId    string
	clientName string

	checkpoint *string

	err    error
	closed bool
	wg     *sync.WaitGroup
}

func (c *Client) NewReader(streamName string, shardId string, clientName string) (*Reader, error) {
	if c.checkpoint == nil {
		return nil, ErrMissingCheckpointer
	}

	r := &Reader{
		client:     c,
		streamName: streamName,
		shardId:    shardId,
		clientName: clientName,
		wg:         &sync.WaitGroup{},
	}
	return r, nil
}

func (lr *Reader) Records() chan *kinesis.Record {
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
		shardIteratorType = aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber)
	}

	iteratorInput := &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(lr.streamName),
		ShardId:           aws.String(lr.shardId),
		ShardIteratorType: shardIteratorType,
	}
	if checkpoint != "" {
		iteratorInput.StartingSequenceNumber = aws.String(checkpoint)
	}

	iterator, err := lr.client.kinesis.GetShardIterator(iteratorInput)
	if err != nil {
		lr.err = err
		close(ch)
		return ch
	}

	lr.wg.Add(1)
	go lr.consumeStream(ch, iterator.ShardIterator)
	return ch
}

func (lr *Reader) UpdateCheckpoint() error {
	if lr.checkpoint == nil {
		return nil
	}

	// TODO: Set set checkpoint to nil after SetCheckpoint if it didn't change

	return lr.client.checkpoint.SetCheckpoint(GetStreamKey(lr.streamName, lr.shardId, lr.clientName), *lr.checkpoint)
}

func (lr *Reader) Close() error {
	if lr.closed {
		return nil
	}

	lr.closed = true
	lr.wg.Wait()

	return lr.err
}

func (lr *Reader) IsClosed() bool {
	return lr.closed
}

func (lr *Reader) consumeStream(ch chan *kinesis.Record, shardIterator *string) {
	for !lr.closed {
		out, err := lr.client.kinesis.GetRecords(&kinesis.GetRecordsInput{
			Limit:         &batchSize,
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

		time.Sleep(sleepTime)
	}

	close(ch)
	lr.wg.Done()
}
