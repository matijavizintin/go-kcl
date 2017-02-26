package kcl

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const defaultReadInterval = 100 * time.Microsecond
const defaultBatchSize int = 100
const defaultChannelSize int = 100

type Reader struct {
	client *Client

	streamName        string
	shardId           string
	clientName        string
	readInterval      time.Duration
	batchSize         *int64
	channelBufferSize int

	checkpoint     *string
	checkpointLock sync.Mutex
	streamReadLock sync.Mutex

	err    error
	closed bool
	wg     *sync.WaitGroup
}

// NewReader initialize a reader on a shard with default parameters. It reads a batch of 100 records from a shard every
// 100 ms
func (c *Client) NewReader(streamName string, shardId string, clientName string) (*Reader, error) {
	return c.NewReaderWithParameters(streamName, shardId, clientName, defaultReadInterval, defaultBatchSize, defaultChannelSize)
}

// NewReaderWithParameters initialize a reader on a shard defining how often a shard should be read, the size of a
// read batch and the channel buffer.
func (c *Client) NewReaderWithParameters(streamName string, shardId string, clientName string, streamReadInterval time.Duration, readBatchSize int, channelBufferSize int) (*Reader, error) {
	if c.checkpoint == nil {
		return nil, ErrMissingCheckpointer
	}

	r := &Reader{
		client:            c,
		streamName:        streamName,
		shardId:           shardId,
		clientName:        clientName,
		wg:                &sync.WaitGroup{},
		readInterval:      streamReadInterval,
		batchSize:         aws.Int64(int64(readBatchSize)),
		channelBufferSize: channelBufferSize,
	}
	return r, nil
}

// Records consumes a shard from the last checkpoint if any, otherwise starts at the last untrimmed record. It returns
// a read-only buffered channel via which results are delivered.
// NOTE: checkpoints are NOT automatically set, you have to do it via UpdateCheckpoint function ideally after you call
// Close and consume all messages from the channel. If you want to make checkpoints while consuming the stream and be
// 100% safe that no messages got unprocessed you should call BlockReading, consume the channel, call UpdateCheckpoint
// and then call ResumeReading.
func (lr *Reader) Records() <-chan *kinesis.Record {
	ch := make(chan *kinesis.Record, lr.channelBufferSize)

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

// UpdateCheckpoint sets the checkpoint to the last record that was read. It waits for the current batch to be
// processed and pushed to the channel.
func (lr *Reader) UpdateCheckpoint() error {
	// acquire lock so that checkpoint increments don't get discarded while updating checkpoint
	lr.checkpointLock.Lock()
	defer lr.checkpointLock.Unlock()

	if lr.checkpoint == nil {
		return nil
	}

	err := lr.client.checkpoint.SetCheckpoint(GetStreamKey(lr.streamName, lr.shardId, lr.clientName), *lr.checkpoint)
	if err != nil {
		return err
	}

	lr.checkpoint = nil
	return nil
}

// BlockReading stops reading from the stream after the current batch is processed. This could be used to safely
// update checkpoints before the reader is closed.
func (lr *Reader) BlockReading() {
	lr.streamReadLock.Lock()
}

// ResumeReading from the stream
func (lr *Reader) ResumeReading() {
	lr.streamReadLock.Unlock()
}

// Close waits for the current batch to be read and pushed to the channel and then stops the reading and closes the
// channel. No further records will be read from the stream. After calling close and consuming the channel is safe to
// call UpdateCheckpoint.
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
		lr.streamReadLock.Lock()

		out, err := lr.client.kinesis.GetRecords(&kinesis.GetRecordsInput{
			Limit:         lr.batchSize,
			ShardIterator: shardIterator,
		})
		if err != nil {
			lr.err = err
			close(ch)
			return
		}

		shardIterator = out.NextShardIterator
		if len(out.Records) == 0 {
			lr.streamReadLock.Unlock()
			continue
		}

		lr.checkpointLock.Lock()
		for _, record := range out.Records {
			ch <- record
			lr.checkpoint = record.SequenceNumber
		}
		lr.checkpointLock.Unlock()
		lr.streamReadLock.Unlock()

		time.Sleep(lr.readInterval)
	}

	close(ch)
	lr.wg.Done()
}
