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
func (r *Reader) Records() <-chan *kinesis.Record {
	ch := make(chan *kinesis.Record, r.channelBufferSize)

	checkpoint, err := r.client.checkpoint.GetCheckpoint(GetStreamKey(r.streamName, r.shardId, r.clientName))
	if err != nil {
		r.err = err
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
		StreamName:        aws.String(r.streamName),
		ShardId:           aws.String(r.shardId),
		ShardIteratorType: shardIteratorType,
	}
	if checkpoint != "" {
		iteratorInput.StartingSequenceNumber = aws.String(checkpoint)
	}

	iterator, err := r.client.kinesis.GetShardIterator(iteratorInput)
	if err != nil {
		r.err = err
		close(ch)
		return ch
	}

	r.wg.Add(1)
	go r.consumeStream(ch, iterator.ShardIterator)
	return ch
}

// UpdateCheckpoint sets the checkpoint to the last record that was read. It waits for the current batch to be
// processed and pushed to the channel.
func (r *Reader) UpdateCheckpoint() error {
	// acquire lock so that checkpoint increments don't get discarded while updating checkpoint
	r.checkpointLock.Lock()
	defer r.checkpointLock.Unlock()

	if r.checkpoint == nil {
		return nil
	}

	err := r.client.checkpoint.SetCheckpoint(GetStreamKey(r.streamName, r.shardId, r.clientName), *r.checkpoint)
	if err != nil {
		return err
	}

	r.checkpoint = nil
	return nil
}

// BlockReading stops reading from the stream after the current batch is processed. This could be used to safely
// update checkpoints before the reader is closed.
func (r *Reader) BlockReading() {
	r.streamReadLock.Lock()
}

// ResumeReading from the stream
func (r *Reader) ResumeReading() {
	r.streamReadLock.Unlock()
}

// Close waits for the current batch to be read and pushed to the channel and then stops the reading and closes the
// channel. No further records will be read from the stream. After calling close and consuming the channel is safe to
// call UpdateCheckpoint.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}

	r.closed = true
	r.wg.Wait()

	return r.err
}

func (r *Reader) IsClosed() bool {
	return r.closed
}

func (r *Reader) consumeStream(ch chan *kinesis.Record, shardIterator *string) {
	for !r.closed {
		r.streamReadLock.Lock()

		out, err := r.client.kinesis.GetRecords(&kinesis.GetRecordsInput{
			Limit:         r.batchSize,
			ShardIterator: shardIterator,
		})
		if err != nil {
			r.err = err
			close(ch)
			return
		}

		shardIterator = out.NextShardIterator
		if len(out.Records) == 0 {
			r.streamReadLock.Unlock()
			continue
		}

		r.checkpointLock.Lock()
		for _, record := range out.Records {
			ch <- record
			r.checkpoint = record.SequenceNumber
		}
		r.checkpointLock.Unlock()
		r.streamReadLock.Unlock()

		time.Sleep(r.readInterval)
	}

	close(ch)
	r.wg.Done()
}
