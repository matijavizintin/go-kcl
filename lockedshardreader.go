package kcl

import "github.com/aws/aws-sdk-go/service/kinesis"

type LockedReader struct {
	client   *Client
	releaser Releaser

	streamName     string
	shardId        string
	checkpointName string

	err error
}

func (c *Client) NewLockedShardReader(streamName string, shardId string, checkpointName string) (*LockedReader, error) {
	if c.distlock == nil {
		return nil, ErrMissingLocker
	}
	if c.checkpoint == nil {
		return nil, ErrMissingCheckpointer
	}

	releaser, success, err := c.distlock.Lock(GetStreamKey(streamName, shardId, checkpointName))
	if err != nil {
		return nil, err
	}
	if !success {
		return nil, ErrShardLocked
	}

	r := &LockedReader{
		client:         c,
		releaser:       releaser,
		streamName:     streamName,
		shardId:        shardId,
		checkpointName: checkpointName,
	}
	return r, nil
}

func (lr *LockedReader) Records() chan *kinesis.Record {
	// TODO
	return nil
}

func (lr *LockedReader) Err() error {
	return lr.err
}

func (lr *LockedReader) UpdateCheckpoint() error {
	// TODO
	return nil
}
