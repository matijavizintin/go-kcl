package kcl

import "github.com/aws/aws-sdk-go/service/kinesis"

type LockedReader struct {
	client *Client

	streamName     string
	checkpointName string

	err error
}

func (c *Client) NewLockedShardReader(streamName string, checkpointName string) (*LockedReader, error) {
	if c.distlock == nil {
		return nil, ErrMissingLocker
	}
	if c.checkpoint == nil {
		return nil, ErrMissingCheckpointer
	}

	r := &LockedReader{
		client:         c,
		streamName:     streamName,
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
