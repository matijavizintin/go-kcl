package kcl

import (
	"errors"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	ErrMissingLocker       = errors.New("Missing locker")
	ErrMissingCheckpointer = errors.New("Missing checkpointer")
)

type SharedReader struct {
	clinet *Client

	streamName     string
	checkpointName string

	err error
}

func (c *Client) NewSharedReader(streamName string, checkpointName string) (*SharedReader, error) {
	if c.distlock == nil {
		return nil, ErrMissingLocker
	}
	if c.checkpoint == nil {
		return nil, ErrMissingCheckpointer
	}

	r := &SharedReader{
		clinet:         c,
		streamName:     streamName,
		checkpointName: checkpointName,
	}

	return r, nil
}

func (sr *SharedReader) Records() chan *kinesis.Record {
	// TODO
	return nil
}

func (sr *SharedReader) Err() error {
	return sr.err
}

func (sr *SharedReader) UpdateCheckpoint() error {
	// TODO
	return sr.clinet.checkpoint.SetCheckpoint("shard", sr.checkpointName, "value")
}
