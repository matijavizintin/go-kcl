package kcl

import (
	"github.com/matijavizintin/go-kcl/locker"
)

type LockedReader struct {
	*Reader
	releaser locker.Releaser
}

func (c *Client) NewLockedReader(streamName string, shardId string, clientName string) (*LockedReader, error) {
	if c.distlock == nil {
		return nil, ErrMissingLocker
	}

	releaser, success, err := c.distlock.Lock(GetStreamKey(streamName, shardId, clientName))
	if err != nil {
		return nil, err
	}
	if !success {
		return nil, ErrShardLocked
	}

	r, err := c.NewReader(streamName, shardId, clientName)
	if err != nil {
		return nil, err
	}

	lr := &LockedReader{
		Reader:   r,
		releaser: releaser,
	}
	return lr, nil
}

func (lr *LockedReader) Close() error {
	if lr.Reader.closed {
		return nil
	}

	var closeErr error
	err := lr.Reader.Close()
	if err != nil {
		closeErr = err
	}

	err = lr.releaser.Release()
	if err != nil {
		closeErr = err
	}

	return closeErr
}
