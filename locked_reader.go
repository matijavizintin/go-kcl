package kcl

import (
	"sync"
	"time"

	"github.com/matijavizintin/go-kcl/locker"
)

type LockedReader struct {
	*Reader

	releaser locker.Releaser
}

// NewLockedReader creates a new reader with default parameters and locks it so no other instance of clientName can
// create a new one on this shard.
func (c *Client) NewLockedReader(streamName string, shardId string, clientName string) (*LockedReader, error) {
	return c.NewLockedReaderWithParameters(streamName, shardId, clientName, defaultReadInterval, defaultBatchSize, defaultChannelSize)
}

// NewLockedReader creates a new reader with specified parameters and locks it so no other instance of clientName can
// create a new one on this shard.
func (c *Client) NewLockedReaderWithParameters(streamName string, shardId string, clientName string, streamReadInterval time.Duration, readBatchSize int, channelBufferSize int) (*LockedReader, error) {
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

	r, err := c.NewReaderWithParameters(streamName, shardId, clientName, streamReadInterval, readBatchSize, channelBufferSize)
	if err != nil {
		return nil, err
	}

	lr := &LockedReader{
		Reader:   r,
		releaser: releaser,
	}
	return lr, nil
}

// Release releases the lock that was created when creating this reader. Successfully calling this function more than once
// will result in no-op.
func (lr *LockedReader) Release() error {
	if lr.releaser == nil {
		return nil
	}

	return lr.releaser.Release()
}

// CloseAndRelease closes the reader and releases the lock AFTER wg.Done() was called.
//
// IMPORTANT: You should call wg.Done() when the channel is closed and consumed. Failing to call wg.Done() will result
// in this call hanging indefinitely.
func (lr *LockedReader) CloseAndRelease(wg *sync.WaitGroup) error {
	// add to wg so it will wait to be released when the channel is closed and consumed
	wg.Add(1)

	err := lr.Close()
	if err != nil {
		return err
	}

	wg.Wait()
	return lr.Release()
}

// CloseUpdateCheckpointAndRelease closes the reader and updates the checkpoint of the reader and releases the lock AFTER
// wg.Done() was called.
//
// IMPORTANT: You should call wg.Done() when the channel is closed and consumed. Failing to call wg.Done() will result
// in this call hanging indefinitely.
func (lr *LockedReader) CloseUpdateCheckpointAndRelease(wg *sync.WaitGroup) error {
	// add to wg so it will wait to be released when the channel is closed and consumed
	wg.Add(1)

	err := lr.Close()
	if err != nil {
		return err
	}

	wg.Wait()
	err = lr.UpdateCheckpoint()
	if err != nil {
		return err
	}

	return lr.Release()
}
