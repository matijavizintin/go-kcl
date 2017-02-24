package kcl

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

type Client struct {
	kinesis    kinesisiface.KinesisAPI
	distlock   Locker
	checkpoint Checkpointer
}

type Reader struct {
	records    <-chan *kinesis.Record
	checkpoint string
}

func New(awsKey, awsSecret, awsRegion string, distlock Locker, checkpoint Checkpointer) *Client {
	awsConfig := &aws.Config{
		Region: aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(
			awsKey,
			awsSecret,
			"",
		),
	}

	return &Client{
		kinesis: kinesis.New(session.New(awsConfig)),
	}
}

func (c *Client) NewLockedShardReader(streamName, shard, checkpointName string) (*Reader, error) {
	// TODO
	return nil, nil
}

func (c *Client) PutRecord(streamName, partitionKey string, record []byte) error {
	// TODO
	return nil
}

func (r *Reader) SetCheckpoint() error {
	// TODO
	return nil
}
