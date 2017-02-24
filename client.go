package kcl

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

var (
	ErrMissingLocker       = errors.New("Missing locker")
	ErrMissingCheckpointer = errors.New("Missing checkpointer")
)

type Client struct {
	kinesis    kinesisiface.KinesisAPI
	distlock   Locker
	checkpoint Checkpointer
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
		kinesis:    kinesis.New(session.New(awsConfig)),
		distlock:   distlock,
		checkpoint: checkpoint,
	}
}

func (c *Client) PutRecord(streamName, partitionKey string, record []byte) error {
	_, err := c.kinesis.PutRecord(&kinesis.PutRecordInput{
		Data:         record,
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String(partitionKey),
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) StreamDescription(streamName string) (*kinesis.StreamDescription, error) {
	out, err := c.kinesis.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	if err != nil {
		return nil, err
	}

	return out.StreamDescription, nil
}
