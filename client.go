package kcl

import (
	"errors"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/matijavizintin/go-kcl/checkpointer"
	"github.com/matijavizintin/go-kcl/locker"
	"github.com/matijavizintin/go-kcl/snitcher"
)

var (
	ErrMissingLocker       = errors.New("Missing locker")
	ErrMissingCheckpointer = errors.New("Missing checkpointer")
	ErrMissingSnitcher     = errors.New("Missing snitcher")
	ErrShardLocked         = errors.New("Shard locked")
)

var Logger = log.New(os.Stderr, "", log.LstdFlags)

type Client struct {
	kinesis    kinesisiface.KinesisAPI
	distlock   locker.Locker
	checkpoint checkpointer.Checkpointer
	snitch     snitcher.Snitcher
}

func New(awsConfig *aws.Config, distlock locker.Locker, checkpoint checkpointer.Checkpointer, snitch snitcher.Snitcher) *Client {
	return &Client{
		kinesis:    kinesis.New(session.New(awsConfig)),
		distlock:   distlock,
		checkpoint: checkpoint,
		snitch:     snitch,
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

func (c *Client) PutRecords(streamName, partitionKey string, records []*kinesis.PutRecordsRequestEntry) error {
	_, err := c.kinesis.PutRecords(&kinesis.PutRecordsInput{
		Records:    records,
		StreamName: aws.String(streamName),
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

func (c *Client) CreateStream(streamName string, shardCount int) error {
	_, err := c.kinesis.CreateStream(&kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int64(int64(shardCount)),
	})
	return err
}

func (c *Client) UpdateStream(streamName string, shardsCount int) error {
	_, err := c.kinesis.UpdateShardCount(&kinesis.UpdateShardCountInput{
		StreamName:       aws.String(streamName),
		ScalingType:      aws.String(kinesis.ScalingTypeUniformScaling),
		TargetShardCount: aws.Int64(int64(shardsCount)),
	})
	return err
}

func (c *Client) DeleteStream(streamName string) error {
	_, err := c.kinesis.DeleteStream(&kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
	return err
}

func (c *Client) ListStreams() ([]string, error) {
	res := []string{}

	hasMore := true
	for hasMore {
		out, err := c.kinesis.ListStreams(&kinesis.ListStreamsInput{})
		if err != nil {
			return nil, err
		}

		hasMore = *out.HasMoreStreams
		for _, streamName := range out.StreamNames {
			res = append(res, *streamName)
		}
	}

	return res, nil
}
