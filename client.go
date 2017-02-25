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
	"github.com/matijavizintin/go-kcl/distlock"
	"github.com/matijavizintin/go-kcl/election"
)

var (
	ErrMissingLocker       = errors.New("Missing locker")
	ErrMissingCheckpointer = errors.New("Missing checkpointer")
	ErrShardLocked         = errors.New("Shard locked")
)

var Logger = log.New(os.Stderr, "", log.LstdFlags)

type Client struct {
	kinesis    kinesisiface.KinesisAPI
	distlock   distlock.Locker
	checkpoint checkpointer.Checkpointer
	elections  election.Election
}

func New(awsConfig *aws.Config, distlock distlock.Locker, checkpoint checkpointer.Checkpointer, elections election.Election) *Client {
	return &Client{
		kinesis:    kinesis.New(session.New(awsConfig)),
		distlock:   distlock,
		checkpoint: checkpoint,
		elections:  elections,
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
