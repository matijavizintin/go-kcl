package kcl

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

type Client struct {
	kinesis kinesisiface.KinesisAPI
}

func New(awsKey, awsSecret, awsRegion string) *Client {
	awsConfig := &aws.Config{
		Region: aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(
			"AKIAI2MUBA4UET6O3IHA",
			"",
			"",
		),
	}
	return &Client{
		kinesis: kinesis.New(session.New(awsConfig)),
	}
}
