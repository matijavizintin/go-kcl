package main

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/matijavizintin/go-kcl"
)

func main() {
	awsConfig := &aws.Config{
		Region: aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(
			"",
			"",
			"",
		),
	}

	c := kcl.New(awsConfig, nil, nil, nil)

	for i := 0; i < 100000; i++ {
		ts := time.Now().Format(time.RFC3339)
		c.PutRecord("Demo", ts, []byte(ts))
		time.Sleep(time.Millisecond * 100)
	}
}
