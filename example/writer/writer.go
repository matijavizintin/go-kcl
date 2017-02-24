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
			"AKIAI2MUBA4UET6O3IHA",
			"5IbcFZLdZXkG2dLWcPwNae0PbvWZleGiYSdCzhlu",
			"",
		),
	}

	c := kcl.New(awsConfig, nil, nil)

	for i := 0; i < 100000; i++ {
		ts := time.Now().Format(time.RFC3339)
		c.PutRecord("Demo", ts, []byte(ts))
		time.Sleep(time.Millisecond * 100)
	}
}
