package main

import (
	"log"

	"github.com/aerospike/aerospike-client-go"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/matijavizintin/go-kcl"
	"github.com/matijavizintin/go-kcl/distlock"
)

func main() {
	client, err := aerospike.NewClient("localhost", 3000)
	if err != nil {
		log.Fatal(err)
	}
	l := distlock.NewAearospikeLocker(client, "test")

	awsConfig := &aws.Config{
		Region: aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(
			"AKIAI2MUBA4UET6O3IHA",
			"",
			"",
		),
	}

	c := kcl.New(awsConfig, l, nil)

	c.PutRecord("test1", "key", []byte("record"))

	reader, err := c.NewSharedReader("test1", "testClient")
	if err != nil {
		log.Fatal(err)
	}

	for m := range reader.Records() {
		log.Print(m.GoString())
	}

	if err := reader.Close(); err != nil {
		log.Fatal(err)
	}
}
