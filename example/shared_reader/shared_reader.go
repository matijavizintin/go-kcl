package main

import (
	"fmt"
	"log"
	"time"

	"github.com/aerospike/aerospike-client-go"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/matijavizintin/go-kcl"
	"github.com/matijavizintin/go-kcl/checkpointer"
	"github.com/matijavizintin/go-kcl/distlock"
)

func main() {
	client, err := aerospike.NewClient("localhost", 3000)
	if err != nil {
		log.Fatal(err)
	}
	l := distlock.NewAearospikeLocker(client, "test")
	cp := checkpointer.NewAearospikeLocker(client, "test")

	awsConfig := &aws.Config{
		Region: aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(
			"AKIAI2MUBA4UET6O3IHA",
			"",
			"",
		),
	}

	c := kcl.New(awsConfig, l, cp)

	go func() {
		for i := 0; i < 1000; i++ {
			c.PutRecord("Test1", fmt.Sprintf("%d", i), []byte(fmt.Sprintf("record %d", i)))
			time.Sleep(time.Second)
		}
	}()

	reader, err := c.NewSharedReader("Test1", "testClient")
	if err != nil {
		log.Fatal(err)
	}

	for m := range reader.Records() {
		log.Print("Data: ", string(m.Data))
	}

	if err := reader.Close(); err != nil {
		log.Fatal(err)
	}
}
