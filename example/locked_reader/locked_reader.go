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
	"github.com/matijavizintin/go-kcl/locker"
	"github.com/matijavizintin/go-kcl/snitcher"
)

func main() {
	client, err := aerospike.NewClient("localhost", 3000)
	if err != nil {
		log.Fatal(err)
	}
	l := locker.NewAearospikeLocker(client, "distlock")
	cp := checkpointer.NewAerospikeCheckpointer(client, "checkpointer")
	s := snitcher.NewAerospikeSnitcher(client, "snitcher")

	awsConfig := &aws.Config{
		Region: aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(
			"",
			"",
			"",
		),
	}

	c := kcl.New(awsConfig, l, cp, s)

	for i := 0; i < 1000; i++ {
		go c.PutRecord("Test1", fmt.Sprintf("%d", i), []byte(fmt.Sprintf("record %d", i)))
		time.Sleep(1 * time.Millisecond)
	}

	reader, err := c.NewLockedReader("Test1", "shardId-000000000000", "testClient")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Reading")
	go func() {
		for m := range reader.Records() {
			log.Print("Data: ", string(m.Data))
		}
	}()

	time.Sleep(15 * time.Second)
	if err := reader.Close(); err != nil {
		log.Fatal(err)
	}
	err = reader.UpdateCheckpoint()
	if err != nil {
		log.Fatal(err)
	}
}
