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
	client, err := aerospike.NewClient("172.28.128.3", 3000)
	if err != nil {
		log.Fatal(err)
	}
	l := distlock.NewAearospikeLocker(client, "distlock")
	cp := checkpointer.NewAearospikeLocker(client, "distlock")

	awsConfig := &aws.Config{
		Region: aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(
			"AKIAI2MUBA4UET6O3IHA",
			"5IbcFZLdZXkG2dLWcPwNae0PbvWZleGiYSdCzhlu",
			"",
		),
	}

	c := kcl.New(awsConfig, l, cp)

	for i := 0; i < 1000; i++ {
		go c.PutRecord("Test1", fmt.Sprintf("%d", i), []byte(fmt.Sprintf("Xrecord %d", i)))
		time.Sleep(1 * time.Millisecond)
	}

	reader, err := c.NewLockedShardReader("Test1", "shardId-000000000000", "testClient")
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
