package main

import (
	"log"

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

	awsConfig := &aws.Config{
		Region: aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(
			"",
			"",
			"",
		),
	}

	locker := distlock.NewAearospikeLocker(client, "test")
	checkpointer := checkpointer.NewAerospikeCheckpointer(client, "test")

	c := kcl.New(awsConfig, locker, checkpointer)

	reader, err := c.NewSharedReader("Demo", "testClient2")
	if err != nil {
		log.Fatal(err)
	}

	for range reader.Records() {
		//log.Print("Data: ", string(m.Data))

		err := reader.UpdateCheckpoint()
		if err != nil {
			log.Fatal(err)
		}
	}

	if err := reader.Close(); err != nil {
		log.Fatal(err)
	}
}
