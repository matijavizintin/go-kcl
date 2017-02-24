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
	l := distlock.NewAearospikeLocker(client, "test")
	cp := checkpointer.NewAearospikeLocker(client, "test")

	awsConfig := &aws.Config{
		Region: aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(
			"AKIAI2MUBA4UET6O3IHA",
			"5IbcFZLdZXkG2dLWcPwNae0PbvWZleGiYSdCzhlu",
			"",
		),
	}

	c := kcl.New(awsConfig, l, cp)

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
