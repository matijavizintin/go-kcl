package main

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/matijavizintin/go-kcl"
	"github.com/matijavizintin/go-kcl/checkpointer"
	"github.com/matijavizintin/go-kcl/locker"
	"github.com/matijavizintin/go-kcl/snitcher"
)

const (
	aerospikeNamespace = "kinesis"
	streamName         = "kcl-example"
	clientName         = "kcl-test-client"
	streamReadInterval = time.Second
	readBatchSize      = 10
	channelSize        = 10
)

func main() {
	region, ok := os.LookupEnv("KCL_AWS_REGION")
	if !ok {
		log.Fatal("KCL_AWS_REGION not set")
	}
	id, ok := os.LookupEnv("KCL_AWS_ID")
	if !ok {
		log.Fatal("KCL_AWS_ID not set")
	}
	secret, ok := os.LookupEnv("KCL_AWS_SECRET")
	if !ok {
		log.Fatal("KCL_AWS_SECRET not set")
	}
	asHostname, ok := os.LookupEnv("KCL_AS_HOSTNAME")
	if !ok {
		log.Fatal("KCL_AS_HOSTNAME not set")
	}

	asClient, err := aerospike.NewClient(asHostname, 3000)
	if err != nil {
		log.Fatal(err)
	}

	aerospikeLocker := locker.NewAearospikeLocker(asClient, aerospikeNamespace)
	aerospikeCheckpointer := checkpointer.NewAerospikeCheckpointer(asClient, aerospikeNamespace)
	aerospikeSnitcher := snitcher.NewAerospikeSnitcher(asClient, aerospikeNamespace)

	awsConfig := &aws.Config{
		Region: aws.String(region),
		Credentials: credentials.NewStaticCredentials(
			id,
			secret,
			"",
		),
	}

	client := kcl.New(awsConfig, aerospikeLocker, aerospikeCheckpointer, aerospikeSnitcher)
	log.Print("Client created")

	// get stream description to retrieve shard ids
	streamDescription, err := client.StreamDescription(streamName)
	if err != nil {
		log.Fatal(err)
	}

	if len(streamDescription.Shards) == 0 {
		log.Fatal("No shards in stream")
	}
	shardId := *streamDescription.Shards[1].ShardId // consume only first shard

	// create a reader for first shard
	reader, err := client.NewLockedReaderWithParameters(streamName, shardId, clientName, streamReadInterval, readBatchSize, channelSize)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Reader on shard %s created", shardId)

	wg := &sync.WaitGroup{}
	go func(wg *sync.WaitGroup) {
		for record := range reader.Records() {
			log.Printf("Record read: %s", string(record.Data))
		}

		// release the lock, this is used to signal that the channel was exhausted and checkpoint can be
		// safely set
		wg.Done()
		log.Println("Channel exhaused, lock released")
	}(wg)

	// consume the stream for 5 seconds and the terminate
	time.Sleep(5 * time.Second)
	err = reader.CloseUpdateCheckpointAndRelease(wg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Reader closed, checkpoint updated and released")
}
