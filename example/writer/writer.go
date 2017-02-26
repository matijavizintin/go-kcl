package main

import (
	"crypto/rand"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/matijavizintin/go-kcl"
)

const (
	streamName        = "kcl-example"
	shardsCount       = 2
	messageLength     = 100
	messageOverhead   = 40
	chunkSize         = 25 * 1000
	insertionInterval = 10 * time.Second
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

	awsConfig := &aws.Config{
		Region: aws.String(region),
		Credentials: credentials.NewStaticCredentials(
			id,
			secret,
			"",
		),
	}

	client := kcl.New(awsConfig, nil, nil, nil)

	err := createStreamIfNecessary(client)
	if err != nil {
		log.Fatal(err)
	}

	for range time.Tick(insertionInterval) {
		// put random records in the shards every insertionInterval
		err := client.PutRecords(streamName, prepareBatch())
		if err != nil {
			log.Printf("Error inserting records. Err: %v", err)
			continue
		}
		log.Println("Message inserted.")
	}
}

func createStreamIfNecessary(client *kcl.Client) error {
	streams, err := client.ListStreams()
	if err != nil {
		log.Fatal(err)
	}

	found := false
	for _, name := range streams {
		if name == streamName {
			found = true
			break
		}
	}

	if !found {
		err = client.CreateStream(streamName, shardsCount)
		if err != nil {
			return err
		}

		log.Printf("Stream %s with %d shards successfully created.", streamName, shardsCount)

	}
	return nil
}

func prepareBatch() []*kinesis.PutRecordsRequestEntry {
	batch := []*kinesis.PutRecordsRequestEntry{}

	batchSize := messageLength + messageOverhead
	for {
		record, err := generateRandomBytes()
		if err != nil {
			log.Printf("Error generating random record. Err: %v", err)
			continue
		}

		batch = append(batch, &kinesis.PutRecordsRequestEntry{
			Data:         record,
			PartitionKey: aws.String(string(record[0:1])),
		})

		batchSize += messageLength + messageOverhead
		if batchSize >= chunkSize {
			break
		}
	}

	return batch
}

func generateRandomBytes() ([]byte, error) {
	b := make([]byte, messageLength)
	_, err := rand.Read(b)

	if err != nil {
		return nil, err
	}

	return b, nil
}
