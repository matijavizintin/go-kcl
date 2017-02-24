package kinesis

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func main() {
	awsConfig := &aws.Config{
		Region: aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(
			"AKIAI2MUBA4UET6O3IHA",
			"",
			"",
		),
	}
	client := kinesis.New(session.New(awsConfig))

	put(client)
	get(client)

}

func put(client *kinesis.Kinesis) {
	out, err := client.PutRecord(&kinesis.PutRecordInput{
		Data:         []byte("Dummy_put"),
		StreamName:   aws.String("Test1"),
		PartitionKey: aws.String("1"),
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(out.GoString())
}

func get(client *kinesis.Kinesis) {
	iterator, err := client.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:        aws.String("Test1"),
		ShardId:           aws.String("shardId-000000000001"),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(iterator.GoString())

	out, err := client.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: iterator.ShardIterator,
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Records: ", len(out.Records))
	for _, rec := range out.Records {
		fmt.Println(rec.GoString())
	}

}
