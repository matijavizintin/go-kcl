# go-kcl
Go Kinesis client library

We don't really believe in tests, we test in production, but anyway they [![CircleCI](https://circleci.com/gh/matijavizintin/go-kcl.svg?style=svg)](https://circleci.com/gh/matijavizintin/go-kcl)

This library is a wrapper around kinesis part of AWS SDK. It facilitates reading from and putting to streams. 

It depends on AWS SDK and Aerospike client library o first of all you'll need
```
go get github.com/aws/aws-sdk-go
go get github.com/aerospike/aerospike-client-go
```
Aerospike is currently used to store locks and state but we plan to add support for etcd.

### Consuming the stream
It supports reading from a single shard and locking it so two clients don't consume the same shard. Example

```
client := kcl.New(awsConfig, locker, checkpointer)

reader, err := client.NewLockedShardReader(streamName, shardId, clientName)
if err != nil {
    return err
}

go func() {
		for record := range reader.Records() {
			// handle record
		}
	}()
	
// wait for until ready to close

if err := reader.Close(); err != nil {
    return err
}

if err = reader.UpdateCheckpoint(); err != nil {
    return err
}
```

It also supports also the shared reader that tries to read from as many shards as available. Example:

```
client := kcl.New(awsConfig, locker, checkpointer)

reader, err := client.NewSharedReader(streamName, clientName)
if err != nil {
    return err
}

go func() {
		for record := range reader.Records() {
			// handle record
		}
	}()
	
// wait for until ready to close

if err := reader.Close(); err != nil {
    return err
}

if err = reader.UpdateCheckpoint(); err != nil {
    return err
}
```

### Pushing into the stream

Example of putting a record into a stream:

```
client := kcl.New(awsConfig, locker, checkpointer)

err := client.PutRecord(streamName, partitionKey, record)
```
