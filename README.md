# go-kcl
Go Kinesis client library

**NOTE: This library currently covers only the streaming part of Kinesis**

We don't really believe in tests, we test in production, but anyway they [![CircleCI](https://circleci.com/gh/matijavizintin/go-kcl.svg?style=svg)](https://circleci.com/gh/matijavizintin/go-kcl)

This library is a wrapper around kinesis part of AWS SDK. It facilitates reading from and putting into the streams. 

It depends on AWS SDK and Aerospike client library so first of all you'll need
```
go get github.com/aws/aws-sdk-go
go get github.com/aerospike/aerospike-client-go
```
Aerospike is currently used to store locks and state but we plan to add support for etcd in the future.

### Stream manipulation
Client:
```
client := kcl.New(awsConfig, locker, checkpointer, snitcher)
```

Stream creation example:
```
err := client.CreateStream(streamName, shardCount)
if err != nil {
    // handle err
}

// so something with the stream
```

Stream update example (change the number of shards):
```
err := client.UpdateStream(streamName, shardsCount)
if err != nil {
    // handle err
}

// so something with new shards
```

Delete stream example:
```
err := client.DeleteStream(streamName)
if err != nil {
    // handle err
}

```

List streams:
```
streamNames, err := client.ListStreams()
if err != nil {
    // handle err
}

// do something with streams
```

### Consuming the stream
It supports reading from a single shard and locking it so two clients don't consume the same shard. Example:

```
client := kcl.New(awsConfig, locker, checkpointer)

reader, err := client.NewLockedReader(streamName, shardId, clientName)
if err != nil {
    return err
}

wg := &sync.WaitGroup{}
go func(wg *sync.WaitGroup) {
		for record := range reader.Records() {
			// handle record
		}
		
		wg.Done()
	}(wg)
	
// wait for until ready to close
err = reader.CloseUpdateCheckpointAndRelease(wg)
```

It also supports also the shared reader that tries to read from as many shards as available. Example:

```
client := kcl.New(awsConfig, locker, checkpointer, snitch)

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
err = reader.CloseUpdateCheckpointAndRelease()
if err != nil {
    // handle err
}

err = reader.UpdateCheckpoint()
```

### Pushing into the stream

Example of putting a record into a stream:

```
client := kcl.New(awsConfig, locker, checkpointer, snitcher)

err := client.PutRecord(streamName, partitionKey, record)
```

```
client := kcl.New(awsConfig, locker, checkpointer, snitcher)

err := client.PutRecords(streamName, records)
```
