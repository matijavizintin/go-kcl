# go-kcl
Go Kinesis client library

client, err := kcl.New(credentails, locker, checkpointer)

reader, err := client.NewLockedShardReader(streamName, shard, checkpointName)
if err == ErrLocked {
  ...
}
for r := range reader.Records() {
  ...
}
reader.Checkpoint()

reader, err := kcl.NewSharedReader(streamName, checkpointName)
for r := range reader.Records() {
  ...
}
reader.Checkpoint()

client.Put(streamName, partitionKey, record)
