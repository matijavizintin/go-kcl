package kcl

func GetStreamKey(streamName, shardId, checkpointName string) string {
	return streamName + "/" + shardId + "/" + checkpointName
}
