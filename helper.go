package kcl

func GetStreamKey(streamName, shardId, clientName string) string {
	return streamName + "/" + shardId + "/" + clientName
}
