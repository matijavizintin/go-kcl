package kcl

type Checkpointer interface {
	SetCheckpoint(shardId, clientName string, value string) error
	GetCheckpoint(shardId, clientName string) (string, error)
}
