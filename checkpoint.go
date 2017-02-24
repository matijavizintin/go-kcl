package kcl

type Checkpointer interface {
	SetCheckpoint(shardIterator string, value string) error
	GetCheckpoint(shardIterator string) (string, error)
}
