package checkpointer

type Checkpointer interface {
	SetCheckpoint(key string, value string) error
	GetCheckpoint(key string) (string, error)
}
