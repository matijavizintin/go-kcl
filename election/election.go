package election

type Election interface {
	CheckAndAdd(key string) bool
}
