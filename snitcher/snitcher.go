package snitcher

type Snitcher interface {
	RegisterKey(key string)
	CheckOwnership(key string) bool
}
