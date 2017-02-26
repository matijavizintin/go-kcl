package snitcher

import (
	"log"
	"os"
)

var Logger = log.New(os.Stderr, "", log.LstdFlags)

type Snitcher interface {
	RegisterKey(key string)
	CheckOwnership(key string) bool
}
