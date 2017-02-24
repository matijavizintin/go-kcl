package distlock

import (
	"log"
	"os"
)

var Logger = log.New(os.Stderr, "", log.LstdFlags)

type Releaser interface {
	Release() error
}

type Locker interface {
	Lock(string) (releaser Releaser, success bool, err error)
	LockWait(string) (releaser Releaser, err error)
}
