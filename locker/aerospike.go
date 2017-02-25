package locker

import (
	"os"
	"time"

	"github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
)

const (
	aerospikePingInterval = time.Second
	waitSleep             = time.Duration(100) * time.Millisecond
	aerospikeTTL          = 5
	waitRetries           = 3
	setName               = "kcl-distlock"
)

type AerospikeReleaser struct {
	locker *AerospikeLocker

	stop chan bool
	key  *aerospike.Key
}

func NewAerospikeReleaser(locker *AerospikeLocker, stop chan bool, key *aerospike.Key) *AerospikeReleaser {
	return &AerospikeReleaser{
		locker: locker,
		stop:   stop,
		key:    key,
	}
}

func (ar *AerospikeReleaser) Release() error {
	ar.stop <- true

	_, err := ar.locker.client.Delete(nil, ar.key)
	if err != nil {
		return err
	}

	return nil
}

type AerospikeLocker struct {
	client    *aerospike.Client
	namespace string
}

func NewAearospikeLocker(client *aerospike.Client, namespace string) *AerospikeLocker {
	return &AerospikeLocker{
		client:    client,
		namespace: namespace,
	}
}

func (al *AerospikeLocker) LockWait(name string) (Releaser, error) {
	var err error
	var releaser Releaser
	var success bool

	errTries := 0
	for {
		releaser, success, err = al.Lock(name)
		if err != nil {
			errTries++
			if errTries > waitRetries {
				return nil, err
			}
		} else {
			errTries = 0
		}

		if success == true {
			return releaser, nil
		}

		time.Sleep(waitSleep)
	}
}

func (al *AerospikeLocker) Lock(name string) (Releaser, bool, error) {
	key, err := aerospike.NewKey(al.namespace, setName, name)
	if err != nil {
		return nil, false, err
	}

	lockedTs := time.Now().UTC().Format(time.RFC3339)

	hostname, err := os.Hostname()
	if err != nil {
		return nil, false, err
	}

	policy := aerospike.NewWritePolicy(0, aerospikeTTL)
	policy.RecordExistsAction = aerospike.CREATE_ONLY

	err = al.client.PutBins(
		policy,
		key,
		aerospike.NewBin("name", name),
		aerospike.NewBin("hostname", hostname),
		aerospike.NewBin("locked", lockedTs),
		aerospike.NewBin("updated", time.Now().UTC().Format(time.RFC3339)),
	)
	if err != nil {
		if aserr, ok := err.(types.AerospikeError); ok && aserr.ResultCode() == types.KEY_EXISTS_ERROR {
			return nil, false, nil
		}
		return nil, false, err
	}

	stop := make(chan bool)
	go func() {
		pingTicker := time.NewTicker(aerospikePingInterval)
		for {
			al.client.PutBins(
				aerospike.NewWritePolicy(0, aerospikeTTL),
				key,
				aerospike.NewBin("updated", time.Now().UTC().Format(time.RFC3339)),
			)
			if err != nil {
				Logger.Printf("Aerospike locker %s error", name)
			}

			select {
			case <-pingTicker.C:
			case <-stop:
				pingTicker.Stop()
				return
			}
		}
	}()

	return NewAerospikeReleaser(al, stop, key), true, nil
}
