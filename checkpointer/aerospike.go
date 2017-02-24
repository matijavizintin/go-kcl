package checkpointer

import (
	"os"
	"time"

	"github.com/aerospike/aerospike-client-go"
)

const waitSleep = time.Duration(100) * time.Millisecond
const aerospikeTTL = 365 * 24 * 3600
const waitRetries = 3
const setName = "checkpoint"
const valueBinName = "checkpoint"

type AerospikeReleaser struct {
	locker *AerospikeLocker

	stop chan bool
	key  *aerospike.Key
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

func (al *AerospikeLocker) GetCheckpoint(key string) (string, error) {
	errTries := 0
	for {
		val, err := al.get(key)
		if err != nil {
			errTries++
			if errTries > waitRetries {
				return "", err
			}
			time.Sleep(waitSleep)
			continue
		}
		return val, nil
	}
}

func (al *AerospikeLocker) SetCheckpoint(key, value string) error {
	var err error

	errTries := 0
	for {
		err = al.set(key, value)
		if err != nil {
			errTries++
			if errTries > waitRetries {
				return err
			}
			time.Sleep(waitSleep)
			continue
		}
		return nil
	}
}

func (al *AerospikeLocker) get(key string) (string, error) {
	asKey, err := aerospike.NewKey(al.namespace, setName, key)
	if err != nil {
		return "", err
	}

	record, err := al.client.Get(aerospike.NewPolicy(), asKey, setName)
	if err != nil {
		return "", err
	}

	if record == nil {
		return "", nil
	}

	for binName, binContent := range record.Bins {
		if binName == valueBinName {
			return binContent.(string), nil
		}
	}
	return "", nil
}

func (al *AerospikeLocker) set(key, value string) error {
	asKey, err := aerospike.NewKey(al.namespace, setName, key)
	if err != nil {
		return err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	policy := aerospike.NewWritePolicy(0, aerospikeTTL)
	policy.RecordExistsAction = aerospike.UPDATE

	err = al.client.PutBins(
		policy,
		asKey,
		aerospike.NewBin("hostname", hostname),
		aerospike.NewBin("updated", time.Now().UTC().Format(time.RFC3339)),
		aerospike.NewBin(valueBinName, value),
	)
	if err != nil {
		return err
	}

	return nil
}
