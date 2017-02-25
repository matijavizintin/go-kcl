package checkpointer

import (
	"os"
	"time"

	"github.com/aerospike/aerospike-client-go"
)

const (
	waitSleep    = time.Duration(100) * time.Millisecond
	aerospikeTTL = 365 * 24 * 3600
	waitRetries  = 3
	setName      = "kcl_checkpoint"
)

type AerospikeReleaser struct {
	stop chan bool
	key  *aerospike.Key
}

type AerospikeCheckpointer struct {
	client    *aerospike.Client
	namespace string
}

func NewAerospikeCheckpointer(client *aerospike.Client, namespace string) *AerospikeCheckpointer {
	return &AerospikeCheckpointer{
		client:    client,
		namespace: namespace,
	}
}

func (al *AerospikeCheckpointer) GetCheckpoint(key string) (string, error) {
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

func (al *AerospikeCheckpointer) SetCheckpoint(key, value string) error {
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

func (al *AerospikeCheckpointer) get(key string) (string, error) {
	asKey, err := aerospike.NewKey(al.namespace, setName, key)
	if err != nil {
		return "", err
	}

	record, err := al.client.Get(aerospike.NewPolicy(), asKey, setName, "checkpoint")
	if err != nil {
		return "", err
	}

	if record == nil {
		return "", nil
	}

	binContent, ok := record.Bins["checkpoint"]
	if ok {
		return binContent.(string), nil
	}
	return "", nil
}

func (al *AerospikeCheckpointer) set(key, value string) error {
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
		aerospike.NewBin("checkpoint", value),
	)
	if err != nil {
		return err
	}

	return nil
}
