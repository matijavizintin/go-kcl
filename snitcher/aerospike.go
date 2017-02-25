package snitcher

import (
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
)

const (
	setName        = "kcl_snitcher"
	aerospikeTTL   = 5
	updateInterval = time.Second
)

var Logger = log.New(os.Stderr, "", log.LstdFlags)

type AerospikeSnitcher struct {
	client    *aerospike.Client
	namespace string
	clientId  string

	candidates       map[string]*candidate
	sortedCandidates []*candidate
	candidatesMu     sync.RWMutex

	rand *rand.Rand
}

type candidate struct {
	key     string
	winnner bool
	order   float64
}

func NewAerospikeSnitcher(client *aerospike.Client, namespace string) *AerospikeSnitcher {
	clientId, _ := newUUID()

	ae := &AerospikeSnitcher{
		client:     client,
		namespace:  namespace,
		clientId:   clientId,
		candidates: map[string]*candidate{},
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	go ae.runSnitchers()

	return ae
}

func (ae *AerospikeSnitcher) getRecord(key *aerospike.Key) (*aerospike.Record, string, float64, error) {
	record, err := ae.client.Get(nil, key, setName, "weight", "clientId")
	if err != nil {
		return nil, "", -1, err
	}

	weight := -1.0
	clientId := ""
	if record != nil {
		s, ok := record.Bins["weight"]
		if ok {
			weight = s.(float64)
		}
		cid, ok := record.Bins["clientId"]
		if ok {
			clientId = cid.(string)
		}
	}

	return record, clientId, weight, nil
}

func (ae *AerospikeSnitcher) updateRecord(record *aerospike.Record, key *aerospike.Key, candidate *candidate, weight float64) error {
	ts := time.Now().UTC().Format(time.RFC3339)

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	policy := aerospike.NewWritePolicy(0, aerospikeTTL)
	if record != nil {
		policy.Generation = record.Generation
		policy.GenerationPolicy = aerospike.EXPECT_GEN_EQUAL
	} else {
		policy.RecordExistsAction = aerospike.CREATE_ONLY
	}

	err = ae.client.PutBins(
		policy,
		key,
		aerospike.NewBin("key", candidate.key),
		aerospike.NewBin("weight", weight),
		aerospike.NewBin("clientId", ae.clientId),
		aerospike.NewBin("hostname", hostname),
		aerospike.NewBin("ts", ts),
	)
	if aserr, ok := err.(types.AerospikeError); ok && (aserr.ResultCode() == types.KEY_EXISTS_ERROR || aserr.ResultCode() == types.GENERATION_ERROR) {
		return nil
	} else if err != nil {
		return err
	}

	return nil
}

func (ae *AerospikeSnitcher) runSnitchers() {
	for range time.Tick(updateInterval) {
		ae.candidatesMu.RLock()
		candidatesCopy := append([]*candidate{}, ae.sortedCandidates...)
		ae.candidatesMu.RUnlock()

		ownSeq := 0.0
		newOwnership := false
		for _, candidate := range candidatesCopy {
			asKey, err := aerospike.NewKey(ae.namespace, setName, candidate.key)
			if err != nil {
				Logger.Print(err)
				continue
			}

			record, currentClientId, currentWeight, err := ae.getRecord(asKey)

			newWeight := ownSeq
			if currentClientId != "" && ae.clientId != currentClientId {
				if newWeight+1.0 > currentWeight {
					if candidate.winnner {
						Logger.Print("Ownership lost: ", candidate.key)
						candidate.winnner = false
					}
					continue
				}
				if newOwnership {
					// One new shard per cycle
					continue
				}
			}

			err = ae.updateRecord(record, asKey, candidate, newWeight)
			if err != nil {
				Logger.Print(err)
				continue
			}

			_, newClientId, _, err := ae.getRecord(asKey)
			if err != nil {
				Logger.Print(err)
				continue
			}

			// Check for database race
			if ae.clientId == newClientId {
				ownSeq += 1.0

				if !candidate.winnner {
					candidate.winnner = true
					newOwnership = true
					Logger.Print("Ownership won: ", candidate.key)
				}
			} else {
				if candidate.winnner {
					candidate.winnner = false
					Logger.Print("Ownership lost: ", candidate.key)
				}
			}
		}
	}
}

func (ae *AerospikeSnitcher) CheckOwnership(key string) bool {
	ae.candidatesMu.RLock()
	w, ok := ae.candidates[key]
	ae.candidatesMu.RUnlock()

	if !ok {
		return false
	}

	return w.winnner
}

func (ae *AerospikeSnitcher) RegisterKey(key string) {
	ae.candidatesMu.RLock()
	_, ok := ae.candidates[key]
	ae.candidatesMu.RUnlock()

	if ok {
		return
	}

	c := &candidate{
		key:   key,
		order: ae.rand.Float64(),
	}

	ae.candidatesMu.Lock()
	defer ae.candidatesMu.Unlock()

	ae.candidates[key] = c
	ae.sortedCandidates = append(ae.sortedCandidates, c)

	sort.Slice(ae.sortedCandidates, func(i, j int) bool {
		return ae.sortedCandidates[i].order < ae.sortedCandidates[j].order
	})
}
