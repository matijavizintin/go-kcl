package election

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
	setName      = "election"
	aerospikeTTL = 5
)

var Logger = log.New(os.Stderr, "", log.LstdFlags)

type AerospikeElection struct {
	client    *aerospike.Client
	namespace string
	clientId  string

	candidates   map[string]*candidate
	candidatesMu sync.RWMutex

	rand *rand.Rand
}

type candidate struct {
	key     string
	winnner bool
	order   float64
}

func NewAerospikeElection(client *aerospike.Client, namespace string) *AerospikeElection {
	clientId, _ := newUUID()

	ae := &AerospikeElection{
		client:     client,
		namespace:  namespace,
		clientId:   clientId,
		candidates: map[string]*candidate{},
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	go ae.runElections()

	return ae
}

func (ae *AerospikeElection) runElections() {
	for range time.Tick(time.Second) {
		candidates := []*candidate{}
		ae.candidatesMu.RLock()
		for _, candidate := range ae.candidates {
			candidates = append(candidates, candidate)
		}
		ae.candidatesMu.RUnlock()

		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].order < candidates[j].order
		})

		numcandidates := 0.0
		for _, candidate := range candidates {
			asKey, err := aerospike.NewKey(ae.namespace, setName, candidate.key)
			if err != nil {
				Logger.Print(err)
				continue
			}

			record, err := ae.client.Get(nil, asKey, setName, "score", "clientId")
			if err != nil {
				Logger.Print(err)
				continue
			}

			topScore := -1.0
			topClientId := ""
			if record != nil {
				score, ok := record.Bins["score"]
				if ok {
					topScore = score.(float64)
				}
				cid, ok := record.Bins["clientId"]
				if ok {
					topClientId = cid.(string)
				}
			}

			myScore := numcandidates + 1.0
			if topClientId == "" || ae.clientId == topClientId {
				myScore -= 0.5
			}

			if (topScore > 0.0 && myScore-topScore > 0.0) && ae.clientId != topClientId {
				if candidate.winnner {
					Logger.Print("Election lost: ", candidate.key)
					candidate.winnner = false
				}
				continue
			}

			ts := time.Now().UTC().Format(time.RFC3339)

			hostname, err := os.Hostname()
			if err != nil {
				Logger.Print(err)
				continue
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
				asKey,
				aerospike.NewBin("key", candidate.key),
				aerospike.NewBin("score", myScore),
				aerospike.NewBin("clientId", ae.clientId),
				aerospike.NewBin("hostname", hostname),
				aerospike.NewBin("ts", ts),
			)
			if err != nil {
				if aserr, ok := err.(types.AerospikeError); ok && (aserr.ResultCode() == types.KEY_EXISTS_ERROR || aserr.ResultCode() == types.GENERATION_ERROR) {
					continue
				}
				Logger.Print(err)
				continue
			}

			time.Sleep(time.Second)

			record, err = ae.client.Get(nil, asKey, setName, "clientId")
			if err != nil {
				Logger.Print(err)
				continue
			}

			cid, ok := record.Bins["clientId"]
			if ok {
				newClientId := cid.(string)
				if ae.clientId == newClientId {
					numcandidates += 1.0

					if !candidate.winnner {
						Logger.Print("Election won: ", candidate.key)
						candidate.winnner = true
					}
					continue
				}
			}
			if candidate.winnner {
				Logger.Print("Election lost: ", candidate.key)
				candidate.winnner = false
			}
		}
	}
}

func (ae *AerospikeElection) CheckAndAdd(key string) bool {
	ae.candidatesMu.RLock()
	w, ok := ae.candidates[key]
	ae.candidatesMu.RUnlock()

	if !ok {
		ae.candidatesMu.Lock()
		ae.candidates[key] = &candidate{
			key:   key,
			order: ae.rand.Float64(),
		}
		ae.candidatesMu.Unlock()
		return false
	}

	return w.winnner
}
