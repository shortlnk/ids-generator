package ids

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
	"strconv"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

const EPOCH int64 = 1672527600000
const MAX_WORKER_ID int64 = (1 << 10) - 1


type Snowflake struct {
	workerIDBits uint
	sequenceBits uint
	maxSequence int64

	workerID int64
	sequence int64
	lastTimestamp int64
	mutex sync.Mutex
}

func NewSnowflake(workerID int64) (*Snowflake, error) {
	if workerID < 0 || workerID > MAX_WORKER_ID {
		return nil, fmt.Errorf("worker ID must be between 0 and %d", MAX_WORKER_ID)
	}

	sf := &Snowflake{
		workerIDBits: 5,
		sequenceBits: 12,
		maxSequence: (1 << 5) - 1,
		workerID: workerID,
		sequence: 0,
		lastTimestamp: 0,
		mutex: sync.Mutex{},
	}

	return sf, nil
}

func (sf *Snowflake) GenerateID() int64 {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()

	timestamp := time.Now().UnixMilli()

	if timestamp < sf.lastTimestamp {
		time.Sleep(time.Duration(sf.lastTimestamp - timestamp))
		timestamp = time.Now().UnixMilli()
	}

	if timestamp == sf.lastTimestamp {
		sf.sequence = (sf.sequence + 1) & sf.maxSequence
		if sf.sequence == 0 {
			for timestamp <= sf.lastTimestamp {
				timestamp = time.Now().UnixMilli()
			}
		}
	} else {
		sf.sequence = 0
	}

	sf.lastTimestamp = timestamp

	id := (timestamp - EPOCH) << (sf.workerIDBits + sf.sequenceBits)
	id |= (sf.workerID << int64(sf.sequenceBits))
	id |= sf.sequence

	return id
}

func init() {
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))

	id := rd.Intn(32)

	sf, err := NewSnowflake(int64(id))
	if err != nil {
		log.Fatal("Error creating Snowflake:", err)
	}

	functions.HTTP("GenIdGet", genIdGet(sf))
}

type Output struct{
	Id string `json:"id"`
}

func genIdGet(sf *Snowflake) func (w http.ResponseWriter, r *http.Request) {
	return func (w http.ResponseWriter, r *http.Request) {
		id := sf.GenerateID()

		if err := json.NewEncoder(w).Encode(Output{ Id: strconv.FormatInt(id, 10) }); err != nil {
			fmt.Fprint(w, "error occured")
		}
	}
}
