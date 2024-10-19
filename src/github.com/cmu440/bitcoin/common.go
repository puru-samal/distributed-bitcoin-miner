package bitcoin

import (
	"encoding/json"
	"log"
	"math"
	"time"

	"github.com/cmu440/lsp"
)

// Miner  _______________________________________________________________________________

// Helper function that handles miner processing.
// Reads a request from a server, does its work and sends result to server
// If a non-nil error is encountered during Read/Write, it returns
func ProcessMiner(miner lsp.Client, logger *log.Logger) {
	for {

		// will block until request sent by server
		payload, err := miner.Read()

		// miner loses contact with the server -> shut down
		if err != nil {
			logger.Printf("[Miner[id %d] Disconnect]\n", miner.ConnID())
			return
		}

		// initialize with lower hash
		// search from [lower+1, upper]
		// for lowest hash and corresponding nonce

		var request Message
		json.Unmarshal(payload, &request)
		logger.Printf("[Miner[id %d] MsgRecv]: %s\n", miner.ConnID(), request.String())

		hash := Hash(request.Data, request.Lower)
		nonce := request.Lower

		for n := request.Lower + 1; n <= request.Upper; n++ {
			h := Hash(request.Data, n)
			if hash > h {
				hash = h
				nonce = n
			}
		}

		result := NewResult(hash, nonce)
		rpayload, _ := json.Marshal(result)

		// miner loses contact with the server -> shut down
		rerr := miner.Write(rpayload)
		logger.Printf("[Miner[id %d] MsgSend]: %s\n", miner.ConnID(), request.String())

		if rerr != nil {
			logger.Printf("[Miner[id %d] Disconnect]\n", miner.ConnID())
			return
		}
	}
}

// Client _______________________________________________________________________________

// Server _______________________________________________________________________________

// Misc.  _______________________________________________________________________________

// RunningStats  ________________________________________________________________________
// A data structure that keeps track of the running mean
// and standard deviation based on the Welford's algorithm.

// usage:
// initialize                            ->  rs := RunningStats{}
// record new job                        -> rs.StartRecording(jobID)
// record job end time and update stats  -> rs.StopRecording(jobID)
// get the running stats (mean, std_dev) -> rs.GetStats()
// mean    = the measure of efficiency
// std_dev = the measure of fairness

type RunningStats struct {
	n             int               // number of samples
	mean          float64           // running mean
	M2            float64           // sum of squared diffs from the mean (used for std_dev)
	activeRecords map[int]time.Time // map keeping track of job start times
}

// records the start time of a job
func (rs *RunningStats) StartRecording(id int) {
	rs.activeRecords[id] = time.Now()
}

// records the end time of an active job
// and updates values required to keep track of running stats
func (rs *RunningStats) StopRecording(id int) {
	startTime, ok := rs.activeRecords[id]
	if ok {
		stopTime := time.Now()
		dur := stopTime.Sub(startTime).Seconds()
		rs.Update(dur)
		delete(rs.activeRecords, id)
	}
}

// adds a new data point and updates
// the running mean and sum of squared diffs
func (rs *RunningStats) Update(x float64) {
	rs.n++
	delta := x - rs.mean
	rs.mean += delta / float64(rs.n)
	delta2 := x - rs.mean
	rs.M2 += delta * delta2
}

// returns the running mean and standard deviation
func (rs *RunningStats) GetStats() (float64, float64) {
	std_dev := 0.0
	if rs.n > 1 {
		std_dev = math.Sqrt(rs.M2 / float64(rs.n))
	}
	return rs.mean, std_dev
}
