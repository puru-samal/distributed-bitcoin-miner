package bitcoin

import (
	"encoding/json"
	"math"
	"time"

	"github.com/cmu440/lsp"
)

// Chunk  _______________________________________________________________________________
// In this system, a Chunk is the smallest unit of work that can be done by a miner
type Chunk struct {
	request *Message // The job request msg for this chunk
}

// Job  _________________________________________________________________________________
// A job is essentially a collection of chunks
type Job struct {
	server        lsp.Server
	clientID      int            // client connID that submitted the request
	data          string         // the data sent in a request
	maxNonce      uint64         // notion of size
	startTime     time.Time      // update with time.Now() when a client request is recv'd
	minHash       uint64         // the current min hash
	minNonce      uint64         // the current minNonce
	pendingChunks *ChunkQ        // queue of pending chunks
	minerMap      map[int]*Chunk // map of minerID -> *Chunk
}

// creates a new job upon getting a client request
// breaks the client request into Chunks that are stored in pendingChunks
func NewJob(server lsp.Server, clientID int, data string, clientRequest *Message, chunkSize uint64) *Job {

	job := &Job{
		server:        server,
		clientID:      clientID,
		data:          data,
		maxNonce:      clientRequest.Upper,
		startTime:     time.Now(),
		minHash:       math.MaxUint64,
		minNonce:      math.MaxUint64,
		pendingChunks: &ChunkQ{},
		minerMap:      make(map[int]*Chunk),
	}

	for i := uint64(0); i < job.maxNonce; i += chunkSize {
		lower := i
		upper := i + chunkSize - 1
		if upper > job.maxNonce {
			upper = job.maxNonce
		}
		chunk := &Chunk{
			request: NewRequest(job.data, lower, upper),
		}
		job.pendingChunks.Enqueue(chunk)
	}
	return job
}

// assigns a job to a miner
// this involves removing a chunk from a queue
// if it exists, then moving it to the minerMap
// a payload should be immediately sent to the miner
func (job *Job) AssignToMiner(minerID int) bool {
	chunk, exist := job.pendingChunks.Dequeue() // Get the first chunk
	if !exist {
		return false
	}

	job.minerMap[minerID] = chunk
	payload, _ := json.Marshal(chunk.request)
	job.server.Write(minerID, payload)
	return true
}

// updates minHash and minNonce when a result is recv'd from a miner
// and then removes the chunk from the minerMap
// server should immediately check if a job is compete, if yes then send result to client
func (job *Job) ProcessResult(minerID int, minerResult *Message) {
	_, exist := job.minerMap[minerID]
	if exist {
		if job.minHash > minerResult.Hash {
			job.minHash = minerResult.Hash
			job.minNonce = minerResult.Nonce
		}
		delete(job.minerMap, minerID)
	}
}

func (job *Job) ProcessComplete() {
	result := NewResult(job.minHash, job.minNonce)
	rpayload, _ := json.Marshal(result)
	job.server.Write(job.clientID, rpayload)
}

// condition for when a job is considered to be complete
func (job *Job) Complete() bool {
	return len(job.minerMap) == 0 && job.pendingChunks.Empty()
}

// Miner  ______________________________________________________________________________
type Miner struct {
	minerID      int // miner connID that joined the server
	currclientID int // current job worker is assigned to. Note: Miner is only processing a chunk.
}

func NewMiner(minerID int) *Miner {
	worker := &Miner{
		minerID:      minerID,
		currclientID: -1,
	}
	return worker
}

func (w *Miner) Busy() bool {
	return w.currclientID != -1
}

// Scheduler Interface  _________________________________________________________________
// A generic scheduler interface for load balancing algorithms.
type Scheduler interface {
	AddMiner(minerID int)
	RemoveMiner(minerID int)
	AddJob(job *Job)
	RemoveJob(clientID int)
	ScheduleJobs()
}

// FCFS Scheduler  ______________________________________________________________________
// A simple baseline first-come-first-served scheduler. Requests are processed in the order
// they are recieved
type FCFS struct {
	server lsp.Server
	miners []*Miner
	jobs   []*Job
}

// initialize
func NewFCFS(srv lsp.Server) *FCFS {
	return &FCFS{
		server: srv,
		miners: make([]*Miner, 0),
		jobs:   make([]*Job, 0),
	}
}

// add a new miner when a join message is recieved
func (fcfs *FCFS) AddMiner(minerID int) {
	miner := NewMiner(minerID)
	fcfs.miners = append(fcfs.miners, miner)
}

// remove a miner when a miner is disconnected
func (fcfs *FCFS) RemoveMiner(minerID int) {
	for i, miner := range fcfs.miners {
		if miner.minerID == minerID {
			fcfs.miners = append(fcfs.miners[:i], fcfs.miners[i+1:]...)
			break
		}
	}
}

// add a job to the job list when a client request is recv'd
func (fcfs *FCFS) AddJob(job *Job) {
	fcfs.jobs = append(fcfs.jobs, job)
}

// remove a job from the job list when a job is complete
func (fcfs *FCFS) RemoveJob(clientID int) {
	for i, job := range fcfs.jobs {
		if job.clientID == clientID {
			fcfs.jobs = append(fcfs.jobs[:i], fcfs.jobs[i+1:]...)
			break
		}
	}
}

// fcfs scheduler
func (fcfs *FCFS) ScheduleJobs() {
	if len(fcfs.jobs) == 0 {
		return
	}
	jobIdx := 0
	for _, miner := range fcfs.miners {
		if !miner.Busy() {
			exist := fcfs.jobs[jobIdx].AssignToMiner(miner.minerID)
			if !exist {
				jobIdx++
			}
		}
	}
}

// RunningStats  ________________________________________________________________________
// A data structure that keeps track of the running mean
// and standard deviation based on the Welford's algorithm.
// usage:
// initialize                            ->  rs := RunningStats{}
// record new job                        -> rs.StartRecording(clientID)
// record job end time and update stats  -> rs.StopRecording(clientID)
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

// ChunkQ  ______________________________________________________________________________
// A queue of chunks, the basic unit of work in this system
// when a job is created for a client, break up the work into chunks
// and sequentially enqueue into the pendingChunks within Job struct
// when a worker is available to be assigned to a chunk, deque a chunk into
// the activeChunk map within jobs, key'd by the minerID
// Job.pendingChunks.Empty() => A job has been completed.

// create with queue := &ChunkQ{}
type ChunkQ struct {
	chunks []*Chunk
}

func (q *ChunkQ) Enqueue(chunk *Chunk) {
	q.chunks = append(q.chunks, chunk)
}

func (q *ChunkQ) Dequeue() (*Chunk, bool) {
	if len(q.chunks) == 0 {
		return nil, false
	}
	chunk := q.chunks[0]
	q.chunks = q.chunks[1:] // Remove the first element
	return chunk, true
}

func (q *ChunkQ) Size() int {
	return len(q.chunks)
}

func (q *ChunkQ) Empty() bool {
	return len(q.chunks) == 0
}
