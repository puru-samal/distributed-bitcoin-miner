package bitcoin

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/cmu440/lsp"
)

// Chunk  _______________________________________________________________________________
// In this system, a Chunk is the smallest unit of work that can be done by a miner

const ChunkSize = 10000

type Chunk struct {
	request *Message // The job request msg for this chunk
}

// inclusive lower and upper bounds
func (c *Chunk) GetSize() uint64 {
	return c.request.Upper - c.request.Lower + 1
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
	minerMap      map[int]*Chunk // map of minerID -> *Chunk => only contains chunks assigned to miners
	nChunks       int            // numbers of chunks created
}

// creates a new job upon getting a client request
// breaks the client request into Chunks that are stored in pendingChunks
// invariant: len(pendingCunks) + len(minerMap) = nChunks
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
		nChunks:       0,
	}

	for i := uint64(0); i < job.maxNonce; i += chunkSize {
		// inclusive lower and upper bounds
		lower := i
		upper := i + chunkSize - 1
		if upper > job.maxNonce {
			upper = job.maxNonce
		}
		chunk := &Chunk{
			request: NewRequest(job.data, lower, upper),
		}
		job.pendingChunks.Enqueue(chunk)
		job.nChunks++
	}
	return job
}

func (job *Job) GetChunkAssignedToMiner(minerID int) (*Chunk, bool) {
	chunk, exist := job.minerMap[minerID]
	return chunk, exist
}

func (job *Job) RemoveChunkAssignedToMiner(minerID int) {
	// [+Assumption] minerID was working on the chunk of the job
	// 1) in RemoveMiner: it already checks if the minerID is in the minerMap
	// 2) when the miner returns a result
	// only the miner who was assigned the chunk can return result
	delete(job.minerMap, minerID)
}

func (job *Job) String() string {
	var result strings.Builder
	result.WriteString(fmt.Sprintf("[job %d] nchunks:%d, pending:%d, proc: %d, miner_map:%s\n",
		job.clientID, job.nChunks,
		len(job.pendingChunks.chunks),
		len(job.minerMap),
		minerMapString(job.minerMap)))
	return result.String()
}

func minerMapString(m map[int]*Chunk) string {
	var sb strings.Builder
	sb.WriteString("[MinerMap]:{")
	first := true
	for key, value := range m {
		if !first {
			sb.WriteString(", ")
		}
		// Append key-value pairs to the string builder
		sb.WriteString(fmt.Sprintf("miner:%d: chunk:%s", key, value.request.String()))
		first = false
	}

	sb.WriteString("}")
	return sb.String()
}

// assigns a job to a miner
// this involves removing a chunk from a queue
// if it exists, then moving it to the minerMap
// a payload should be immediately sent to the miner
// returns true if a chunk exists, false if not
// returns an nil if a request was sent to miner
// returns a non-nil error if seding failed for potential reassignment
func (job *Job) AssignChunkToMiner(minerID int) (bool, error) {
	chunk, exist := job.pendingChunks.Dequeue() // Get the first chunk
	if !exist {
		return false, nil
	}
	payload, _ := json.Marshal(chunk.request)
	error := job.server.Write(minerID, payload)
	// miner is lost
	if error != nil {
		job.pendingChunks.Enqueue(chunk)
		return true, error
	}
	job.minerMap[minerID] = chunk
	return true, nil
}

// updates minHash and minNonce when a result is recv'd from a miner
// and then removes the chunk from the minerMap
// server should immediately check if a job is compete, if yes then send result to client
func (job *Job) ProcessResult(minerID int, minerResult *Message) {
	if job.minHash > minerResult.Hash {
		job.minHash = minerResult.Hash
		job.minNonce = minerResult.Nonce
	}
}

// Called when a job is complete
// server sends the result to client
func (job *Job) ProcessComplete() {
	result := NewResult(job.minHash, job.minNonce)
	rpayload, _ := json.Marshal(result)
	err := job.server.Write(job.clientID, rpayload)
	//[+Assumption] if error occurs while writing to the client
	// it should cease working on any requests being done on behalf of the client
	// ignore the result
	if err != nil {
		fmt.Printf("[ProcessComplete] Error while writing to the client; Ignore the result\n")
	}
}

// condition for when a job is considered to be complete
// when all pendingChunks.Empty() && len(minerMap) == 0
func (job *Job) Complete() bool {
	return job.pendingChunks.Empty() && len(job.minerMap) == 0
}

// gets the current minimum hash and nonce
func (job *Job) GetCurrResult() (uint64, uint64) {
	return job.minHash, job.minNonce
}

// for SRTF
func (job *Job) GetPendingChunks() int {
	return job.pendingChunks.Size()
}

// Miner  ______________________________________________________________________________
type Miner struct {
	minerID      int // miner connID that joined the server
	currClientID int // current job worker is assigned to. Note: Miner is only processing a chunk.
}

// Create new miner
func NewMiner(minerID int) *Miner {
	worker := &Miner{
		minerID:      minerID,
		currClientID: -1,
	}
	return worker
}

func (w *Miner) Busy() bool {
	return w.currClientID != -1
}

func (miner *Miner) String() string {
	result := fmt.Sprintf("[Miner %d] job:%d\n", miner.minerID, miner.currClientID)
	return result
}

// Scheduler  ______________________________________________________________________

type Scheduler struct {
	server lsp.Server
	miners map[int]*Miner // key: minerID
	jobs   map[int]*Job   // key: clientID
}

// create a new scheduler
func NewScheduler(srv lsp.Server) *Scheduler {
	return &Scheduler{
		server: srv,
		miners: make(map[int]*Miner),
		jobs:   make(map[int]*Job),
	}
}

// Miner Management ________________________________________________________________

// create and add a new miner to internal data structures
// called when when a join message is recieved from miner
func (scheduler *Scheduler) AddMiner(minerID int) {
	miner := NewMiner(minerID)
	scheduler.miners[minerID] = miner
}

// remove a miner when a miner is disconnected
// disconnection can happen on read/writes!
// remove any references from all internal data structures (job.minerMap, scheduler.miners, )
// if it was busy, return the chunk it was tasked with processing, and the jobID the chunk belongs to
func (scheduler *Scheduler) RemoveMiner(minerID int) (*Chunk, int) {
	miner, exist := scheduler.miners[minerID]
	var chunk *Chunk
	var chunkExist bool
	jobID := -1
	// if miner is busy, get the chuck assigned to it
	if exist && miner.Busy() {
		job, _, jobExist := scheduler.GetMinersJob(minerID)
		// [+Assumption] job exists in scheduler.jobs
		if !jobExist {
			fmt.Printf("[RemoveMiner] Job does not exist in scheduler.jobs\n")
			return nil, -1
		}
		// [+Assumption] chunk exists in job.minerMap

		chunk, chunkExist = job.GetChunkAssignedToMiner(minerID)
		if !chunkExist {
			fmt.Printf("[RemoveMiner] Chunk does not exist in job.minerMap\n")
		}
		job.RemoveChunkAssignedToMiner(minerID)
		// assert minerID.currClientID == job.clientID
		if job.clientID != miner.currClientID {
			fmt.Printf("[RemoveMiner] MinerID.currClientID != Job.clientID\n")
		}
		jobID = job.clientID
	}
	// [+Assumption] minerID exists in scheduler.miners
	if exist {
		delete(scheduler.miners, minerID)
	}
	return chunk, jobID
}

// iterate through the map and create a slice of idle miners
// might be expensive, but reduces the overhead of data-sturcture bookkeeping
func (scheduler *Scheduler) GetIdleMiners() []*Miner {
	idleMiners := []*Miner{}
	for _, miner := range scheduler.miners {
		if !miner.Busy() {
			idleMiners = append(idleMiners, miner)
		}
	}
	return idleMiners
}

// used to mark a miner as idle and available
func (scheduler *Scheduler) MinerIdle(minerID int) {
	scheduler.miners[minerID].currClientID = -1
}

// checks if an id is a miner, if false => is a client
// used to diffrentiate between dropped connections
func (scheduler *Scheduler) IsMiner(id int) bool {
	_, minerExist := scheduler.miners[id]
	return minerExist
}

// Job Management __________________________________________________________________

// add a job to the job list,
// called when a a client request is recv'd
func (scheduler *Scheduler) AddJob(job *Job) {
	scheduler.jobs[job.clientID] = job
}

// remove a job from the job list
// called when a job is complete
func (scheduler *Scheduler) RemoveJob(clientID int) {
	// [+Assumption] clientID exists in scheduler.jobs
	// RemoveJob is done when:
	// 1. A job is complete
	// 2. A client is disconnected
	delete(scheduler.jobs, clientID)
}

// get a job based on the clientID that created it
// also returns a bool, true if it exists, false if not
func (scheduler *Scheduler) GetJob(clientID int) (*Job, bool) {
	job, exist := scheduler.jobs[clientID]
	return job, exist
}

// retreive the job associated with a miner
// also returns the job, the jobID(clientID) a bool indicating if it is valid
func (scheduler *Scheduler) GetMinersJob(minerID int) (*Job, int, bool) {
	// [+Assumption] minerID exists in scheduler.miners and is Busy()
	miner := scheduler.miners[minerID]
	job, exist := scheduler.GetJob(miner.currClientID)
	return job, miner.currClientID, exist
}

// no jobs to process
func (scheduler *Scheduler) NoJobs() bool {
	return len(scheduler.jobs) == 0
}

func (scheduler *Scheduler) PrintAllJobs(logger *log.Logger) {
	logger.Printf("Jobs:\n")
	for _, job := range scheduler.jobs {
		result := job.String()
		logger.Println(result)
	}
}

func (scheduler *Scheduler) PrintAllMiners(logger *log.Logger) {
	logger.Printf("Miners:\n")
	for _, miner := range scheduler.miners {
		result := miner.String()
		logger.Println(result)
	}
}

// Chunk Management __________________________________________________________________

// attempts to reassign a chunk to a miner
// returns true if chunk was successfully reassigned
func (scheduler *Scheduler) ReassignChunk(chunk *Chunk, jobID int) {

	job := scheduler.jobs[jobID]
	job.pendingChunks.Enqueue(chunk)
}

// !!!!!!!!!!!!!!!!!!!!!!! [UNVERIFIED]  !!!!!!!!!!!!!!!!!!!!!!!!!!!

// Scheduler scheduler, false means nothing was scheduled
func (scheduler *Scheduler) ScheduleJobs(logger *log.Logger) {

	idleMiners := scheduler.GetIdleMiners()
	// nothing to schedule, return
	if scheduler.NoJobs() || len(idleMiners) == 0 {
		logger.Printf("[Scheduler] No Jobs || No idleMiners\n")
		logger.Printf("[Scheduler]:\n")
		scheduler.PrintAllJobs(logger)
		scheduler.PrintAllMiners(logger)
		return
	}

	// earliest job based on remaining processing time
	jobQ := NewPQ()
	for _, job := range scheduler.jobs {
		jobQ.Insert(job)
	}
	// [+Assumption] there's always a job in the jobQ
	// RemoveMin already checks for the empty condition
	currJob, _ := jobQ.RemoveMin()
	// [+Assumption] scheduler.jobs and jobQ do not need to be in sync
	for len(idleMiners) > 0 && !scheduler.NoJobs() {
		minerID := idleMiners[0].minerID
		chunkExist, minerDropped := currJob.AssignChunkToMiner(minerID)

		if chunkExist && minerDropped == nil {
			scheduler.miners[minerID].currClientID = currJob.clientID
		}
		// pendingChunks is empty (but might be in process; there are some chunks in minerMap)
		if !chunkExist && minerDropped == nil {
			newJob, err := jobQ.RemoveMin()
			if err != nil {
				logger.Printf("[Scheduler] Miner available but no more jobs to schedule \n")
				logger.Printf("[Scheduler]:\n")
				scheduler.PrintAllJobs(logger)
				scheduler.PrintAllMiners(logger)
				return
			}
			currJob = newJob
		}
		// miner has been lost: remove
		if minerDropped != nil {
			// miner is lost
			scheduler.RemoveMiner(minerID)
		}

		if idleMiners[0].Busy() {
			idleMiners = idleMiners[1:]
		}
	}

	logger.Printf("[Scheduler]:\n")
	scheduler.PrintAllJobs(logger)
	scheduler.PrintAllMiners(logger)
}

// Data Structures ______________________________________________________________________
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

func NewRS() RunningStats {
	rs := RunningStats{
		n:             0,
		mean:          0.0,
		M2:            0.0,
		activeRecords: make(map[int]time.Time),
	}
	return rs
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

func (q *ChunkQ) Peek() *Chunk {
	return q.chunks[0]
}

// Time-Based PQ  _______________________________________________________________________
// list of messages inside the priority queue
type jobTimeQ struct {
	q []*Job
}

/** API **/

// create a new priority queue
func NewPQ() *jobTimeQ {
	newQueue := &jobTimeQ{
		q: make([]*Job, 0),
	}
	return newQueue
}

// insert a new message into the priority queue
// and maintain the min heap property
func (pq *jobTimeQ) Insert(elem *Job) {
	pq.q = append(pq.q, elem)
	pq.minHeapifyUp(len(pq.q) - 1)
}

// get the message with the minimum sequence number
// if the priority queue is empty, return an error
func (pq *jobTimeQ) GetMin() (*Job, error) {
	if len(pq.q) == 0 {
		return nil, fmt.Errorf("priority queue is empty")
	}

	return pq.q[0], nil
}

// remove the message with the minimum sequence number
// and maintain the minheap property
func (pq *jobTimeQ) RemoveMin() (*Job, error) {
	min, err := pq.GetMin()

	if err != nil {
		return nil, err
	}

	pq.q[0] = pq.q[len(pq.q)-1]
	pq.q = pq.q[:len(pq.q)-1]
	pq.minHeapifyDown(0)
	return min, nil
}

// check if the priority queue is empty
func (pq *jobTimeQ) Empty() bool {
	return len(pq.q) == 0
}

// return the size of the priority queue
func (pq *jobTimeQ) Size() int {
	return len(pq.q)
}

/** internal helpers **/

// check if the index is valid in the priority queue
func (pq *jobTimeQ) isValidIdx(idx int) bool {
	return (0 <= idx) && (idx < len(pq.q))
}

// get the parent of the current index
func (pq *jobTimeQ) parent(idx int) int {
	newIdx := (idx - 1) >> 1
	return newIdx
}

// get the left child of the current index
func (pq *jobTimeQ) leftChild(idx int) int {
	newIdx := (idx << 1) + 1
	return newIdx
}

// get the right child of the current index
func (pq *jobTimeQ) rightChild(idx int) int {
	newIdx := (idx << 1) + 2
	return newIdx
}

// maintain the min heap property by moving the element down
// used when removing the minimum element
func (pq *jobTimeQ) minHeapifyDown(idx int) {
	if !pq.isValidIdx(idx) {
		return
	}

	lch := pq.leftChild(idx)
	rch := pq.rightChild(idx)
	minIdx := idx

	if pq.isValidIdx(lch) {
		if pq.q[minIdx].maxNonce > pq.q[lch].maxNonce {
			// Priority1: maxNonce
			minIdx = lch
		} else if pq.q[minIdx].maxNonce == pq.q[lch].maxNonce {
			if pq.q[minIdx].GetPendingChunks() > pq.q[lch].GetPendingChunks() {
				// Priority2: #pendingChunks
				minIdx = lch
			} else if pq.q[minIdx].GetPendingChunks() == pq.q[lch].GetPendingChunks() {
				if pq.q[minIdx].startTime.Compare(pq.q[lch].startTime) == 1 {
					// Priority3: jobStartTime
					minIdx = lch
				}
			}
		}
	}

	if pq.isValidIdx(rch) {
		if pq.q[minIdx].maxNonce > pq.q[rch].maxNonce {
			// Priority1: maxNonce
			minIdx = rch
		} else if pq.q[minIdx].maxNonce == pq.q[rch].maxNonce {
			if pq.q[minIdx].GetPendingChunks() > pq.q[rch].GetPendingChunks() {
				// Priority2: #pendingChunks
				minIdx = rch
			} else if pq.q[minIdx].GetPendingChunks() == pq.q[rch].GetPendingChunks() {
				if pq.q[minIdx].startTime.Compare(pq.q[rch].startTime) == 1 {
					// Priority3: jobStartTime
					minIdx = rch
				}
			}
		}
	}

	/* if pq.isValidIdx(lch) && pq.q[minIdx].GetPendingChunks() > pq.q[lch].GetPendingChunks() {
		minIdx = lch
	} else if pq.isValidIdx(lch) && pq.q[minIdx].GetPendingChunks() == pq.q[lch].GetPendingChunks() && pq.q[minIdx].startTime.Compare(pq.q[lch].startTime) == 1 {
		minIdx = lch
	}
	if pq.isValidIdx(rch) && pq.q[minIdx].GetPendingChunks() > pq.q[rch].GetPendingChunks() {
		minIdx = rch
	} else if pq.isValidIdx(rch) && pq.q[minIdx].GetPendingChunks() == pq.q[rch].GetPendingChunks() && pq.q[minIdx].startTime.Compare(pq.q[rch].startTime) == 1 {
		minIdx = rch
	} */

	// prioritize based on the earliest startTime
	// if pq.isValidIdx(lch) && pq.q[minIdx].startTime.Compare(pq.q[lch].startTime) == 1 {
	// 	minIdx = lch
	// }
	// if pq.isValidIdx(rch) && pq.q[minIdx].startTime.Compare(pq.q[rch].startTime) == 1 {
	// 	minIdx = rch
	// }

	if minIdx != idx {
		tmp := pq.q[idx]
		pq.q[idx] = pq.q[minIdx]
		pq.q[minIdx] = tmp
		pq.minHeapifyDown(minIdx)
	}
}

// maintain the min heap property by moving the element up
// used when inserting a new element
func (pq *jobTimeQ) minHeapifyUp(idx int) {
	if !pq.isValidIdx(idx) {
		return
	}
	p := pq.parent(idx)
	maxIdx := idx

	if pq.isValidIdx(p) {
		if pq.q[maxIdx].maxNonce < pq.q[p].maxNonce {
			// Priority1: maxNonce
			maxIdx = p
		} else if pq.q[maxIdx].maxNonce == pq.q[p].maxNonce {
			if pq.q[maxIdx].GetPendingChunks() < pq.q[p].GetPendingChunks() {
				// Priority2: #pendingChunks
				maxIdx = p
			} else if pq.q[maxIdx].GetPendingChunks() == pq.q[p].GetPendingChunks() {
				if pq.q[maxIdx].startTime.Compare(pq.q[p].startTime) == -1 {
					// Priority3: jobStartTime
					maxIdx = p
				}
			}
		}
	}

	// prioritize based on the shortest pendingChunks
	// if the pendingChunks are equal, prioritize based on the size of the chunk
	/* if pq.isValidIdx(p) && pq.q[maxIdx].GetPendingChunks() < pq.q[p].GetPendingChunks() {
		maxIdx = p
	} else if pq.isValidIdx(p) && pq.q[maxIdx].GetPendingChunks() == pq.q[p].GetPendingChunks() && pq.q[maxIdx].startTime.Compare(pq.q[p].startTime) == -1 {
		maxIdx = p
	} */

	// prioritize based on the earliest startTime
	// if pq.isValidIdx(p) && pq.q[maxIdx].startTime.Compare(pq.q[p].startTime) == -1 {
	// 	maxIdx = p
	// }

	if maxIdx != idx {
		tmp := pq.q[idx]
		pq.q[idx] = pq.q[maxIdx]
		pq.q[maxIdx] = tmp
		pq.minHeapifyUp(maxIdx)
	}
}
