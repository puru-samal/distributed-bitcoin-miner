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
	request *Message // The request msg to process this chunk
}

// Job  _________________________________________________________________________________
// a job is a collection of chunks. A job is created when a request is recieved from the client.
// it is then broken up into chunks and assigned to miners for processing
// when all chunks are processed, the final result is computed and then sent to client
type Job struct {
	server        lsp.Server     // server referece required to send the result to the client
	clientID      int            // client connID that submitted the request
	data          string         // the data sent in a request
	maxNonce      uint64         // the maximum nonce we're supoosed to search over
	startTime     time.Time      // update with time.Now() when a client request is recv'd
	minHash       uint64         // the current min hash
	minNonce      uint64         // the current minNonce
	pendingChunks *ChunkQ        // queue of pending chunks
	minerMap      map[int]*Chunk // map of minerID -> *Chunk => only contains chunks assigned to miners
	nChunks       int            // numbers of chunks created
}

// creates a new job upon getting a client request
// breaks the client request into Chunks that are stored in pendingChunks
// when a miner is assigned to a chunk, a chunk is moved from pendingChunk -> minerMap
// key'd by the minerID of the miner that is currently tasked with processing the chunk
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

	// break the job into chunks which will contain
	// the miner request message required to process that chunk
	// store them all in pending chunks

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

// getter for the job's clientID
func (job *Job) GetClientID() int {
	return job.clientID
}

// given a minerID, look into the minerMap to see
// if it is currently assigned to process a chunk.
// 1. return chunk, true if it exists
// 2. return nil, false it it does not
func (job *Job) GetChunkAssignedToMiner(minerID int) (*Chunk, bool) {
	chunk, exist := job.minerMap[minerID]
	return chunk, exist
}

// mark that a miner is processing a chunk: job.minerMap[minerID] = chunk
func (job *Job) AssignChunkToMiner(minerID int, chunk *Chunk) {
	job.minerMap[minerID] = chunk
}

// remove a chunk from the minerMap
func (job *Job) RemoveChunkAssignedToMiner(minerID int) {
	// 1. in RemoveMiner: it already checks if the minerID is in the minerMap
	// 2. when the miner returns a result
	// only the miner who was assigned the chunk can return result
	delete(job.minerMap, minerID)
}

// a string representation of a job for logging
func (job *Job) String() string {
	var result strings.Builder
	result.WriteString(fmt.Sprintf("[job %d] nchunks:%d, pending:%d, proc: %d, miner_map:%s\n",
		job.clientID, job.nChunks,
		len(job.pendingChunks.chunks),
		len(job.minerMap),
		minerMapString(job.minerMap)))
	return result.String()
}

// a string representation of the minerMap used
// by the job's String() method
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

// deque get a pending chunk from a job,
// 1. return chunk, true if one exists
// 2. return nil, false if it does not
func (job *Job) GetPendingChunk() (*Chunk, bool) {
	chunk, exist := job.pendingChunks.Dequeue() // Get the first chunk
	if !exist {
		return nil, false
	}
	return chunk, true
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
	// it should cease working on any requests being done on behalf of the client
	// ignore the result
	if err != nil {
		fmt.Printf("[ProcessComplete] Error while writing to the client; Ignore the result\n")
	}
}

// condition for when a job is considered to be complete
// 1. returns true if pendingChunks.Empty() && len(minerMap) == 0
// 2. returns false otherwise
func (job *Job) Complete() bool {
	return job.pendingChunks.Empty() && len(job.minerMap) == 0
}

// gets the current minimum hash and nonce
func (job *Job) GetCurrResult() (uint64, uint64) {
	return job.minHash, job.minNonce
}

// get the number of pending chunks of the job
func (job *Job) GetNumPendingChunks() int {
	return job.pendingChunks.Size()
}

// Miner  ______________________________________________________________________________
// this is a worker representation (i.e. the miner) from the server side interface
// ideally, miners are assigned work (chunks), which they work on and then return the result
type Miner struct {
	minerID      int // miner connID that joined the server
	currClientID int // the jobID of the chunk miner is currently assigned to.
}

// Create new miner
func NewMiner(minerID int) *Miner {
	worker := &Miner{
		minerID:      minerID,
		currClientID: -1,
	}
	return worker
}

// see if a miner is busy or not:
// 1. returns true if miner.currClientID != -1
// 2. returns false otherwise
func (miner *Miner) Busy() bool {
	return miner.currClientID != -1
}

// String representation of a miner
func (miner *Miner) String() string {
	result := fmt.Sprintf("[Miner %d] job:%d\n", miner.minerID, miner.currClientID)
	return result
}

// mark the miner as processing a job : miner.currClientID = jobID
func (miner *Miner) AssignMinerToJob(jobID int) {
	miner.currClientID = jobID
}

// used to mark a miner as idle and available
func (miner *Miner) Idle() {
	miner.currClientID = -1
}

// Scheduler  ______________________________________________________________________
// the scheduler manages work(chunk) allocations from jobs to (workers)miners
// our job priority strategy is as follows:
// Priority#1 -> request size (jobs.maxNonce)
// Priority#2 -> estd. response time (size of jobs.pendingChunks)
// Proirity#3 -> job start time which is recorded when a job is recv'd
// This prioritization is managed by a custom priorirty queue: JobQ (see below)

type Scheduler struct {
	server lsp.Server     // server
	miners map[int]*Miner // map of miners available key: minerID
	jobs   map[int]*Job   // map of job requests key: clientID
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
// helper functions for miner management

// create and add a new miner to internal data structures
func (scheduler *Scheduler) AddMiner(minerID int) {
	miner := NewMiner(minerID)
	scheduler.miners[minerID] = miner
}

// remove a miner when a miner is disconnected
// disconnection can be detected  on read/write
// remove any references from all internal data structures (job.minerMap, scheduler.miners... )
// 1. If the miner was busy, return the chunk it was tasked with processing, and the jobID the chunk belongs to
// 2. otherwise return nil, -1
func (scheduler *Scheduler) RemoveMiner(minerID int) (*Chunk, int) {
	miner, exist := scheduler.miners[minerID]
	var chunk *Chunk
	var chunkExist bool
	jobID := -1
	// if miner is busy, get the chuck assigned to it
	if exist && miner.Busy() {
		job, jobExist := scheduler.GetMinersJob(minerID)
		if !jobExist {
			fmt.Printf("[RemoveMiner] Job does not exist in scheduler.jobs\n")
			return nil, -1
		}
		chunk, chunkExist = job.GetChunkAssignedToMiner(minerID)
		if !chunkExist {
			fmt.Printf("[RemoveMiner] Chunk does not exist in job.minerMap\n")
		}
		job.RemoveChunkAssignedToMiner(minerID)
		if job.clientID != miner.currClientID {
			fmt.Printf("[RemoveMiner] MinerID.currClientID != Job.clientID\n")
		}
		jobID = job.clientID
	}
	if exist {
		delete(scheduler.miners, minerID)
	}
	return chunk, jobID
}

// iterate through the map and create a slice of idle miners
// having a seperate data structure would be faster but adds additional complexity
// to preserve our invariants, this reduces the overhead of data-sturcture bookkeeping
func (scheduler *Scheduler) GetIdleMiners() []*Miner {
	idleMiners := []*Miner{}
	for _, miner := range scheduler.miners {
		if !miner.Busy() {
			idleMiners = append(idleMiners, miner)
		}
	}
	return idleMiners
}

// used to mark a miner as idle and available.
func (scheduler *Scheduler) MinerIdle(minerID int) {
	miner, exist := scheduler.miners[minerID]
	if exist {
		miner.Idle()
	}
}

// checks if an id is a miner, if false => is a client
// used to diffrentiate between dropped connections
func (scheduler *Scheduler) IsMiner(id int) bool {
	_, minerExist := scheduler.miners[id]
	return minerExist
}

// Job Management __________________________________________________________________
// helper functions for job management

// add a job to the job list,
// called when a a client request is recv'd
func (scheduler *Scheduler) AddJob(job *Job) {
	scheduler.jobs[job.clientID] = job
}

// remove a job from the job list
// RemoveJob is done when:
// 1. A job is complete
// 2. A client is disconnected
func (scheduler *Scheduler) RemoveJob(clientID int) {
	delete(scheduler.jobs, clientID)
}

// get a job based on the clientID that created it
// 1. returns *Job, true if it exists
// 2. nil, false otherwise
func (scheduler *Scheduler) GetJob(clientID int) (*Job, bool) {
	job, exist := scheduler.jobs[clientID]
	return job, exist
}

// retreive the job associated with a miner
// 1. returns *Job, true if a miner was working on a job
// 2. returns false otherwise
func (scheduler *Scheduler) GetMinersJob(minerID int) (*Job, bool) {
	miner := scheduler.miners[minerID]
	job, exist := scheduler.GetJob(miner.currClientID)
	return job, exist
}

// condition for no jobs left to process: len(scheduler.jobs) == 0
func (scheduler *Scheduler) NoJobs() bool {
	return len(scheduler.jobs) == 0
}

// helper to log all jobs in the scheduler
func (scheduler *Scheduler) PrintAllJobs(logger *log.Logger) {
	logger.Printf("Jobs:\n")
	for _, job := range scheduler.jobs {
		result := job.String()
		logger.Println(result)
	}
}

// helper to job miners in the scheduler
func (scheduler *Scheduler) PrintAllMiners(logger *log.Logger) {
	logger.Printf("Miners:\n")
	for _, miner := range scheduler.miners {
		result := miner.String()
		logger.Println(result)
	}
}

// Chunk Management __________________________________________________________________
// helper functions for chunk management

// reassigns a chunk for later scheduling: job.pendingChunks.Enqueue(chunk)
// used to handle the case when a busy miner is dropped
func (scheduler *Scheduler) ReassignChunk(chunk *Chunk, jobID int) {
	job, exist := scheduler.jobs[jobID]
	if exist {
		job.pendingChunks.Enqueue(chunk)
	}
}

// the main scheduler
func (scheduler *Scheduler) ScheduleJobs(logger *log.Logger) {

	// no jobs => nothing to schedule, return
	if scheduler.NoJobs() {
		logger.Printf("[Scheduler] No Jobs\n")
		logger.Printf("[Scheduler]:\n")
		scheduler.PrintAllJobs(logger)
		scheduler.PrintAllMiners(logger)
		return
	}

	idleMiners := scheduler.GetIdleMiners()
	// no idle miners => nothing to schedule, return
	if len(idleMiners) == 0 {
		logger.Printf("No idleMiners\n")
		logger.Printf("[Scheduler]:\n")
		scheduler.PrintAllJobs(logger)
		scheduler.PrintAllMiners(logger)
		return
	}

	// insert existing jobs into the custom-PQ
	// jobs should be sorted acc. to out prioritization
	// strategy
	jobQ := NewJQ()
	for _, job := range scheduler.jobs {
		jobQ.Insert(job)
	}

	// while there are jobs to process and miners that are available
	// 1. get a chunk from a job,
	//    1. if chunk exists
	//    	1. send a request to that miner
	//       	1. if a miner was lost, remove miner, restore invariants
	//       	2. else, change state in *Job, *Miner, *Scheduler struct
	//             to indicate that the miner is currently working on that chunk
	//      2. if chunk does not exist, move on to the next job
	// 2. if the current miner is busy and there are more miners left
	//    go on to the next miner and repeat

	currJob, _ := jobQ.RemoveMin()
	for len(idleMiners) > 0 && jobQ.Size() >= 0 {

		logger.Printf("[idleMiner]: %s\n", idleMiners[0].String())

		chunk, chunkExists := currJob.GetPendingChunk()
		logger.Printf("[ChunkExists]: %v\n", chunkExists)
		if chunkExists {
			logger.Printf("[Chunk] %s\n", chunk.request)
			payload, _ := json.Marshal(chunk.request)
			minerWriteErr := currJob.server.Write(idleMiners[0].minerID, payload)
			// miner is lost
			if minerWriteErr != nil {
				scheduler.RemoveMiner(idleMiners[0].minerID)
				scheduler.ReassignChunk(chunk, currJob.clientID)
				idleMiners = idleMiners[1:]
			} else {
				currJob.AssignChunkToMiner(idleMiners[0].minerID, chunk)
				idleMiners[0].AssignMinerToJob(currJob.clientID)
				logger.Printf("[Chunk] Assigned -> %s\n", idleMiners[0].String())
			}
		} else {
			logger.Printf("-> next job\n")
			nextJob, err := jobQ.RemoveMin()
			if err != nil {
				logger.Printf("[Scheduler]: Job has no pending chunks, theres no next job")
				logger.Printf("[Scheduler]: Nothing to schedule!")
				break
			}
			currJob = nextJob
		}
		if len(idleMiners) > 0 && idleMiners[0].Busy() {
			logger.Printf("-> next idle miner\n")
			idleMiners = idleMiners[1:]
		}
	}

	logger.Printf("[Scheduler]:\n")
	scheduler.PrintAllJobs(logger)
	scheduler.PrintAllMiners(logger)
}

// Data Structures ______________________________________________________________________
// RunningStats  ________________________________________________________________________
// A data structure that keeps track of the running mean and standard deviation based on the Welford's algorithm.
// We created this data structure as a means of tracking the performance of diffrent load balancing strategies.
// The goal is to jointly optimize efficiency and fairness by minimizing mean response time and variance.
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

// create a new running stats
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

// create with queue := &ChunkQ{}
type ChunkQ struct {
	chunks []*Chunk
}

// enqueue a chunk into the queue
func (q *ChunkQ) Enqueue(chunk *Chunk) {
	q.chunks = append(q.chunks, chunk)
}

// dequeue a chunk from the queue
func (q *ChunkQ) Dequeue() (*Chunk, bool) {
	if len(q.chunks) == 0 {
		return nil, false
	}
	chunk := q.chunks[0]
	q.chunks = q.chunks[1:] // Remove the first element
	return chunk, true
}

// get the size of the queue
func (q *ChunkQ) Size() int {
	return len(q.chunks)
}

// check if the queue is empty
func (q *ChunkQ) Empty() bool {
	return len(q.chunks) == 0
}

// Custom Job PQ  _______________________________________________________________________
// A custom priority queue implementation
// Uses a custom comparator based on oue load balancing strategy
// to maintain the min-heap invariants.

type JobQ struct {
	q []*Job
}

/** USER API **/

// create a new priority queue
func NewJQ() *JobQ {
	newQueue := &JobQ{
		q: make([]*Job, 0),
	}
	return newQueue
}

// insert a new message into the priority queue
// and maintain the min heap property
func (pq *JobQ) Insert(elem *Job) {
	pq.q = append(pq.q, elem)
	pq.minHeapifyUp(len(pq.q) - 1)
}

// get the message with the minimum sequence number
// if the priority queue is empty, return an error
func (pq *JobQ) GetMin() (*Job, error) {
	if len(pq.q) == 0 {
		return nil, fmt.Errorf("priority queue is empty")
	}

	return pq.q[0], nil
}

// remove the message with the minimum sequence number
// and maintain the minheap property
func (pq *JobQ) RemoveMin() (*Job, error) {
	min, err := pq.GetMin()

	if err != nil {
		return nil, err
	}

	pq.q[0] = pq.q[len(pq.q)-1]
	pq.q = pq.q[:len(pq.q)-1]
	pq.minHeapifyDown(0)
	return min, nil
}

// return the size of the priority queue
func (pq *JobQ) Size() int {
	return len(pq.q)
}

/** internal helpers **/

// check if the index is valid in the priority queue
func (pq *JobQ) isValidIdx(idx int) bool {
	return (0 <= idx) && (idx < len(pq.q))
}

// get the parent of the current index
func (pq *JobQ) parent(idx int) int {
	newIdx := (idx - 1) >> 1
	return newIdx
}

// get the left child of the current index
func (pq *JobQ) leftChild(idx int) int {
	newIdx := (idx << 1) + 1
	return newIdx
}

// get the right child of the current index
func (pq *JobQ) rightChild(idx int) int {
	newIdx := (idx << 1) + 2
	return newIdx
}

// maintain the min heap property by moving the element down
// used when removing the minimum element
func (pq *JobQ) minHeapifyDown(idx int) {
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

			if pq.q[minIdx].GetNumPendingChunks() > pq.q[lch].GetNumPendingChunks() {
				// Priority2: #pendingChunks
				minIdx = lch
			} else if pq.q[minIdx].GetNumPendingChunks() == pq.q[lch].GetNumPendingChunks() {
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
			if pq.q[minIdx].GetNumPendingChunks() > pq.q[rch].GetNumPendingChunks() {
				// Priority2: #pendingChunks
				minIdx = rch
			} else if pq.q[minIdx].GetNumPendingChunks() == pq.q[rch].GetNumPendingChunks() {
				if pq.q[minIdx].startTime.Compare(pq.q[rch].startTime) == 1 {
					// Priority3: jobStartTime
					minIdx = rch
				}
			}
		}
	}

	if minIdx != idx {
		tmp := pq.q[idx]
		pq.q[idx] = pq.q[minIdx]
		pq.q[minIdx] = tmp
		pq.minHeapifyDown(minIdx)
	}
}

// maintain the min heap property by moving the element up
// used when inserting a new element
func (pq *JobQ) minHeapifyUp(idx int) {
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
			if pq.q[maxIdx].GetNumPendingChunks() < pq.q[p].GetNumPendingChunks() {
				// Priority2: #pendingChunks
				maxIdx = p
			} else if pq.q[maxIdx].GetNumPendingChunks() == pq.q[p].GetNumPendingChunks() {
				if pq.q[maxIdx].startTime.Compare(pq.q[p].startTime) == -1 {
					// Priority3: jobStartTime
					maxIdx = p
				}
			}
		}
	}

	if maxIdx != idx {
		tmp := pq.q[idx]
		pq.q[idx] = pq.q[maxIdx]
		pq.q[maxIdx] = tmp
		pq.minHeapifyUp(maxIdx)
	}
}
