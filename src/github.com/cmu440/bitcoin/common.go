package bitcoin

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/cmu440/lsp"
)

// Chunk  _______________________________________________________________________________
// In this system, a Chunk is the smallest unit of work that can be done by a miner
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
	}
	return job
}

// assigns a job to a miner
// this involves removing a chunk from a queue
// if it exists, then moving it to the minerMap
// a payload should be immediately sent to the miner
func (job *Job) AssignToMiner(minerID int) (bool, error) {
	chunk, exist := job.pendingChunks.Dequeue() // Get the first chunk
	if !exist {
		return false, nil
	}
	job.minerMap[minerID] = chunk
	payload, _ := json.Marshal(chunk.request)
	error := job.server.Write(minerID, payload)
	// miner is lost
	if error != nil {
		return true, error
	}
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

func (job *Job) ProcessComplete() {
	result := NewResult(job.minHash, job.minNonce)
	rpayload, err := json.Marshal(result)
	// error while marshalling
	if err != nil {
		return
	}
	error := job.server.Write(job.clientID, rpayload)
	// error while sending result to client
	if error != nil {
		return
	}
}

// condition for when a job is considered to be complete
func (job *Job) Complete() bool {
	return job.pendingChunks.Empty()
}

func (job *Job) GetMinHash() (uint64, uint64) {
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

// Scheduler Interface  _________________________________________________________________
// A generic scheduler interface for load balancing algorithms.
type Scheduler interface {
	AddMiner(minerID int)
	IsMiner(id int) bool
	MinerDone(minerID int)
	RemoveMiner(minerID int)
	GetMinersJob(minerID int) (*Job, int, bool)
	AddJob(job *Job)
	RemoveJob(clientID int)
	GetJob(clientID int) (*Job, bool)
	ScheduleJobs() bool
	JobComplete() bool
}

// FCFS Scheduler  ______________________________________________________________________
// A simple baseline first-come-first-served scheduler. Requests are processed in the order
// they are recieved
type FCFS struct {
	server     lsp.Server
	miners     map[int]*Miner
	jobs       map[int]*Job
	idleMiners []int // list of idle minerIDs
}

// initialize
func NewFCFS(srv lsp.Server) *FCFS {
	return &FCFS{
		server:     srv,
		miners:     make(map[int]*Miner),
		jobs:       make(map[int]*Job),
		idleMiners: make([]int, 0),
	}
}

// add a new miner when a join message is recieved
func (fcfs *FCFS) AddMiner(minerID int) {
	miner := NewMiner(minerID)
	fcfs.miners[minerID] = miner
	fcfs.idleMiners = append(fcfs.idleMiners, minerID)
}

func (fcfs *FCFS) IsMiner(id int) bool {
	_, minerExist := fcfs.miners[id]
	return minerExist
}

func (fcfs *FCFS) MinerDone(minerID int) {
	fcfs.miners[minerID].currClientID = -1
	fcfs.idleMiners = append(fcfs.idleMiners, minerID)
}

// remove a miner when a miner is disconnected
func (fcfs *FCFS) RemoveMiner(minerID int) {
	miner := fcfs.miners[minerID]
	if miner.Busy() {
		job, _ := fcfs.GetJob(miner.currClientID)
		chunk := job.minerMap[minerID]
		// re-enqueue the chunk
		job.pendingChunks.Enqueue(chunk)
	}
	delete(fcfs.miners, minerID)
	for i, id := range fcfs.idleMiners {
		if id == minerID {
			fcfs.idleMiners = append(fcfs.idleMiners[:i], fcfs.idleMiners[i+1:]...)
			break
		}
	}
}

func (fcfs *FCFS) GetMinersJob(minerID int) (*Job, int, bool) {
	miner := fcfs.miners[minerID]
	job, exist := fcfs.GetJob(miner.currClientID)
	return job, job.clientID, exist
}

// add a job to the job list when a client request is recv'd
func (fcfs *FCFS) AddJob(job *Job) {
	fcfs.jobs[job.clientID] = job
}

func (fcfs *FCFS) GetJob(clientID int) (*Job, bool) {
	job, exist := fcfs.jobs[clientID]
	return job, exist
}

// remove a job from the job list when a job is complete
func (fcfs *FCFS) RemoveJob(clientID int) {
	delete(fcfs.jobs, clientID)
}

func (fcfs *FCFS) JobComplete() bool {
	return len(fcfs.jobs) == 0
}

// fcfs scheduler, false means nothing was scheduled
func (fcfs *FCFS) ScheduleJobs() bool {
	if len(fcfs.jobs) == 0 {
		return false
	}

	// earliest job
	jobQ := NewPQ()
	for _, job := range fcfs.jobs {
		jobQ.Insert(job)
	}

	currJob, _ := jobQ.RemoveMin()

	// for _, miner := range fcfs.miners {
	// 	if !miner.Busy() {
	// 		exist, err := currJob.AssignToMiner(miner.minerID)
	// 		// pendingChunks is empty
	// 		if !exist && err == nil {
	// 			newJob, err := jobQ.RemoveMin()
	// 			if err != nil {
	// 				return true
	// 			}
	// 			currJob = newJob
	// 		} else if err != nil {
	// 			// miner is lost
	// 			fcfs.RemoveMiner(miner.minerID)
	// 		}
	// 	}
	// }

	for len(fcfs.idleMiners) > 0 && !fcfs.JobComplete() {
		minerID := fcfs.idleMiners[0]
		exist, err := currJob.AssignToMiner(minerID)
		// pendingChunks is empty
		if !exist && err == nil {
			newJob, err := jobQ.RemoveMin()
			if err != nil {
				return true
			}
			currJob = newJob
		} else if err != nil {
			// miner is lost
			fcfs.RemoveMiner(minerID)
		}
		fcfs.idleMiners = fcfs.idleMiners[1:]
	}

	return true
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

	// prioritize based on the shortest pendingChunks
	// if the pendingChunks are equal, prioritize based on the size of the chunk
	if pq.isValidIdx(lch) && pq.q[minIdx].GetPendingChunks() > pq.q[lch].GetPendingChunks() {
		minIdx = lch
	} else if pq.isValidIdx(lch) && pq.q[minIdx].GetPendingChunks() == pq.q[lch].GetPendingChunks() && pq.q[minIdx].pendingChunks.Peek().GetSize() > pq.q[lch].pendingChunks.Peek().GetSize() {
		minIdx = lch
	}
	if pq.isValidIdx(rch) && pq.q[minIdx].GetPendingChunks() > pq.q[rch].GetPendingChunks() {
		minIdx = rch
	} else if pq.isValidIdx(rch) && pq.q[minIdx].GetPendingChunks() == pq.q[rch].GetPendingChunks() && pq.q[minIdx].pendingChunks.Peek().GetSize() > pq.q[rch].pendingChunks.Peek().GetSize() {
		minIdx = rch
	}

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

	// prioritize based on the shortest pendingChunks
	// if the pendingChunks are equal, prioritize based on the size of the chunk
	if pq.isValidIdx(p) && pq.q[maxIdx].GetPendingChunks() < pq.q[p].GetPendingChunks() {
		maxIdx = p
	} else if pq.isValidIdx(p) && pq.q[maxIdx].GetPendingChunks() == pq.q[p].GetPendingChunks() && pq.q[maxIdx].pendingChunks.Peek().GetSize() < pq.q[p].pendingChunks.Peek().GetSize() {
		maxIdx = p
	}

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
