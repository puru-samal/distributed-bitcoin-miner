package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

// server
type server struct {
	lspServer lsp.Server
	rStats    bitcoin.RunningStats
	scheduler bitcoin.Scheduler

	// Channel
	configureChan chan *config
}

type config struct {
	connID       int
	message      *bitcoin.Message
	disconnected bool
}

func startServer(port int) (*server, error) {
	// TODO: implement this!
	// start new server
	lspServer, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}
	server := &server{
		lspServer:     lspServer,
		rStats:        bitcoin.NewRS(),
		scheduler:     *bitcoin.NewScheduler(lspServer),
		configureChan: make(chan *config),
	}
	return server, nil
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "serverLog.txt"
		flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()

	// TODO: implement this!
	go srv.reader()
	srv.processor()
}

func (srv *server) reader() {

	for {
		connID, payload, err := srv.lspServer.Read()
		disconnected := false
		if err != nil {
			disconnected = true
		}
		// handle different type of message
		var message bitcoin.Message
		json.Unmarshal(payload, &message)
		config := &config{
			connID:       connID,
			message:      &message,
			disconnected: disconnected,
		}
		srv.configureChan <- config
	}
}

/*
Cases of Removing/Adding/Referening
1. Disconnected
	*** schedulers.miners map is a map of miners ***
	- [srv.scheduler.IsMiner] If Miner: Remove the miner and Reassign
		- [srv.scheduler.RemoveMiner] Remove Miner and returns the chunk and the jobID(currClientID) of the miner
			- Get the miner from scheduler.miners (config.connID is the minerID)
			- If the miner exists in the scheduler.miners and is Busy() (has a currClientID that is not -1)
				- [scheduler.GetMinersJob] Get Miner's Job, ClientID, Exist (If the minerID exists in the scheduler.jobs)
					- [scheduler.GetJob] Get Job of the miner's currClientID from scheduler.jobs
				- [job.GetChunkAssignedToMiner] Get the chunk assigned to the miner from the job.minerMap
				- [job.RemoveChunkAssignedToMiner] Remove the minerID from the job.minerMap
			- If the miner exists in the scheduler.miners and is Idle() (has a currClientID that is -1)
		- [srv.scheduler.ReassignChunk] Reassign Chunk
	- [srv.scheduler.RemoveJob] If Client: Remove the client from scheduler.jobs (PASS)
2. Join
	- [srv.scheduler.AddMiner] Add Miner (PASS)
	- [srv.scheduler.ScheduleJobs] Schedule Jobs
		- [scheduler.GetIdleMiners] Get Idle Miners
		- [NewPQ] Create a new Priority Queue of Jobs from the scheduler.Jobs
		- [jobQ.RemoveMin] Remove the first elements from the Priority Queue
		- If the idleMiner exists and the scheduler.jobs is not empty
			- pop the first idleMiner and assign it to be minerID
			- [currJob.AssignChunkToMine] Assign the first chunk of the job's pendingChunks to the miner
			- If there is a chunkExists and the miner did not Drop
				- assign the minerID's currClientID to the job's clientID
			- If there is no chunkExists and the miner did not Drop
				- [jobQ.RemoveMin] Remove the next element from the Priority Queue
				- Update the current job to be the next element
			- If the miner Drop
				- [scheduler.RemoveMiner] Remove the minerID from the scheduler.miners
			- If the miner is properly assigned a job
				- remove it from the idleMiners
3. Request
	- [bitcoin.NewJob] Make New Job (PASS)
	- [srv.scheduler.AddJob] Add Job (PASS)
	- [srv.scheduler.ScheduleJobs] Schedule Jobs
4. Result
	- [srv.scheduler.GetMinersJob] Get Miner's Job, ClientID, Exist If the minerID exists in the scheduler.jobs)
		- [scheduler.GetJob] Get Job of the miner's currClientID from scheduler.jobs
	- If the job exists in the scheduler.jobs
		- [job.RemoveChunkAssignedToMiner] Remove the minerID from the job.minerMap
		- [job.ProcessResult] Comparison of the current minHash and minNonce with the miner's result (PASS)
		- [job.Complete] If Complete: Job's pendingChunks and the minerMap is empty (PASS)
			- [job.ProcessComplete] send the result of the job to the client
			- [srv.scheduler.RemoveJob] Remove the clientID from the scheduler.jobs (PASS)
	- [srv.scheduler.MinerIdle] Mark the miner's currClientID as -1; idle
	- [srv.scheduler.ScheduleJobs] Schedule Jobs
*/

func (srv *server) processor() {
	for {
		config := <-srv.configureChan
		if config.disconnected {

			// if server loses contact with a miner
			// reassign any job that was assigned to the miner
			// to a different worker
			// If there are no available miners left,
			// the server should wait for a new miner to join before reassigning the old miner’s job.
			if srv.scheduler.IsMiner(config.connID) {
				LOGF.Printf("[Server] Miner[id %d] Disconnect\n", config.connID)
				chunk, jobID := srv.scheduler.RemoveMiner(config.connID)
				if chunk != nil {
					srv.scheduler.ReassignChunk(chunk, jobID)
				}
				srv.scheduler.ScheduleJobs(LOGF)

			} else {

				// if server loses contact with a client
				// cease working on any requests being done on behalf of the client
				// wait for the miner's result and ignore its result
				LOGF.Printf("[Server] Client[id %d] Disconnect]\n", config.connID)
				srv.scheduler.RemoveJob(config.connID)
			}

		} else if config.message.Type == bitcoin.Join {

			// Add the miner to the scheduler
			// Miner will be marked as idle

			LOGF.Printf("[Server] Miner[id %d] Joined\n", config.connID)
			srv.scheduler.AddMiner(config.connID)
			srv.scheduler.ScheduleJobs(LOGF)
			// TODO: LOG ALL SCHEDULER DATA-STRUCTURES HERE

		} else if config.message.Type == bitcoin.Request {

			// add job will add a job to scheduler.jobs
			// start recording to calculate metrics

			LOGF.Printf("[Server] Client[id %d] Request: %s\n", config.connID, config.message.Data)
			srv.rStats.StartRecording(config.connID)
			job := bitcoin.NewJob(srv.lspServer, config.connID, config.message.Data, config.message, bitcoin.ChunkSize)
			srv.scheduler.AddJob(job)
			srv.scheduler.ScheduleJobs(LOGF)
			// TODO: PRINT ALL SCHEDULER DATA-STRUCTURES HERE

		} else if config.message.Type == bitcoin.Result {

			LOGF.Printf("[Server] Miner[id %d] result:%s\n", config.connID, config.message)
			minerID := config.connID

			// if a job exists
			// if a client disconnects, a job should have been removed,
			// in this case exist will return false and the result will be ignored
			job, clientID, exist := srv.scheduler.GetMinersJob(minerID)
			if exist {
				job.RemoveChunkAssignedToMiner(minerID)
				// update client's minNonce, minHash
				job.ProcessResult(minerID, config.message)
				currMinHash, currMinNonce := job.GetCurrResult()
				LOGF.Printf("[Server] Client[id %d] result: currentMinHash:%d currentMinNonce:%d\n", clientID, currMinHash, currMinNonce)

				// If a job is complete, send the result back to the client
				if job.Complete() {
					LOGF.Printf("[Server] Client[id %d] sending final result minHash:%d minNonce:%d\n", clientID, currMinHash, currMinNonce)
					srv.rStats.StopRecording(clientID)
					job.ProcessComplete()
					srv.scheduler.RemoveJob(clientID)
					mean, dev := srv.rStats.GetStats()
					LOGF.Printf("[Final Stat]: %f %f\n", mean, dev)
				}
			}
			srv.scheduler.MinerIdle(minerID) // Mark the Miner as Idle
			srv.scheduler.ScheduleJobs(LOGF)
		}
	}
}
