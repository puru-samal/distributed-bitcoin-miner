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

// LOAD BALANCING STRATEGY
// We use a variable-priority strategy to load balance
// All of this is managed by a custom Priority Queue
// data structure -> JobQ (see common.go)
// We ourder of priority is as follows :
// 1. Jobs that have a lower nonce
// 2. Jobs that have less pending chunks to be processed;
//    the reason for this is that the number of chunks a
//    job has should correlate with the response time of
//    job, to follow the SRTF strategy to minimize our
//    response time. A special case is when there are
//    no pending chunks. Those jobs are not scheduled.
// 3. Jobs that were recieved earlier;
//   this is to ensure that jobs with identical requests are processed
//	 in the order they were received
// After prioritizing the jobs, we assign the job to the idle miner.
// If there are no idle miners, we wait for a miner to join before reassigning the job.

// server
type server struct {
	lspServer lsp.Server
	rStats    bitcoin.RunningStats
	scheduler bitcoin.Scheduler

	// Channel
	configureChan chan *config
}

// config for determining the type of message and connection
type config struct {
	connID       int
	message      *bitcoin.Message
	disconnected bool
}

// starts a server and returns a pointer to the server
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

// variables for logging
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

	go srv.reader()
	srv.processor()
}

// reads messages from the lsp server
// and sends the message to the processor
// through a configuration channel (configureChan)
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

// reads the configuration channel and processes the message
// based on the type of message
// If the message is
// 1. Disconnected: Remove the miner/client from the scheduler
// 1(a). If Miner is disconnected and it had process to execute: Reassign the job to a different worker
// 1(b). If Client is disconnected: Cease working on any requests being done on behalf of the client
// 2. Join: Add the miner to the scheduler
// 3. Request: Add the job to the scheduler
// 4. Result: Process the result from the miner
// 4(a). If the job is complete, send the result back to the client
func (srv *server) processor() {
	for {
		config := <-srv.configureChan
		if config.disconnected {
			// MESSAGE TYPE: DISCONNECTED
			// if server loses contact with a miner
			// put the chunk the miner was working in back in the job
			// rescheudule
			// If there are no available miners left,
			// the server will wait for a new miner to join before reassigning the old miner’s job.
			if srv.scheduler.IsMiner(config.connID) {
				LOGF.Printf("[Server] Miner[id %d] Disconnect\n", config.connID)
				chunk, jobID := srv.scheduler.RemoveMiner(config.connID)
				if chunk != nil {
					srv.scheduler.ReassignChunk(chunk, jobID)
				}
				srv.scheduler.ScheduleJobs(LOGF)
				// check if the minerID is not in the minerMap and scheduler.miners after disconnect
				srv.scheduler.PrintAllJobs(LOGF)
				srv.scheduler.PrintAllMiners(LOGF)
			} else {

				// if server loses contact with a client
				// cease working on any requests being done on behalf of the client
				// wait for the miner's result and ignore its result
				LOGF.Printf("[Server] Client[id %d] Disconnect]\n", config.connID)
				srv.scheduler.RemoveJob(config.connID)
			}
		} else if config.message.Type == bitcoin.Join {
			// MESSAGE TYPE: JOIN
			// Add the miner to the scheduler
			// Miner will be marked as idle
			LOGF.Printf("[Server] Miner[id %d] Joined\n", config.connID)
			srv.scheduler.AddMiner(config.connID)
			srv.scheduler.ScheduleJobs(LOGF)

		} else if config.message.Type == bitcoin.Request {
			// MESSAGE TYPE: REQUEST
			// add job will add a job to scheduler.jobs
			// start recording to calculate metrics
			LOGF.Printf("[Server] Client[id %d] Request: %s\n", config.connID, config.message.Data)
			srv.rStats.StartRecording(config.connID)
			job := bitcoin.NewJob(srv.lspServer, config.connID, config.message.Data, config.message, bitcoin.ChunkSize)
			srv.scheduler.AddJob(job)
			srv.scheduler.ScheduleJobs(LOGF)

		} else if config.message.Type == bitcoin.Result {
			// MESSAGE TYPE: RESULT
			LOGF.Printf("[Server] Miner[id %d] result:%s\n", config.connID, config.message)
			minerID := config.connID

			// if a job exists
			// if a client disconnects, a job should have been removed,
			// in this case exist will return false and the result will be ignored
			job, exist := srv.scheduler.GetMinersJob(minerID)
			if exist {
				job.RemoveChunkAssignedToMiner(minerID)
				// update client's minNonce, minHash
				job.ProcessResult(minerID, config.message)
				currMinHash, currMinNonce := job.GetCurrResult()
				LOGF.Printf("[Server] Client[id %d] result: currentMinHash:%d currentMinNonce:%d\n", job.GetClientID(), currMinHash, currMinNonce)

				// If a job is complete, send the result back to the client
				if job.Complete() {
					LOGF.Printf("[Server] Client[id %d] sending final result minHash:%d minNonce:%d\n", job.GetClientID(), currMinHash, currMinNonce)
					srv.rStats.StopRecording(job.GetClientID())
					job.ProcessComplete()
					srv.scheduler.RemoveJob(job.GetClientID())
					mean, dev := srv.rStats.GetStats()
					LOGF.Printf("[Final Stat]: %f %f\n", mean, dev)
				}
			}
			srv.scheduler.MinerIdle(minerID) // Mark the Miner as Idle
			srv.scheduler.ScheduleJobs(LOGF)
		}
	}
}
