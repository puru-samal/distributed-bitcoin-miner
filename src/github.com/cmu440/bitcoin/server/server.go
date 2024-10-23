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
		LOGF.Printf("[Server] msg recv'd connID:%d msg:%s\n", config.connID, config.message)
		srv.configureChan <- config
	}
}

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

			} else {

				// if server loses contact with a client
				// cease working on any requests being done on behalf of the client
				// wait for the miner's result and ignore its result
				LOGF.Printf("[Server] Client[id %d] Disconnect]\n", config.connID)
				srv.scheduler.RemoveJob(config.connID)
			}

		}
		if config.message.Type == bitcoin.Join {

			// Add the miner to the scheduler
			// Miner will be marked as idle

			LOGF.Printf("[Server] Miner[id %d] Joined\n", config.connID)
			srv.scheduler.AddMiner(config.connID)
			srv.scheduler.ScheduleJobs()
			// TODO: LOG ALL SCHEDULER DATA-STRUCTURES HERE

		} else if config.message.Type == bitcoin.Request {

			// add job will add a job to scheduler.jobs
			// start recording to calculate metrics

			LOGF.Printf("[Server] Client[id %d] Request: %s\n", config.connID, config.message.Data)
			srv.rStats.StartRecording(config.connID)
			job := bitcoin.NewJob(srv.lspServer, config.connID, config.message.Data, config.message, bitcoin.ChunkSize)
			srv.scheduler.AddJob(job)
			srv.scheduler.ScheduleJobs()
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
				LOGF.Printf("[Server] Client[id %d] result: currentMinHash:%d currentMinNonce:%d\n", config.connID, currMinHash, currMinNonce)

				// If a job is complete, send the result back to the client
				if job.Complete() {
					LOGF.Printf("[Server] Client[id %d] sending final result minHash:%d minNonce:%d\n", clientID, currMinHash, currMinNonce)
					srv.rStats.StopRecording(clientID)
					job.ProcessComplete()
					srv.scheduler.RemoveJob(clientID)
					mean, dev := srv.rStats.GetStats()
					fmt.Printf("[Final Stat]: %f %f\n", mean, dev)
					LOGF.Printf("[Final Stat]: %f %f\n", mean, dev)
				}
				srv.scheduler.ScheduleJobs()
			} else {
				LOGF.Printf("[Server] GetMinersJob exist==False\n")
			}
			srv.scheduler.MinerIdle(minerID) // Mark the Miner as Idle
		}
	}
}
