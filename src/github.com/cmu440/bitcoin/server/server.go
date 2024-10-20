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
		lspServer: lspServer,
		rStats:    bitcoin.RunningStats{},
		scheduler: bitcoin.NewFCFS(lspServer),
	}
	return server, nil
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "serverLog.txt"
		flag = os.O_RDWR | os.O_CREATE
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
	go srv.processor()
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
		err = json.Unmarshal(payload, &message)
		if err != nil {
			return
		}
		config := &config{
			connID:       connID,
			message:      &message,
			disconnected: disconnected,
		}

		srv.configureChan <- config
	}
}

func (srv *server) processor() {
	for {
		select {
		case config := <-srv.configureChan:
			if config.disconnected {
				isMiner := srv.scheduler.IsMiner(config.connID)
				// if server loses contact with a miner
				// reassign any job that was assigned to the miner
				if isMiner {
					LOGF.Printf("[Miner[id %d] Disconnect]\n", config.connID)
					srv.scheduler.RemoveMiner(config.connID)
				} else {
					// if server loses contact with a client
					// cease working on any requests being done on behalf of the client
					// wait for the miner's result and ignore its result
					LOGF.Printf("[Client[id %d] Disconnect]\n", config.connID)
					srv.scheduler.RemoveJob(config.connID)
				}

			}
			if config.message.Type == bitcoin.Join {
				LOGF.Printf("[Miner[id %d] Connected]\n", config.connID)
				srv.scheduler.AddMiner(config.connID)
				srv.scheduler.ScheduleJobs()

			} else if config.message.Type == bitcoin.Request {
				LOGF.Printf("[Client[id %d] Request]: %s\n", config.connID, config.message.Data)
				srv.rStats.StartRecording(config.connID)
				job := bitcoin.NewJob(srv.lspServer, config.connID, config.message.Data, config.message, 10000)
				srv.scheduler.AddJob(job)
				srv.scheduler.ScheduleJobs()

			} else if config.message.Type == bitcoin.Result {
				minerID := config.connID

				job, clientID, exist := srv.scheduler.GetMinersJob(minerID)
				srv.scheduler.MinerDone(minerID)
				// if the client has been disconnected
				// ignore the result
				if !exist {
					return
				}
				job.ProcessResult(minerID, config.message)

				currMinHash, currMinNonce := job.GetMinHash()

				LOGF.Printf("[Miner[id %d] Result Received] currentMinHash:%s currentMinNonce:%s\n", config.connID, currMinHash, currMinNonce)
				// send result back to cliet
				if job.Complete() {
					LOGF.Printf("[Client[id %d] Final Result Send] minHash:%s minNonce:%s\n", clientID, currMinHash, currMinNonce)
					srv.rStats.StopRecording(clientID)
					job.ProcessComplete()
					srv.scheduler.RemoveJob(clientID)
					mean, dev := srv.rStats.GetStats()
					LOGF.Printf("[Final Stat]: %f %f\n", mean, dev)
				}
				srv.scheduler.ScheduleJobs()
			}
		}
	}
}
