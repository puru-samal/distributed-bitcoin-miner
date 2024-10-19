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
	// clientMap      map[int]*info // clientID -> info
	// idleMiner      []int
	// connectedMiner []int
	// executingTasks map[int]*task // minerID -> task
	rStats    bitcoin.RunningStats
	scheduler bitcoin.Scheduler

	// Channel
	configureChan chan *config
}

type info struct {
	connID       int
	message      *bitcoin.Message
	minHashValue uint64
	minHashNonce uint64
	numTaskLeft  int
	taskQueue    []*task // FIFO
}

type task struct {
	connID int // clientID
	start  uint64
	end    uint64
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
		// clientMap:      make(map[int]*info),
		// idleMiner:      make([]int, 0),
		// connectedMiner: make([]int, 0),
		// executingTasks: make(map[int]*task),
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
	// go srv.distributor()
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
				if isMiner {
					srv.scheduler.RemoveMiner(config.connID)
				} else {
					srv.scheduler.RemoveJob(config.connID)
				}
				//srv.handleDisconnected(config.connID)
			}
			if config.message.Type == bitcoin.Join {
				srv.scheduler.AddMiner(config.connID)
				srv.scheduler.ScheduleJobs()
				//srv.handleMinerJoin(config.connID)
			} else if config.message.Type == bitcoin.Request {
				srv.rStats.StartRecording(config.connID)
				job := bitcoin.NewJob(srv.lspServer, config.connID, config.message.Data, config.message, 10000)
				srv.scheduler.AddJob(job)
				srv.scheduler.ScheduleJobs()
				//srv.handleClientRequest(config.connID, config.message)
			} else if config.message.Type == bitcoin.Result {
				minerID := config.connID
				//clientID := miner.currClientID

				job, clientID, _ := srv.scheduler.GetMinersJob(minerID)
				job.ProcessResult(minerID, config.message)

				// send result back to cliet
				if job.Complete() {
					srv.rStats.StopRecording(clientID)
					job.ProcessComplete()
					srv.scheduler.RemoveJob(clientID)
				}
				srv.scheduler.ScheduleJobs()
				//srv.handleResult(config.connID)
			}
		}
	}
}

/*
func (srv *server) distributor() {

}


func (srv *server) handleClientRequest(connID int, message *bitcoin.Message) {
	// need to divide the request into chunks
	// need update on the numTaskLeft and taskQueue
	requestInfo := &info{
		connID:       connID,
		message:      message,
		minHashValue: ^uint64(0),
		minHashNonce: 0,
		numTaskLeft:  0,
		taskQueue:    make([]*task, 0),
	}
	srv.clientMap[connID] = requestInfo
	srv.requestQueue.insert(requestInfo)
}

func (srv *server) handleMinerJoin(connID int) {
	srv.idleMiner = append(srv.idleMiner, connID)
	srv.connectedMiner = append(srv.connectedMiner, connID)
}

func (srv *server) handleResult(connID int, message *bitcoin.Message) {
	clientID := srv.executingTasks[connID].connID
	requestInfo, exist := srv.clientMap[clientID]

	delete(srv.executingTasks, connID)
	srv.idleMiner = append(srv.idleMiner, connID)
	if !exist {
		return
	}

	requestInfo.numTaskLeft--
	if message.Hash < requestInfo.minHashValue {
		requestInfo.minHashValue = message.Hash
		requestInfo.minHashNonce = message.Nonce
	}
	if requestInfo.numTaskLeft == 0 {
		// send the result back to the client
		result := bitcoin.NewResult(requestInfo.minHashValue, requestInfo.minHashNonce)
		payload, err := json.Marshal(result)
		if err != nil {
			return
		}
		err = srv.lspServer.Write(clientID, payload)
		if err != nil {
			return
		}
		delete(srv.clientMap, clientID)
		srv.requestQueue.remove(requestInfo)
	}

}

func (srv *server) handleDisconnected(connID int) {}
*/
