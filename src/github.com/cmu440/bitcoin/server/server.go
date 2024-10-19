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

type server struct {
	lspServer      lsp.Server
	clientMap      map[int]*info
	idleMiner      []int
	connectedMiner []int
	executingTasks map[int]*task

	// Channels
	clientRequestChan chan bool
	minerJoinChan     chan bool
	resultChan        chan int // return connID
	disconnectedChan  chan int // return connID
}

type info struct {
	connID       int
	message      *bitcoin.Message
	minHashValue uint64
	minHashNonce uint64
	numTaskLeft  int
	taskQueue    []*task // task priority queue
}

type task struct {
	connID int
	start  uint64
	end    uint64
}

func startServer(port int) (*server, error) {
	// TODO: implement this!
	// start new server
	lspServer, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}
	server := &server{
		lspServer:      lspServer,
		clientMap:      make(map[int]*info),
		idleMiner:      make([]int, 0),
		connectedMiner: make([]int, 0),
		executingTasks: make(map[int]*task),
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
		if err != nil {
			srv.disconnectedChan <- connID
		}
		// handle different type of message
		var message bitcoin.Message
		err = json.Unmarshal(payload, &message)
		if err != nil {
			return
		}
		switch message.Type {
		case bitcoin.Join:
			// handle join message
			srv.minerJoinChan <- true
		case bitcoin.Request:
			// handle request message
			srv.clientRequestChan <- true
		case bitcoin.Result:
			// handle result message
			srv.resultChan <- connID
		}
	}
}

func (srv *server) processor() {
	for {
		select {
		case <-srv.clientRequestChan:
			// handle client request
			srv.handleClientRequest()
		case <-srv.minerJoinChan:
			// handle miner join
			srv.handleMinerJoin()
		case connID := <-srv.resultChan:
			// handle result
			srv.handleResult(connID)
		case connID := <-srv.disconnectedChan:
			// handle disconnected
			srv.handleDisconnected(connID)
		}
	}
}

func (srv *server) handleClientRequest() {}

func (srv *server) handleMinerJoin() {}

func (srv *server) handleResult(connID int) {}

func (srv *server) handleDisconnected(connID int) {}
