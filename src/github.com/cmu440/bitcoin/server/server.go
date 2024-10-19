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

// priorityQueue
type priorityQueue struct {
	q []*info
}

func newPQ() *priorityQueue {
	newQueue := &priorityQueue{
		q: make([]*info, 0),
	}
	return newQueue
}

func (pq *priorityQueue) insert(elem *info) {
	pq.q = append(pq.q, elem)
	pq.minHeapifyUp(len(pq.q) - 1)
}

func (pq *priorityQueue) getMin() (*info, error) {
	if len(pq.q) == 0 {
		return nil, fmt.Errorf("priority queue is empty")
	}
	return pq.q[0], nil
}

func (pq *priorityQueue) removeMin() (*info, error) {
	min, err := pq.getMin()
	if err != nil {
		return nil, err
	}
	pq.q[0] = pq.q[len(pq.q)-1]
	pq.q = pq.q[:len(pq.q)-1]
	pq.minHeapifyDown(0)
	return min, nil
}

func (pq *priorityQueue) empty() bool {
	return len(pq.q) == 0
}

func (pq *priorityQueue) size() int {
	return len(pq.q)
}

func (pq *priorityQueue) minHeapifyUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if pq.q[parent].message.Nonce > pq.q[i].message.Nonce {
			pq.q[parent], pq.q[i] = pq.q[i], pq.q[parent]
			i = parent
		} else {
			break
		}
	}
}

func (pq *priorityQueue) minHeapifyDown(i int) {
	for {
		left := 2*i + 1
		right := 2*i + 2
		smallest := i
		if left < len(pq.q) && pq.q[left].message.Nonce < pq.q[smallest].message.Nonce {
			smallest = left
		}
		if right < len(pq.q) && pq.q[right].message.Nonce < pq.q[smallest].message.Nonce {
			smallest = right
		}
		if smallest != i {
			pq.q[i], pq.q[smallest] = pq.q[smallest], pq.q[i]
			i = smallest
		} else {
			break
		}
	}
}

func (pq *priorityQueue) remove(elem *info) {
	for i, e := range pq.q {
		if e == elem {

			break
		}
	}
}

// server
type server struct {
	lspServer      lsp.Server
	clientMap      map[int]*info // clientID -> info
	requestQueue   *priorityQueue
	idleMiner      []int
	connectedMiner []int
	executingTasks map[int]*task // minerID -> task

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
		lspServer:      lspServer,
		clientMap:      make(map[int]*info),
		requestQueue:   newPQ(),
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
	go srv.distributor()
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
				srv.handleDisconnected(config.connID)
			}
			if config.message.Type == bitcoin.Join {
				srv.handleMinerJoin(config.connID)
			} else if config.message.Type == bitcoin.Request {
				srv.handleClientRequest(config.connID, config.message)
			} else if config.message.Type == bitcoin.Result {
				srv.handleResult(config.connID)
			}
		}
	}
}

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
