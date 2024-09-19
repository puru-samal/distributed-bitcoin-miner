// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cmu440/lspnet"
)

type server struct {
	params           *Params
	conn             *lspnet.UDPConn
	clientInfo       map[int]*clientInfo
	nextConnectionID int

	incomingMsgChan   chan *clientMessage
	readRequestChan   chan bool
	readResponseChan  chan *readResponse
	writeRequestChan  chan *clientWriteRequest
	writeResponseChan chan error

	closeConnRequestChan  chan int
	closeConnResponseChan chan error
	removeClientChan      chan int

	isClosed             bool
	serverShutdownChan   chan bool
	shutdownCompleteChan chan bool
	connectionLostChan   chan bool
}

type clientMessage struct {
	message *Message
	addr    *lspnet.UDPAddr
}

type clientWriteRequest struct {
	connID  int
	payload []byte
}

type readResponse struct {
	connID  int
	payload []byte
}

type clientInfo struct {
	addr            *lspnet.UDPAddr
	hasReceivedData bool
	hasSentData     bool
	closed          bool
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	laddr, err := lspnet.ResolveUDPAddr("udp", ":"+fmt.Sprint(port))
	if err != nil {
		return nil, err
	}
	connection, err := lspnet.ListenUDP("udp", laddr)
	if err != nil {
		return nil, err
	}

	server := &server{
		params:           params,
		conn:             connection,
		clientInfo:       make(map[int]*clientInfo),
		nextConnectionID: 1,

		incomingMsgChan:   make(chan *clientMessage),
		readRequestChan:   make(chan bool),
		readResponseChan:  make(chan *readResponse),
		writeRequestChan:  make(chan *clientWriteRequest),
		writeResponseChan: make(chan error),

		closeConnRequestChan:  make(chan int),
		closeConnResponseChan: make(chan error),
		removeClientChan:      make(chan int),

		isClosed:             false,
		serverShutdownChan:   make(chan bool),
		shutdownCompleteChan: make(chan bool),
		connectionLostChan:   make(chan bool),
	}
	go server.handleIncomingMessages()
	go server.monitorDisconnectedClients()
	go server.serverMain()

	return server, nil
}

func (s *server) handleIncomingMessages() {

	buffer := make([]byte, 1024)

	for {
		n, addr, err := s.conn.ReadFromUDP(buffer)
		fmt.Printf("[Server] Receive Message: %v\n", n)
		if err != nil {
			continue
		}
		var msg Message
		err = json.Unmarshal(buffer[:n], &msg)
		if err != nil {
			continue
		}
		clientMsg := &clientMessage{
			message: &msg,
			addr:    addr,
		}
		s.incomingMsgChan <- clientMsg
	}
}

func (s *server) monitorDisconnectedClients() {
	for {
		select {
		case <-s.connectionLostChan:
			return
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	for {
		s.readRequestChan <- true
		readRes := <-s.readResponseChan
		if readRes.payload != nil {
			return readRes.connID, readRes.payload, nil
		} else if s.isClosed {
			return -1, nil, errors.New("Server is closed")
		}
	}
}

func (s *server) Write(connId int, payload []byte) error {
	writeMsg := &clientWriteRequest{
		connID:  connId,
		payload: payload,
	}
	s.writeRequestChan <- writeMsg
	return <-s.writeResponseChan
}

func (s *server) CloseConn(connId int) error {
	s.closeConnRequestChan <- connId
	return <-s.closeConnResponseChan
}

func (s *server) Close() error {
	defer s.conn.Close()
	s.isClosed = true
	s.connectionLostChan <- true
	<-s.shutdownCompleteChan
	s.serverShutdownChan <- true
	return nil
}
