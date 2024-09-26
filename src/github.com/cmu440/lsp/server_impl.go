// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cmu440/lspnet"
)

type server struct {
	params           *Params
	conn             *lspnet.UDPConn
	clientInfo       map[int]*clientInfo
	nextConnectionID int

	// Channels to read and write messages to/from clients
	ticker            *time.Ticker
	incomingMsgChan   chan *clientMessage
	readRequestChan   chan bool
	readResponseChan  chan *readResponse
	writeRequestChan  chan *clientWriteRequest
	writeResponseChan chan error

	// Channels for closing connection w/ client
	closeConnRequestChan  chan int
	closeConnResponseChan chan error
	removeClientChan      chan int

	// Channels for server close
	isClosed             bool
	serverShutdownChan   chan bool
	shutdownCompleteChan chan bool
	connectionLostChan   chan bool
}

// clientMessage contains a message and the address of the client that sent it
type clientMessage struct {
	message *Message
	addr    *lspnet.UDPAddr
}

// clientWriteRequest contains the connection ID and payload to be sent to a client
type clientWriteRequest struct {
	connID  int
	payload []byte
}

// readResponse contains the connection ID and payload to be read by the client
type readResponse struct {
	connID  int
	payload []byte
}

// clientInfo contains information about a client
type clientInfo struct {
	addr           *lspnet.UDPAddr
	pendingPayload map[int][]byte

	// Sequence numbers for reading and writing
	readSeqNum  int
	writeSeqNum int

	unAckedMsgs *sWindowMap
	pendingMsgs *priorityQueue

	// variables to keep track of whether the client has received or sent data
	hasReceivedData bool
	hasSentData     bool
	closed          bool

	unReceivedNum int
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

		ticker:            time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)),
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

	go server.serverMain()

	go server.monitorDisconnectedClients()

	return server, nil
}

// serverMain is the main function for the server. It handles all incoming messages
func (s *server) serverMain() {
	shuttingDown := false

	for {
		select {
		case <-s.ticker.C:
			// Resend unacknowledged messages
			s.resendUnAckedMessages()
			// Acknowledgement to clients that have not received any messages during the last epoch
			s.sendHeartbeatMessages()

		case clientMsg := <-s.incomingMsgChan:
			messageType := clientMsg.message.Type
			clientAddr := clientMsg.addr
			connId := clientMsg.message.ConnID
			switch messageType {
			case MsgConnect:
				alreadyConnected := s.checkConnection(clientMsg, clientAddr)
				if alreadyConnected {
					continue
				}
			case MsgData:
				s.DataHandler(clientMsg, clientAddr, connId)

			case MsgAck:
				acknowledged := s.AckHandler(clientMsg, connId, shuttingDown)
				if acknowledged {
					return
				}
			case MsgCAck:
				cacknowledged := s.CAckHandler(clientMsg, connId, shuttingDown)
				if cacknowledged {
					return
				}
			}
			if client, ok := s.clientInfo[connId]; ok {
				client.hasReceivedData = true
				client.unReceivedNum = 0
			}

		case <-s.readRequestChan:
			s.readRequest()

		case writeMsg := <-s.writeRequestChan:
			s.writeRequest(writeMsg)

		case id := <-s.removeClientChan:
			delete(s.clientInfo, id)

		case id := <-s.closeConnRequestChan:
			client, ok := s.clientInfo[id]
			if !ok || client.closed {
				s.closeConnResponseChan <- errors.New("Connection not found")
			} else {
				client.closed = true
				s.closeConnResponseChan <- nil
			}

		case <-s.serverShutdownChan:
			shuttingDown = true
			for connId, client := range s.clientInfo {
				if client.closed || (client.pendingMsgs.Empty() && client.unAckedMsgs.Empty()) {
					delete(s.clientInfo, connId)
				}
			}
			if len(s.clientInfo) == 0 {
				s.shutdownCompleteChan <- true
				return
			}

		default:
			s.defaultActions()
		}
	}
}

// handleIncomingMessages reads incoming messages from clients and signals them to the server
func (s *server) handleIncomingMessages() {
	buffer := make([]byte, 1024)

	for {
		n, addr, err := s.conn.ReadFromUDP(buffer)
		if err != nil {
			errString := err.Error()
			if strings.Contains(errString, "use of closed network connection") {
				return
			}
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

// monitorDisconnectedClients monitors the connectionLostChan for disconnected clients
func (s *server) monitorDisconnectedClients() {
	for {
		select {
		case <-s.connectionLostChan:
			return
		}
	}
}

// Read reads a message from a client. If the server is closed, it returns an error
func (s *server) Read() (int, []byte, error) {
	for {
		s.readRequestChan <- true
		if readRes := <-s.readResponseChan; readRes.payload != nil {
			return readRes.connID, readRes.payload, nil
		} else if readRes.connID != -1 {
			s.removeClientChan <- readRes.connID
			return readRes.connID, nil, errors.New("server is closed")
		}
		time.Sleep(time.Millisecond)
	}
}

// Write writes a message to a client. If the server is closed, it returns an error
func (s *server) Write(connId int, payload []byte) error {
	writeMsg := &clientWriteRequest{
		connID:  connId,
		payload: payload,
	}
	s.writeRequestChan <- writeMsg
	return <-s.writeResponseChan
}

// CloseConn closes a connection with a client of the given connection ID
func (s *server) CloseConn(connId int) error {
	s.closeConnRequestChan <- connId
	return <-s.closeConnResponseChan
}

// Close closes the server
func (s *server) Close() error {
	defer s.conn.Close()
	s.isClosed = true
	s.connectionLostChan <- true
	<-s.shutdownCompleteChan
	s.serverShutdownChan <- true
	return nil
}
