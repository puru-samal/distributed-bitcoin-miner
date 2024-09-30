// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

	logLvl int
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

		ticker:            time.NewTicker(time.Duration(params.EpochMillis * int(time.Millisecond))),
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

		logLvl: 4,
	}

	go server.handleIncomingMessages()

	go server.serverMain()

	return server, nil
}

// serverMain is the main function for the server. It handles all incoming messages
func (s *server) serverMain() {
	shuttingDown := false
	for {
		select {
		case <-s.ticker.C:
			sLog(s, "[Server ticker.C]", 1)
			// Acknowledgement to clients that have not received any messages during the last epoch
			s.sendHeartbeatMessages()
			s.resendUnAckedMessages()

		case clientMsg := <-s.incomingMsgChan:
			sLog(s, "[Server incomingMsgChan]", 1)
			messageType := clientMsg.message.Type
			clientAddr := clientMsg.addr
			connId := clientMsg.message.ConnID
			switch messageType {
			case MsgConnect:
				sLog(s, "[Server incomingMsgChan MsgConnect]", 1)
				alreadyConnected := s.checkConnection(clientMsg, clientAddr)
				if alreadyConnected {
					continue
				}
			case MsgData:
				sLog(s, "[Server incomingMsgChan MsgData]", 1)
				s.DataHandler(clientMsg, clientAddr, connId)

			case MsgAck:
				sLog(s, "[Server incomingMsgChan MsgAck]", 1)
				acknowledged := s.AckHandler(clientMsg, connId, shuttingDown)
				if acknowledged {
					return
				}
			case MsgCAck:
				sLog(s, "[Server incomingMsgChan MsgCAck]", 1)
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
			sLog(s, "[Server readRequestChan]", 1)
			s.readRequest()

		case writeMsg := <-s.writeRequestChan:
			sLog(s, "[Server writeRequestChan]", 1)
			s.writeRequest(writeMsg)

		case id := <-s.removeClientChan:
			sLog(s, "[Server removeClientChan]", 1)
			delete(s.clientInfo, id)

		case id := <-s.closeConnRequestChan:
			sLog(s, "[Server closeConnRequestChan]", 1)
			client, ok := s.clientInfo[id]
			if !ok || client.closed {
				s.closeConnResponseChan <- errors.New("connection not found")
			} else {
				client.closed = true
				s.closeConnResponseChan <- nil
			}

		case <-s.serverShutdownChan:
			sLog(s, "[Server serverShutdownChan]", 1)
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
			sLog(s, "[Server default]", 1)
			s.defaultActions()
		}
	}
}

// handleIncomingMessages reads incoming messages from clients and signals them to the server
func (s *server) handleIncomingMessages() {
	sLog(s, "[Server handleIncomingMessages]", 1)

	buffer := make([]byte, 1024)

	for {
		n, addr, err := s.conn.ReadFromUDP(buffer)
		if err != nil {
			errString := err.Error()
			if strings.Contains(errString, "use of closed network connection") {
				return
			}
			log.Println("Error reading from UDP connection:", err)
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

// Read reads a message from a client. If the server is closed, it returns an error
func (s *server) Read() (int, []byte, error) {
	sLog(s, "[Server Read]", 1)
	for {
		s.readRequestChan <- true
		readRes, ok := <-s.readResponseChan
		if !ok {
			return readRes.connID, nil, errors.New("client connection does not exist")
		}

		if readRes.payload != nil {
			return readRes.connID, readRes.payload, nil
		} else if readRes.connID != -1 {
			s.removeClientChan <- readRes.connID
			return readRes.connID, nil, errors.New("need to delete client")
		}

		if s.isClosed {
			return 0, nil, errors.New("server is closed")
		}
	}
}

// Write writes a message to a client. If the server is closed, it returns an error
func (s *server) Write(connId int, payload []byte) error {
	sLog(s, "[Server Write]", 1)
	writeMsg := &clientWriteRequest{
		connID:  connId,
		payload: payload,
	}
	s.writeRequestChan <- writeMsg
	return <-s.writeResponseChan
}

// CloseConn closes a connection with a client of the given connection ID
func (s *server) CloseConn(connId int) error {
	sLog(s, "[Server CloseConn]", 1)
	s.closeConnRequestChan <- connId
	return <-s.closeConnResponseChan
}

// Close closes the server
func (s *server) Close() error {
	sLog(s, "[Server Close]", 1)
	defer s.conn.Close()
	s.isClosed = true
	<-s.shutdownCompleteChan
	s.serverShutdownChan <- true
	return nil
}
