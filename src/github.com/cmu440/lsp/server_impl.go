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
	// Server information
	params           *Params             // configuration parameters for a LSP server
	conn             *lspnet.UDPConn     // UDP connection for the server
	clientInfo       map[int]*clientInfo // map of connection IDs to client information
	nextConnectionID int                 // next connection ID to assign to a new client

	// Channels to handle different functions
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

	// Logging level
	logLvl int
}

// message and the address of the client that sent it
type clientMessage struct {
	message *Message
	addr    *lspnet.UDPAddr
}

// connection ID and payload to be sent to a client
type clientWriteRequest struct {
	connID  int
	payload []byte
}

// connection ID and payload to be read by the client
type readResponse struct {
	connID  int
	payload []byte
}

// information about a client
type clientInfo struct {
	// Client information
	addr           *lspnet.UDPAddr // address of the client
	pendingPayload map[int][]byte  // map of sequence numbers to payloads

	// Sequence numbers for reading and writing
	readSeqNum  int
	writeSeqNum int

	// Queues of unAcked messages and pending messages
	unAckedMsgs *sWindowMap
	pendingMsgs *priorityQueue

	// Status of the Client
	hasReceivedData bool // client has sent data to the server
	hasSentData     bool // client has received data from the server
	isClosed        bool // client has closed the connection

	// Number of epochs since the client has received a message
	unReceivedNum int
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	// Start listening on the specified port
	// return err if there was an error resolving on the specified port number
	laddr, err := lspnet.ResolveUDPAddr("udp", ":"+fmt.Sprint(port))
	if err != nil {
		return nil, err
	}
	// Create a UDP connection
	// return err if there was an error listening on the specified port number
	connection, err := lspnet.ListenUDP("udp", laddr)
	if err != nil {
		return nil, err
	}

	// Create and initialize the server
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

		logLvl: 0,
	}

	// read routine for the server
	go server.handleIncomingMessages()

	// main routine for the server
	go server.serverMain()

	return server, nil
}

// serverMain is the main function for the server. It handles all incoming messages
func (s *server) serverMain() {
	shuttingDown := false

	for {
		select {
		// timer for sending heartbeat messages and resending unacknowledged messages
		case <-s.ticker.C:
			sLog(s, "[servEpochFire]", 4)
			// Acknowledgement to clients that have not received any messages during the last epoch
			s.sendHeartbeatMessages()
			s.resendUAckedMessages()

		case clientMsg := <-s.incomingMsgChan:
			sLog(s, "[Server incomingMsgChan]", 4)
			messageType := clientMsg.message.Type
			clientAddr := clientMsg.addr
			connID := clientMsg.message.ConnID
			// Handle the message based on its type
			switch messageType {
			// Connect
			case MsgConnect:
				sLog(s, fmt.Sprintf("[Connect:recv]: %d\n", clientMsg.message.SeqNum), 4)
				alreadyConnected := s.checkConnection(clientMsg, clientAddr)
				if alreadyConnected {
					return
				}
			// Data
			case MsgData:
				sLog(s, fmt.Sprintf("[Data:recv]: %d\n", clientMsg.message.SeqNum), 4)
				s.processDataHandler(clientMsg, clientAddr, connID)
			// Acknowledgement
			case MsgAck:
				sLog(s, fmt.Sprintf("[Ack:recv]: %d\n", clientMsg.message.SeqNum), 4)
				acknowledged := s.processAckHandler(clientMsg, connID, shuttingDown)
				if acknowledged {
					return
				}
			// Cumulative Acknowledgement
			case MsgCAck:
				sLog(s, fmt.Sprintf("[CAck:recv]: %d\n", clientMsg.message.SeqNum), 4)
				cacknowledged := s.processCAckHandler(clientMsg, connID, shuttingDown)
				if cacknowledged {
					return
				}
			}
			if client, ok := s.clientInfo[connID]; ok {
				client.hasReceivedData = true
				client.unReceivedNum = 0
			}

		// Read a message from a client
		case <-s.readRequestChan:
			sLog(s, "[Server readRequestChan]", 4)
			s.readRequest()

		// Write a message to a client
		// writeResponseChan is from Write()
		case writeMsg := <-s.writeRequestChan:
			sLog(s, "[Server writeRequestChan]", 4)
			s.writeRequest(writeMsg)

		// If the client has no payload to read, it will remove the client
		case id := <-s.removeClientChan:
			sLog(s, "[Server removeClientChan]", 4)
			delete(s.clientInfo, id)

		// close connection of the id received from CloseConn()
		// if the connection does not exist, it returns an error
		// else it closes the connection
		case id := <-s.closeConnRequestChan:
			sLog(s, "[Server closeConnRequestChan]", 4)
			client, ok := s.clientInfo[id]
			if !ok || client.isClosed {
				sLog(s, "[Server closeConnRequestChan] connection not found", 4)
				s.closeConnResponseChan <- errors.New("connection not found")
			} else {
				// close the connection
				sLog(s, fmt.Sprintln("[Server closeConnRequestChan] client Close", id), 4)
				client.isClosed = true
				s.closeConnResponseChan <- nil
			}

		// Close() has been called on the server
		// block until all pending messages to each client have been sent and acknowledged
		case <-s.serverShutdownChan:
			sLog(s, "[Server serverShutdownChan]", 4)
			shuttingDown = true

			for connId, client := range s.clientInfo {
				if client.isClosed || (client.pendingMsgs.Empty() && client.unAckedMsgs.Empty()) {
					sLog(s, fmt.Sprintf("[Close Check] unack: %s\n", client.unAckedMsgs.String()), 4)
					sLog(s, fmt.Sprintf("[Close Check] pendM: %v\n", client.pendingMsgs.q), 4)
					delete(s.clientInfo, connId)
				}
			}

			// if there are no clients left, close the server
			if len(s.clientInfo) == 0 {
				sLog(s, "[Server serverShutdownChan] closeDoneChan", 4)
				s.shutdownCompleteChan <- true
				return
			}

		// move the pending messages to the unAcked queue as many as possible
		// the peneding message needs to be in range or the unAcked queue is empty
		default:
			sLog(s, "[Server default]", 1)
			for _, client := range s.clientInfo {
				size := client.pendingMsgs.Size()
				for size > 0 {
					msg, _ := client.pendingMsgs.RemoveMin()
					if client.isValidMessage(msg.SeqNum) || client.unAckedMsgs.Empty() {
						err := s.sendMessage(msg, client.addr)
						if err != nil {
							log.Println(err)
						}
						insertFail := client.unAckedMsgs.Put(msg.SeqNum, msg)
						if !insertFail {
							sLog(s, "[defaultActions] Error inserting into unAckedMsgs", 2)
						} else {
							sLog(s, fmt.Sprintln("[defaultActions] Inserted into unAckedMsgs: ", msg.SeqNum, "of", msg.ConnID), 4)
							size--
						}
					} else {
						client.pendingMsgs.Insert(msg)
						break
					}
				}
			}
		}
	}
}

// listen for packets from clients and unmarshal them into a Message struct
// send the message to incomingMsgChan to be handled by the serverMain function
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

// sends readRequestChan to the main function to read a message from a client (readRequest function)
// if the return value of readResponseChan from readRequest is
// (1) connID!=-1 && payload !=nil: it returns connID and payload
// (2) connID!=-1 && payload ==nil: it removes the client by sending the ID
// to removeClientChan (will be taken care in main function) and returns an error
// (3) if Close() has been called on the server, it returns an error
func (s *server) Read() (int, []byte, error) {
	sLog(s, "[Server Read]", 1)
	for {
		s.readRequestChan <- true
		readRes := <-s.readResponseChan

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

// sends writeRequestChan to the main function to write a message to a client (writeRequest function)
// return a non-nil error only if the specified connection ID does not exist/client has disconnected
// writeResponseChan is from writeRequest function
func (s *server) Write(connId int, payload []byte) error {
	sLog(s, "[Server Write]", 1)
	writeMsg := &clientWriteRequest{
		connID:  connId,
		payload: payload,
	}
	s.writeRequestChan <- writeMsg
	return <-s.writeResponseChan
}

// closes a connection with a client by sending closeConnRequestChan to the main function
func (s *server) CloseConn(connId int) error {
	sLog(s, fmt.Sprintln("[Server CloseConn]"), 4)
	s.closeConnRequestChan <- connId
	return <-s.closeConnResponseChan
}

// closes the server by sending serverShutdownChan to the main function
// when there is no client left, it sends shutdownCompleteChan and finalizes the server
func (s *server) Close() error {
	defer s.conn.Close()
	sLog(s, "[Server Close]", 4)
	s.serverShutdownChan <- true
	<-s.shutdownCompleteChan
	s.isClosed = true
	return nil
}
