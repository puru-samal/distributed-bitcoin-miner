// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/cmu440/lspnet"
)

// Helper types

// Type for clients internal state
type ClientState int

const (
	Connect ClientState = iota // Connect State
	Active                     // Active State
	Closing                    // Closing State
	Lost                       // Conn lost State
)

// A type for internal requests
type InternalType int

const (
	Read InternalType = iota
	Write
	ID
	Close
)

type client struct {

	// Internal client state
	state       ClientState
	serverAddr  *lspnet.UDPAddr
	clientConn  *lspnet.UDPConn
	params      *Params
	connID      int
	writeSeqNum int
	readSeqNum  int
	connFailed  chan int

	// Return signals
	returnNewClient chan int
	returnMain      chan int
	returnReader    chan int

	// Read | Write | ConnID
	toBeRead         *internalMsg
	readReturnChan   chan *internalMsg
	connIDReturnChan chan *internalMsg
	writeReturnChan  chan *internalMsg
	closeReturnChan  chan *internalMsg
	processInternal  chan *internalMsg

	// Timing
	epochTimer     *time.Ticker
	epLimitCounter int

	// Msg Send | Recv ch
	newConnectSendChan chan *Message
	msgRecvChan        chan *Message

	// Internal Data Structures
	unAckedMsgs  *sWindowMap
	unProcData   *sWindowMap
	pendingRead  *priorityQueue
	pendingWrite *priorityQueue

	// Logging
	logLvl int
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// initialSeqNum is an int representing the Initial Sequence Number (ISN) this
// client must use. You may assume that sequence numbers do not wrap around.
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, initialSeqNum int, params *Params) (Client, error) {

	// Create a connection
	serverAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		log.Println("Error: serverAddr un-resolved")
		return nil, err
	}

	conn, err := lspnet.DialUDP("udp", nil, serverAddr)
	if err != nil {
		log.Println("Error: Could not connect")
		return nil, err
	}

	c := &client{
		state:       Connect,
		serverAddr:  serverAddr,
		clientConn:  conn,
		params:      params,
		connID:      0,
		writeSeqNum: initialSeqNum,
		readSeqNum:  initialSeqNum + 1,
		connFailed:  make(chan int),

		returnNewClient: make(chan int),
		returnMain:      make(chan int),
		returnReader:    make(chan int),

		readReturnChan:   make(chan *internalMsg),
		toBeRead:         nil,
		writeReturnChan:  make(chan *internalMsg),
		connIDReturnChan: make(chan *internalMsg),
		closeReturnChan:  make(chan *internalMsg),
		processInternal:  make(chan *internalMsg),

		epochTimer:     time.NewTicker(time.Duration(params.EpochMillis * int(time.Millisecond))),
		epLimitCounter: 0,

		newConnectSendChan: make(chan *Message),
		msgRecvChan:        make(chan *Message),
		unAckedMsgs:        NewSWM(0, 1, 1),
		unProcData:         NewSWM(0, 1, 1),
		pendingRead:        NewPQ(),
		pendingWrite:       NewPQ(),

		logLvl: 0,
	}

	// Launch Main Routine
	// Launch Read Routine
	go c.main()
	go c.reader()

	// Send NewConnect to main
	// Block until connID set or EpochLimit reached
	c.newConnectSendChan <- NewConnect(initialSeqNum)
	select {
	case <-c.connFailed:
		returnAll(c)
		return nil, errors.New("connection could not be established")
	case <-c.returnNewClient:
		cLog(c, "Client -> Connected!", 1)
		cLog(c, fmt.Sprintf("Params %s\n", c.params.String()), 1)
		return c, nil
	}
}

// Sends an internalMsg to Main
// Main processes the request and sends a response with the clients connID
func (c *client) ConnID() int {
	cLog(c, "[Client ConnID]", 1)
	c.processInternal <- &internalMsg{mtype: ID}
	req := <-c.connIDReturnChan
	return req.id
}

// Sends an internalMsg to Main
// Main blocks until the data message with sn:readSeqNum is available
// Once available, Read returns
// On state:Closing w/ no pending messages left, or on state:Lost
// The Internal msg will have a nil msg, indicating that read should return an error
func (c *client) Read() ([]byte, error) {
	cLog(c, "[Client Read]", 1)
	resp := <-c.readReturnChan
	if resp.msg != nil {
		return resp.msg.Payload, resp.err
	}
	return nil, errors.New("connection lost/dropped")
}

// Creates and sends a data message with 0 id, sn, checksum
// Main validates (assigns id, sn, checksum, errors) and sends the the message
// If state:Closing or state:Lost, nothing is sent, an error is sent back immediately
func (c *client) Write(payload []byte) error {
	cLog(c, "[Client Write]", 1)
	wr_msg := NewData(0, 0, len(payload), payload, 0)
	c.processInternal <- &internalMsg{mtype: Write, msg: wr_msg}
	resp := <-c.writeReturnChan
	return resp.err
}

// Close sends an internalRequest
// closeReturnChan becomes available once:
// all close conditions have been met or conn is lost,
func (c *client) Close() error {
	defer c.clientConn.Close()
	cLog(c, "[Client Close]", 1)
	c.processInternal <- &internalMsg{mtype: Close}
	req := <-c.closeReturnChan
	close(c.returnMain)
	return req.err
}

// Routine handles sending and recieving messages
// manages the internal data structures
// manages the timers and the logic that come with them
func (c *client) main() {
	for {
		// If read error condition is met, set c.toBeRead to have a
		// nil msg field which will cause Read to return an error
		if c.readErrCond() {
			c.toBeRead = &internalMsg{mtype: Read, msg: nil}
		}

		// readActiveChan is a nil channel which gets set to
		// readReturnChan when a message is available for reading
		var readActiveChan chan *internalMsg
		if c.toBeRead != nil {
			cLog(c, "[readActiveChan] activated", 1)
			cLog(c, fmt.Sprintf("[#PendingRead]: %d\n", c.pendingRead.Size()), 1)
			readActiveChan = c.readReturnChan
		}

		select {

		case <-c.returnMain:
			// This channel becomes active when main has to return
			cLog(c, "[returnMain]", 1)
			return

		case msg := <-c.newConnectSendChan:
			// This channel gets the initial NewConnect message
			// from NewClient which it sends to server
			// see handleConnet for how the client state is initialized
			success := handleConnect(c, msg)
			cLog(c, fmt.Sprintf("[Connect] %d, Sent?:%v\n", msg.ConnID, success), 2)

		case msg := <-c.msgRecvChan:
			// This channel recieves messages forwarded by the Reader routine
			// see comments for each MsgType handler for more details
			cLog(c, "[msgRecvChan]", 2)
			switch msg.Type {

			case MsgData:
				cLog(c, fmt.Sprintf("[Data]: %d\n", msg.SeqNum), 2)
				processRecvData(c, msg)

			case MsgAck:
				cLog(c, fmt.Sprintf("[Ack]: %d\n", msg.SeqNum), 2)
				processRecvAcks(c, msg)

			case MsgCAck:
				// simulate the server recieving a series of acks
				cLog(c, fmt.Sprintf("[CAck]: %d\n", msg.SeqNum), 2)
				lower := c.unAckedMsgs.LB
				for i := lower; i <= msg.SeqNum; i++ {
					ack := NewAck(c.connID, i)
					processRecvAcks(c, ack)
				}
			}

		case req := <-c.processInternal:
			// This channel handles the processing of ConnID, Write and Close calls
			cLog(c, "[procInternal]", 1)
			switch req.mtype {

			case ID:
				// Send the connID
				c.connIDReturnChan <- &internalMsg{mtype: ID, id: c.connID}

			case Write:
				// If connection is closed || lost, immediately return an error
				if c.state == Closing || c.state == Lost {
					req.err = errors.New("client closed/lost")
					c.writeReturnChan <- req
				}

				// If connection is active, validate the message in the write request
				// Then, either send the data if within the bounds of the sliding window
				// or put the message in pendingWrites to send later
				if c.state == Active {
					c.writeSeqNum++
					validateWriteInternal(c, req)
					sent := processSendData(c, req.msg)
					cLog(c, fmt.Sprintf("[Write]: %d, pending?: %v\n", req.msg.SeqNum, sent), 2)
					cLog(c, fmt.Sprintf("[Swin state] %s\n", c.unAckedMsgs.String()), 3)
					c.writeReturnChan <- req
				}

			case Close:
				// If connection is lost || closed, immediately return an error
				if c.state == Lost || c.state == Closing {
					c.closeReturnChan <- &internalMsg{mtype: Close, err: nil}
					return
				}

				// If error condition is met, stop timer, signal reader to return and return
				c.state = Closing
				if c.closeDoneCond() {
					c.epochTimer.Stop()
					close(c.returnReader)
					c.closeReturnChan <- &internalMsg{mtype: Close, err: nil}
				}
			}

		case readActiveChan <- c.toBeRead:
			// This channel handles the processing of Read
			processRead(c)

		case <-c.epochTimer.C:
			// This channel handles processing during epoch fire's
			// epochLimit counter is incremented, if it reaches EpochLimit
			// the connection is deemed to be lost
			// It is the responsibility of the message recievers to reset epLimitCounter
			cLog(c, "[EpFire]", 1)
			if c.state == Connect {
				processSendConnect(c)
			} else if c.state == Active || c.state == Closing {
				processEpochFire(c)
			}
			c.epLimitCounter++
			if c.epLimitCounter == c.params.EpochLimit {
				cLog(c, "[EpochLimit]", 2)
				processEpochLimit(c)
			}
		}
	}
}

// Routine that listens for messages from server
func (c *client) reader() {
	for {
		select {
		case <-c.returnReader:
			cLog(c, "[returnReader]", 1)
			return
		default:
			var msg Message
			err := recvFromServer(c.clientConn, &msg)
			if err == nil {
				c.msgRecvChan <- &msg
			}
		}
	}
}
