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
type ClientState int

const (
	Connect ClientState = iota // Connect State
	Active                     // Active State
	Closing                    // Closing State
	Lost                       // Conn lost State
)

type InternalType int

const (
	Read InternalType = iota
	Write
	ID
	Close
)

type client struct {
	// internal state
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
	returnTimer     chan int

	// Read | Write | ConnID
	toBeRead            *internalMsg
	readReturnChan      chan *internalMsg
	connIDReturnChan    chan *internalMsg
	writeReturnChan     chan *internalMsg
	closeProcessingChan chan int
	closeReturnChan     chan *internalMsg
	processInternal     chan *internalMsg

	// TODO: timing
	epFire       chan int
	epLimFire    chan int
	resetEpLimit chan int

	// msg send/recv/retry
	newMsgSendChan chan *Message
	msgRecvChan    chan *Message

	// internal data structures
	unAckedMsgs  *sWindowMap
	pendingRead  *priorityQueue
	pendingWrite *priorityQueue

	// logging
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
		returnTimer:     make(chan int),

		readReturnChan:      make(chan *internalMsg),
		toBeRead:            nil,
		writeReturnChan:     make(chan *internalMsg),
		connIDReturnChan:    make(chan *internalMsg),
		closeProcessingChan: make(chan int),
		closeReturnChan:     make(chan *internalMsg),
		processInternal:     make(chan *internalMsg),

		epFire:       make(chan int),
		epLimFire:    make(chan int),
		resetEpLimit: make(chan int),

		newMsgSendChan: make(chan *Message),
		msgRecvChan:    make(chan *Message),
		unAckedMsgs:    NewSWM(0, 1, 1),
		pendingRead:    NewPQ(),
		pendingWrite:   NewPQ(),

		logLvl: 0,
	}

	// Launch Main Routine
	// Launch Read Routine
	// Launch Epoch Timer
	go c.main()
	go c.reader()
	go c.timer()

	// Signal main to send NewConnect message
	// Block until connID set or EpochLimit reached
	c.newMsgSendChan <- NewConnect(initialSeqNum)
	select {
	case <-c.connFailed:
		returnAll(c)
		return nil, errors.New("connection could not be established")
	case <-c.returnNewClient:
		log.Println("Client -> Connected!")
		log.Printf("Params %s\n", c.params.String())
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
// Main blocks until the data message with sn:writeSeqNum is available
// Once available, Read returns
func (c *client) Read() ([]byte, error) {
	cLog(c, "[Client Read]", 1)
	resp := <-c.readReturnChan
	return resp.msg.Payload, resp.err
}

// Creates and sends a data message with 0 id, sn, checksum
// Main validates (assigns id, sn, checksum, errors) and sends the the message
// Sends the err back to Write
func (c *client) Write(payload []byte) error {
	cLog(c, "[Client Write]", 1)
	wr_msg := NewData(0, 0, len(payload), payload, 0)
	c.processInternal <- &internalMsg{mtype: Write, msg: wr_msg}
	resp := <-c.writeReturnChan
	return resp.err
}

// TODO:
func (c *client) Close() error {
	cLog(c, "[Client close]", 1)
	c.processInternal <- &internalMsg{mtype: Close}
	for {
		select {
		case req := <-c.closeReturnChan:
			c.returnMain <- 1
			c.returnReader <- 1
			c.returnTimer <- 1
			return req.err

		case <-c.closeProcessingChan:
			c.processInternal <- &internalMsg{mtype: Close}
		}
	}
}

func (c *client) main() {
	for {
		var readActiveChan chan *internalMsg
		if c.toBeRead != nil {
			cLog(c, "[readActiveChan] activated", 1)
			readActiveChan = c.readReturnChan
		}

		select {
		case <-c.returnMain:
			cLog(c, "[returnMain]", 1)
			return

		case msg := <-c.newMsgSendChan:

			cLog(c, "[newMsgSendChan]", 1)
			switch msg.Type {
			case MsgConnect:
				cLog(c, " :Connect", 1)
				processSendConnect(c, msg)
			case MsgData:
				cLog(c, fmt.Sprintf("[Data]: %s\n", msg), 1)
				processSendNewData(c, msg)
			}

		case msg := <-c.msgRecvChan:

			cLog(c, "[msgRecvChan]", 1)
			switch msg.Type {
			case MsgData:
				cLog(c, fmt.Sprintf("[Data]: %s\n", msg), 1)
				processRecvData(c, msg)
			case MsgAck, MsgCAck:
				cLog(c, fmt.Sprintf("[Ack]: %s\n", msg), 1)
				processRecvAcks(c, msg)
			}

		case req := <-c.processInternal:
			cLog(c, "[procInternal]", 1)
			switch req.mtype {
			case ID:
				c.connIDReturnChan <- &internalMsg{mtype: ID, id: c.connID}

			case Write:

				c.writeSeqNum++
				validateWriteInternal(c, req)
				if c.state == Closing || c.state == Lost {
					req.err = errors.New("client closed/lost")
				}

				if c.state == Active {
					sent := processSendNewData(c, req.msg)
					if sent {
						cLog(c, fmt.Sprintf("[Write proc'd] %s\n", req.msg), 1)
					} else {
						cLog(c, fmt.Sprintf("[Write pend'n] %s\n", req.msg), 1)
					}
					cLog(c, fmt.Sprintf("[Swin state] %s\n", c.unAckedMsgs.String()), 1)
				}

				c.writeReturnChan <- req

			case Close:
				// TODO: Send all msgs in Unacked messages
				// TODO: Send 0 to closeProcessingChan
				c.state = Closing
				if !c.unAckedMsgs.Empty() {
					for !c.unAckedMsgs.Empty() {
						msg, _ := c.unAckedMsgs.GetMinMsg()
						sendToServer(c.clientConn, msg)
					}
					c.closeProcessingChan <- 1
				} else {
					c.clientConn.Close()
					c.closeReturnChan <- &internalMsg{mtype: Close, err: nil}
				}
			}
		case readActiveChan <- c.toBeRead:

			cLog(c, fmt.Sprintf("[!Read] %s\n", c.toBeRead.msg), 1)
			c.readSeqNum++
			c.toBeRead = nil
			if pqmsg, exist := c.pendingRead.GetMin(); exist == nil && pqmsg.SeqNum == c.readSeqNum {
				c.pendingRead.RemoveMin()
				var err error
				if c.state == Closing || (c.state == Lost && c.pendingRead.Empty()) {
					err = errors.New("client closed or conn lost")
				}
				c.toBeRead = &internalMsg{mtype: Read, msg: pqmsg, err: err}
				cLog(c, fmt.Sprintf("[toBeRead]: %s\n", pqmsg), 2)
			}

		case <-c.epFire:

			cLog(c, "[epFire]", 2)
			if c.state == Connect {
				processReSendConnect(c)
			} else {
				processReSendDataOrHeartbeat(c)
			}

		case <-c.epLimFire:
			cLog(c, "[epLimFire]", 2)
			if c.state == Connect {
				c.connFailed <- 1
			} else {
				c.state = Lost
				c.returnReader <- 1
				c.returnTimer <- 1
				return
			}
		}
	}
}

func (c *client) reader() {
	for {
		select {
		case <-c.returnReader:
			cLog(c, "returning: reader", 1)
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

func (c *client) timer() {
	epochTimer := time.NewTimer(time.Duration(c.params.EpochMillis) * time.Millisecond)
	epochLimitTimer := time.NewTimer(time.Duration(c.params.EpochMillis*c.params.EpochLimit) * time.Millisecond)
	for {
		select {
		case <-c.returnTimer:
			cLog(c, "returning: timer", 1)
			return
		case <-epochTimer.C: // Epoch tick
			c.epFire <- 1
			epochTimer.Reset(time.Duration(c.params.EpochMillis) * time.Millisecond)

		case <-epochLimitTimer.C: // Epoch Limit tick
			c.epLimFire <- 1

		case <-c.resetEpLimit:
			epochLimitTimer.Reset(time.Duration(c.params.EpochMillis*c.params.EpochLimit) * time.Millisecond)
		}
	}
}
