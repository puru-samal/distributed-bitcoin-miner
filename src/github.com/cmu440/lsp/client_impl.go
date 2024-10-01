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
	epochTimer     *time.Ticker
	epLimitCounter int

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

		readReturnChan:      make(chan *internalMsg),
		toBeRead:            nil,
		writeReturnChan:     make(chan *internalMsg),
		connIDReturnChan:    make(chan *internalMsg),
		closeProcessingChan: make(chan int),
		closeReturnChan:     make(chan *internalMsg),
		processInternal:     make(chan *internalMsg),

		epochTimer:     time.NewTicker(time.Duration(params.EpochMillis * int(time.Millisecond))),
		epLimitCounter: 0,

		newMsgSendChan: make(chan *Message),
		msgRecvChan:    make(chan *Message),
		unAckedMsgs:    NewSWM(0, 1, 1),
		pendingRead:    NewPQ(),
		pendingWrite:   NewPQ(),

		logLvl: 1,
	}

	// Launch Main Routine
	// Launch Read Routine
	// Launch Epoch Timer
	go c.main()
	go c.reader()

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

	if resp.msg != nil {
		return resp.msg.Payload, resp.err
	}
	return nil, errors.New("connection lost/dropped")
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
	defer c.clientConn.Close()
	cLog(c, "[Client Close]", 1)
	c.processInternal <- &internalMsg{mtype: Close}
	req := <-c.closeReturnChan
	return req.err
}

func (c *client) main() {
	for {

		if (c.state == Lost && c.toBeRead == nil) || c.state == Closing {
			c.toBeRead = &internalMsg{mtype: Read, msg: nil}
		}

		var readActiveChan chan *internalMsg
		if c.toBeRead != nil {
			cLog(c, fmt.Sprintf("[state]: %d\n", c.state), 1)
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
				cLog(c, "[Connect]", 1)
				success := processSendConnect(c, msg)
				if success {
					cLog(c, fmt.Sprintln("Sent to server", msg.ConnID, msg.Size), 1)
				} else {
					cLog(c, fmt.Sprintln("Failed to send"), 1)
				}
			case MsgData:
				cLog(c, fmt.Sprintf("[Data]: %d\n", msg.SeqNum), 1)
				processSendData(c, msg)
			}

		case msg := <-c.msgRecvChan:

			cLog(c, "[msgRecvChan]", 1)
			switch msg.Type {
			case MsgData:
				cLog(c, fmt.Sprintf("[Data]: %d\n", msg.SeqNum), 1)
				processRecvData(c, msg)
			case MsgAck, MsgCAck:
				cLog(c, fmt.Sprintf("[Ack]: %d\n", msg.SeqNum), 1)
				processRecvAcks(c, msg)
			}

		case req := <-c.processInternal:
			cLog(c, "[procInternal]", 1)
			switch req.mtype {
			case ID:
				c.connIDReturnChan <- &internalMsg{mtype: ID, id: c.connID}

			case Write:

				if c.state == Closing || c.state == Lost {
					req.err = errors.New("client closed/lost")
					c.writeReturnChan <- req
				}

				if c.state == Active {
					c.writeSeqNum++
					validateWriteInternal(c, req)
					sent := processSendData(c, req.msg)
					if sent {
						cLog(c, fmt.Sprintf("[Write proc'd] %d\n", req.msg.SeqNum), 2)
					} else {
						cLog(c, fmt.Sprintf("[Write pend'n] %d\n", req.msg.SeqNum), 2)
					}
					cLog(c, fmt.Sprintf("[Swin state] %s\n", c.unAckedMsgs.String()), 3)
					c.writeReturnChan <- req
				}

			case Close:

				if c.state == Lost {
					c.closeReturnChan <- &internalMsg{mtype: Close, err: nil}
					return
				}
				c.state = Closing
				if c.state == Closing && c.unAckedMsgs.Empty() && c.pendingWrite.Empty() {
					c.epochTimer.Stop()
					close(c.returnReader)
					c.closeReturnChan <- &internalMsg{mtype: Close, err: nil}
					return
				}
			}
		case readActiveChan <- c.toBeRead:
			processRead(c)

		case <-c.epochTimer.C:

			cLog(c, "[epFire]", 2)
			if c.state == Connect {
				processReSendConnect(c)
				c.epLimitCounter++
			} else if c.state == Active || c.state == Closing {
				processReSendDataOrHeartbeat(c)
			}
			c.epLimitCounter++

			if c.epLimitCounter == c.params.EpochLimit {
				cLog(c, "[EpochLimit]", 2)
				processEpochLimit(c)
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
