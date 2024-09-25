// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
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
	state      ClientState
	serverAddr *lspnet.UDPAddr
	clientConn *lspnet.UDPConn
	params     *Params
	connID     int
	currSeqNum int
	connLost   chan int

	// Return signals
	returnNewClient chan int
	returnMain      chan int
	returnReader    chan int
	returnTimer     chan int

	// Read | Write | ConnID
	processRead      bool
	readReturnChan   chan *internalMsg
	connIDReturnChan chan *internalMsg
	writeReturnChan  chan *internalMsg
	closeMsgChan     chan *Message
	closeReturnChan  chan *internalMsg
	processInternal  chan *internalMsg

	// TODO: timing
	counter       int
	updateCounter chan int

	// msg send/recv
	msgSendChan chan *Message
	msgRecvChan chan *Message

	// internal data structures
	unAckedMsgs *sWindowMap
	pendingRead *priorityQueue
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
		state:      Connect,
		serverAddr: serverAddr,
		clientConn: conn,
		params:     params,
		connID:     0,
		currSeqNum: initialSeqNum,
		connLost:   make(chan int),

		returnNewClient: make(chan int),
		returnMain:      make(chan int),
		returnReader:    make(chan int),
		returnTimer:     make(chan int),

		processRead:      false,
		readReturnChan:   make(chan *internalMsg),
		writeReturnChan:  make(chan *internalMsg),
		connIDReturnChan: make(chan *internalMsg),
		closeReturnChan:  make(chan *internalMsg),
		processInternal:  make(chan *internalMsg),

		counter:       0,
		updateCounter: make(chan int),

		msgSendChan: make(chan *Message),
		msgRecvChan: make(chan *Message),

		unAckedMsgs: NewSWM(0, 1, 1),
		pendingRead: NewPQ(),
	}

	// Launch Main Routine
	// Launch Read Routine
	// Launch Epoch Timer
	go c.main()
	go c.reader()
	go c.epochTimer()

	// Signal main to send NewConnect message
	// Block until connID set or EpochLimit reached
	c.msgSendChan <- NewConnect(initialSeqNum)
	select {
	case <-c.connLost:
		return nil, errors.New("connection could not be established")
	case <-c.returnNewClient:
		log.Println("Client -> Connected!")
		return c, nil
	}
}

// Sends an internalMsg to Main
// Main processes the request and sends a response with the clients connID
func (c *client) ConnID() int {
	c.processInternal <- &internalMsg{mtype: ID}
	req := <-c.connIDReturnChan
	return req.id
}

// Sends an internalMsg to Main
// Main blocks until the data message with sn:currSeqNum is available
// Once available, Read sends an Ack message and returns
func (c *client) Read() ([]byte, error) {
	c.processInternal <- &internalMsg{mtype: Read}
	resp := <-c.readReturnChan
	if resp.err == nil {
		c.msgSendChan <- NewAck(c.connID, resp.msg.SeqNum)
	}
	return resp.msg.Payload, resp.err
}

// Creates and sends a data message with 0 id, sn, checksum
// Main validates (assigns id, sn, checksum, errors) and sends the the message back to Write
// Write then sends the message to it to Main if there's no errors
func (c *client) Write(payload []byte) error {
	wr_msg := NewData(0, 0, len(payload), payload, 0)
	c.processInternal <- &internalMsg{mtype: Write, msg: wr_msg}
	resp := <-c.writeReturnChan
	if resp.err == nil {
		c.msgSendChan <- resp.msg
	}
	return resp.err
}

// TODO:
func (c *client) Close() error {
	for {
		c.processInternal <- &internalMsg{mtype: Close}
		select {
		case req := <-c.closeReturnChan:
			return req.err

		case msg := <-c.closeMsgChan:
			c.msgSendChan <- msg
		}
	}
}

func (c *client) main() {
	for {
		select {
		case <-c.returnMain:
			log.Printf("returning: main")
			return
		case msg := <-c.msgSendChan:
			log.Printf("send msg: %s\n", msg)
			switch c.state {
			// Connection pending...
			case Connect:
				// unAckedMsgs is init to have be a 1-sized window
				// The lower-bound on this window is currSeqNum
				// An Ack message has to match and remove MsgConnect
				// Only then, a connection is declared to be successful
				if msg.Type == MsgConnect {
					LB := c.currSeqNum
					UB := c.currSeqNum + 1
					mSz := 1
					c.unAckedMsgs.Reinit(LB, UB, mSz)
					c.unAckedMsgs.Put(c.currSeqNum, msg)
					sendToServer(c.clientConn, msg, true)
				}
			// Connection established!
			case Active:
				switch msg.Type {
				case MsgData:
					// Attempt to put inside sliding window
					// Send if putting inside the window was a success
					// Since, write increments currSeqNum
					// If data message dropped, currSeqNum is decremented to restore order
					// TODO: Resend messages in unAckedMsgs on epochFire acc to backoff rules
					if c.unAckedMsgs.In(msg.SeqNum) { // Retry Data Message
						sendToServer(c.clientConn, msg, true)
					} else { // New Data Message
						if c.unAckedMsgs.Put(msg.SeqNum, msg) {
							sendToServer(c.clientConn, msg, true)
						} else { // Dropped, so restore
							c.currSeqNum--
						}
					}

				case MsgAck, MsgCAck:
					// TODO  : Handle client heartbeat
					// Maybe : nothing special?
					if msg.SeqNum == 0 {
						log.Println("client heartbeat!")
					}
					sendToServer(c.clientConn, msg, true)
				}
			case Closing:
				if msg.Type == MsgData {
					sendToServer(c.clientConn, msg, true)
				}
			}

		case msg := <-c.msgRecvChan:
			log.Printf("recv msg: %s\n", msg)
			switch c.state {
			case Connect:
				// If valid connID
				// Resize sliding window
				// Change client state
				// Signal NewClient to return
				if msg.Type == MsgAck && msg.ConnID != 0 {
					_, exist := c.unAckedMsgs.Remove(msg.SeqNum)
					if exist {
						c.connID = msg.ConnID
						c.state = Active
						LB := c.currSeqNum + 1
						UB := LB + c.params.WindowSize
						mSz := c.params.MaxUnackedMessages
						c.unAckedMsgs.Reinit(LB, UB, mSz)
						c.returnNewClient <- 1
					}
				}
			case Active:
				switch msg.Type {
				case MsgData:
					// Put in priorityQueue
					// TODO: handle duplicates based on currSeqNum + window ?
					c.pendingRead.Insert(msg)
					if c.processRead {
						pqmsg, exist := c.pendingRead.GetMin()
						if exist && pqmsg.SeqNum == c.currSeqNum {
							_, err := c.pendingRead.RemoveMin()
							c.readReturnChan <- &internalMsg{mtype: Read, msg: pqmsg, err: err}
							c.processRead = false
						}
					}
				case MsgAck, MsgCAck:
					// TODO  : Handle server heartbeat
					// TODO  : Reset the EpochLimit timer
					// TODO  : By sending to a channel in Timer
					if msg.SeqNum == 0 {
						c.counter = 0 // Sent
					} else {
						c.unAckedMsgs.Remove(msg.SeqNum)
					}
				}
			case Closing:
				if msg.Type == MsgAck || msg.Type == MsgCAck {
					// TODO  : Handle server heartbeat
					// TODO  : Reset the EpochLimit timer
					// TODO  : By sending to a channel in Timer
					if msg.SeqNum == 0 {
						c.counter = 0 // Sent
					} else {
						c.unAckedMsgs.Remove(msg.SeqNum)
					}
				}
			}
		case req := <-c.processInternal:
			switch req.mtype {
			case ID:
				c.connIDReturnChan <- &internalMsg{mtype: ID, id: c.connID}

			case Read:
				c.processRead = true

			case Write:
				// Assign checksum, connID, seqNum to a data msg within request
				// Assign err (if any)(TODO: Handle non-nil error on lost connection)
				// Then send back to Write
				c.currSeqNum++
				validateWriteInternal(c, req)
				c.writeReturnChan <- req

			case Close:
				// TODO: While theres unAckd's messages
				// TODO: Get message with the lowest sn
				// TODO: Send data msgs to the closeMsgChan
				// TODO: closeMsgChan in Close forwards it to msgSendChan (new Close State required ?)
				// TODO: Acks recieved in msgRecvChan will remove the items (new Close State required ?)
				// TODO: Then, Close will send another InternalRequest
				// TODO: Only when unAckedMsgs is empty or connection has been lost,
				// TODO: closeReturnChan <- 1 will prompt close to return
				c.state = Closing
				msg, exist := c.unAckedMsgs.GetMinMsg()
				if exist {
					c.closeMsgChan <- msg
				} else {
					c.closeReturnChan <- &internalMsg{mtype: Close, err: nil}
				}
			}
			/*
				// TODO: Epoch Timer
				case <-c.updateCounter:
					c.counter++
					log.Printf("Epoch Tick!")
					if c.counter == c.params.EpochLimit {
						log.Printf("EpochLimit!")
						c.returnAll <- 1
					}
			*/
		}
	}
}

func (c *client) reader() {
	for {
		select {
		// handle termination due to server Close
		case <-c.returnReader:
			log.Printf("returning: reader")
			return
		default:
			var msg Message
			err := recvFromServer(c.clientConn, &msg, true)
			if err == nil {
				c.msgRecvChan <- &msg
			}
		}
	}
}

func (c *client) epochTimer() {
	timer := time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
	for {
		select {
		case <-c.returnTimer:
			log.Printf("returning: timer")
			return
		case <-timer.C: // Epoch tick
			c.updateCounter <- 1
		}
	}
}
