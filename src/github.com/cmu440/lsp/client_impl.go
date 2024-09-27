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
	currSeqNum  int
	readSeqNum  int
	connLost    chan int
	pendingConn bool

	// Return signals
	returnNewClient chan int
	returnMain      chan int
	returnReader    chan int
	returnTimer     chan int
	returnRetry     chan int

	// Read | Write | ConnID
	processRead      bool
	processWrite     bool
	readReturnChan   chan *internalMsg
	connIDReturnChan chan *internalMsg
	writeReturnChan  chan *internalMsg
	closeMsgChan     chan *Message
	closeReturnChan  chan *internalMsg
	processInternal  chan *internalMsg
	processRetry     chan int

	// TODO: timing
	epFire    chan int
	epLimFire chan int
	resetEp   chan int

	// msg send/recv/retry
	msgSendChan  chan *Message
	msgRecvChan  chan *Message
	msgRetryChan chan *priorityQueue
	msgAckChan   chan *Message
	msgDataChan  chan *Message

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
		currSeqNum:  initialSeqNum,
		readSeqNum:  initialSeqNum + 1,
		connLost:    make(chan int),
		pendingConn: false,

		returnNewClient: make(chan int),
		returnMain:      make(chan int),
		returnReader:    make(chan int),
		returnTimer:     make(chan int),
		returnRetry:     make(chan int),

		processRead:      false,
		processWrite:     false,
		readReturnChan:   make(chan *internalMsg),
		writeReturnChan:  make(chan *internalMsg),
		connIDReturnChan: make(chan *internalMsg),
		closeReturnChan:  make(chan *internalMsg),
		processInternal:  make(chan *internalMsg),
		processRetry:     make(chan int),

		epFire:    make(chan int),
		epLimFire: make(chan int),
		resetEp:   make(chan int),

		msgSendChan:  make(chan *Message),
		msgRecvChan:  make(chan *Message),
		msgRetryChan: make(chan *priorityQueue),
		msgAckChan:   make(chan *Message),
		msgDataChan:  make(chan *Message),

		unAckedMsgs:  NewSWM(0, 1, 1),
		pendingRead:  NewPQ(),
		pendingWrite: NewPQ(),

		logLvl: 0,
	}

	// Launch Main Routine
	// Launch Read Routine
	// Launch Epoch Timer
	go c.main()
	go c.reader()
	go c.timer()
	go c.redirect()

	// Signal main to send NewConnect message
	// Block until connID set or EpochLimit reached
	c.msgSendChan <- NewConnect(initialSeqNum)
	select {
	case <-c.connLost:
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
	cLog(c, "Client connid!", 1)
	c.processInternal <- &internalMsg{mtype: ID}
	req := <-c.connIDReturnChan
	return req.id
}

// Sends an internalMsg to Main
// Main blocks until the data message with sn:currSeqNum is available
// Once available, Read returns
func (c *client) Read() ([]byte, error) {
	cLog(c, "Client read!", 1)
	c.processInternal <- &internalMsg{mtype: Read}
	resp := <-c.readReturnChan
	cLog(c, "Client returning read!", 1)
	return resp.msg.Payload, resp.err
}

// Creates and sends a data message with 0 id, sn, checksum
// Main validates (assigns id, sn, checksum, errors) and sends the the message back to Write
// Write then sends the message to it to Main if there's no errors
func (c *client) Write(payload []byte) error {
	cLog(c, "Client write!", 1)
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
	cLog(c, "Client close!", 1)
	for {
		c.processInternal <- &internalMsg{mtype: Close}
		select {
		case req := <-c.closeReturnChan:
			cLog(c, "close: pending msgs processed, returning.", 1)
			c.returnMain <- 1
			c.returnReader <- 1
			c.returnTimer <- 1
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
			cLog(c, "returning: main", 1)
			return
		case msg := <-c.msgSendChan:
			sWin1 := c.unAckedMsgs.String()
			var sent bool
			switch msg.Type {
			case MsgConnect:
				sent = processSendConnect(c, msg)
			case MsgData:
				sent = processSendData(c, msg)
			case MsgAck, MsgCAck:
				sent = processSendAcks(c, msg)
			}
			if sent {
				cLog(c, fmt.Sprintf("send msg: %s | sn: %d\n", msg, c.currSeqNum), 2)
			} else {
				cLog(c, fmt.Sprintf("dropped msg: %s | sn: %d\n", msg, c.currSeqNum), 2)
			}
			sWin2 := c.unAckedMsgs.String()
			if msg.Type == MsgData {
				cLog(c, fmt.Sprintf("send: sWin: %s -> %s\n", sWin1, sWin2), 2)
			}
		case msg := <-c.msgRecvChan:
			cLog(c, fmt.Sprintf("recv'd msg: %s | rsn: %d\n", msg, c.readSeqNum), 2)
			sWin1 := c.unAckedMsgs.String()
			switch msg.Type {
			case MsgData:
				processRecvData(c, msg)
			case MsgAck, MsgCAck:
				processRecvAcks(c, msg)
			}
			sWin2 := c.unAckedMsgs.String()
			if msg.Type == MsgAck && msg.SeqNum != 0 {
				cLog(c, fmt.Sprintf("recv: sWin: %s -> %s\n", sWin1, sWin2), 2)
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
				c.processWrite = true
				c.currSeqNum++
				cLog(c, fmt.Sprintf("pre-validated: %s\n", req.msg), 4)
				validateWriteInternal(c, req)
				cLog(c, fmt.Sprintf("post-validated: %s\n", req.msg), 4)
				if c.state == Closing {
					req.err = errors.New("client closed")
				}
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
					c.clientConn.Close()
					c.closeReturnChan <- &internalMsg{mtype: Close, err: nil}
				}
			}
		case <-c.epFire:
			cLog(c, "Epoch Fire!", 2)
			if c.state == Connect {
				PQ := NewPQ()
				PQ.Insert(NewConnect(c.currSeqNum))
				c.pendingConn = true
				c.msgRetryChan <- PQ
			} else {
				retryMsgs, exist := c.unAckedMsgs.UpdateBackoffs(c.params.MaxBackOffInterval)
				if exist {
					cLog(c, fmt.Sprintf("unacked pq: %v\n", retryMsgs.q), 2)
					c.msgRetryChan <- retryMsgs
				} else {
					PQ := NewPQ()
					PQ.Insert(NewAck(c.connID, 0))
					c.msgRetryChan <- PQ
				}
			}

		case <-c.epLimFire:
			// TODO:
			cLog(c, "EpochLimit Fire!", 3)

		}
	}
}

func (c *client) reader() {
	for {
		select {
		// handle termination due to server Close
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

		case <-c.resetEp:
			epochLimitTimer.Reset(time.Duration(c.params.EpochMillis*c.params.EpochLimit) * time.Millisecond)
		}
	}
}

func (c *client) redirect() {
	for {
		select {
		case <-c.returnRetry:
			cLog(c, "returning: redirect", 3)
			return
		case pq := <-c.msgRetryChan:
			cLog(c, fmt.Sprintf("recv retry queue: %v\n", pq.q), 3)
			for !pq.Empty() {
				msg, _ := pq.RemoveMin()
				c.msgSendChan <- msg
				<-c.processRetry
			}
		case msg := <-c.msgAckChan:
			c.msgSendChan <- msg
		case msg := <-c.msgDataChan:
			c.msgSendChan <- msg
		}
	}
}
