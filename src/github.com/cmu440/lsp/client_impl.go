// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/cmu440/lspnet"
)

type ClientState int

const (
	Connect ClientState = iota // Connect State
	Active                     // Active State
)

type InternalType int

const (
	Read InternalType = iota
	Write
	ID
	Close
)

type internalRequest struct {
	rType   InternalType
	payload []byte
	err     error
	id      int
}

type client struct {
	// internal state
	state      ClientState
	serverAddr *lspnet.UDPAddr
	clientConn *lspnet.UDPConn
	params     *Params
	connID     int
	currSeqNum int

	// cleanup
	returnNewClient chan int
	returnAll       chan int

	// Read | Write | ConnID
	processInternal chan *internalRequest
	getInternal     chan *internalRequest

	// timing
	counter       int
	updateCounter chan int

	// msg send/recv
	msgSendChan chan *Message
	msgRecvChan chan *Message

	// internal data structures
	sWin   *sWindowMap
	pQueue *priorityQueue
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

		returnNewClient: make(chan int),
		returnAll:       make(chan int),

		processInternal: make(chan *internalRequest),
		getInternal:     make(chan *internalRequest),

		counter:       0,
		updateCounter: make(chan int),

		msgSendChan: make(chan *Message),
		msgRecvChan: make(chan *Message),

		sWin:   NewSWM(0, 1, 1),
		pQueue: NewPQ(),
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
	case <-c.returnAll:
		return nil, errors.New("connection could not be established")
	case <-c.returnNewClient:
		log.Println("Success: Connected!")
		return c, nil
	}
}

func (c *client) ConnID() int {
	c.processInternal <- &internalRequest{rType: ID}
	req := <-c.getInternal
	return req.id
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	c.processInternal <- &internalRequest{rType: Read}
	req := <-c.getInternal
	return req.payload, req.err
}

func (c *client) Write(payload []byte) error {
	c.processInternal <- &internalRequest{rType: Write}
	req := <-c.getInternal
	return req.err
}

func (c *client) Close() error {
	c.processInternal <- &internalRequest{rType: Close}
	req := <-c.getInternal
	return req.err
}

func (c *client) main() {
	for {
		select {
		case <-c.returnAll:
			return
		case msg := <-c.msgSendChan:
			log.Printf("to send msg: %s| state: %s\n", cStateString(c.state), msg)
			switch c.state {
			case Connect:
				// Only message that can be sent is MsgConnect
				// Only the first MsgConnect will be put in sWin
				// Subsequent calls with not put anything since sWin maxSize = 1
				// When an Ack message matches and removes the MsgConnect
				// the connection is declared to be successful
				if msg.Type == MsgConnect {
					c.sWin.Put(msg.SeqNum, msg)
					sendToServer(c.clientConn, msg)
					log.Printf("sent connect..\n")
				}

			case Active:
				// Once a connection is successful
				// Attempt to put inside sliding window
				// send if not dropped
				// Ack, CAcks get sent directly
				switch msg.Type {
				case MsgData:
					if c.sWin.Put(msg.SeqNum, msg) {
						sendToServer(c.clientConn, msg)
						log.Printf("sent\n")
					} else {
						log.Printf("dropped\n")
						log.Printf("sliding window: %s\n", c.sWin.String())
					}
				case MsgAck, MsgCAck:
					if msg.SeqNum == 0 {
						log.Println("client heartbeat!")
					}
					sendToServer(c.clientConn, msg)
				}
			}

		case msg := <-c.msgRecvChan:
			log.Printf("recv msg: %s | state: %s\n", cStateString(c.state), msg)
			switch c.state {
			case Connect:
				// if valid connID
				// resize sliding window
				// change client state
				// signal NewClient to return
				if msg.Type == MsgAck && msg.ConnID != 0 {
					_, exist := c.sWin.Remove(msg.SeqNum)
					if exist {
						c.connID = msg.ConnID
						c.state = Active
						c.sWin.Reinit(1, c.params.WindowSize+1, c.params.MaxUnackedMessages)
						c.returnNewClient <- 1
					}
				}

			case Active:
				switch msg.Type {
				case MsgData:
					// attempt to put in sWin
					// if successful, send Ack to msgChan
					log.Println("Unhandled: MsgData")
				case MsgAck, MsgCAck:
					if msg.SeqNum == 0 {
						// Heartbeat
						log.Println("server heartbeat!")
						c.counter = 0
					} else {
						// Data Ack, CAck
						log.Println("data ack/cack!")
					}
				}
			}

		// Epoch Timer
		case <-c.updateCounter:
			c.counter++
			log.Printf("Epoch Tick!")
			if c.counter == c.params.EpochLimit {
				log.Printf("EpochLimit!")
				c.returnAll <- 1
			}
			// TODO: First prioritize sending un Ack'd data messages
			// If sWin is empty, then send set sentHeartbeat to true
			// Heartbeat
			switch c.state {
			case Connect:
				// An Ack to the connect request would:
				// remove the connect message from sWin
				// and init sWin (see Connect case of msgRecvChan)
				// so, Is sWin is not empty, resend connection request
				//if !c.sWin.Empty() {
				//	c.msgSendChan <- NewConnect(c.currSeqNum)
				//}
			case Active:
			}
		}
	}
}

func (c *client) reader() {
	for {
		select {
		// handle termination due to server Close
		case <-c.returnAll:
			return
		default:
			buf := make([]byte, 2000)
			n, err := c.clientConn.Read(buf)
			if err == nil {
				var msg Message
				json.Unmarshal(buf[:n], &msg)
				// Check integrity
				if checkIntegrity(&msg) {
					c.msgRecvChan <- &msg
					log.Printf("read msg: %s | state: %s\n", cStateString(c.state), &msg)
				}
			}
		}
	}
}

func (c *client) epochTimer() {
	timer := time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
	for {
		select {
		case <-c.returnAll:
			return
		case <-timer.C: // Epoch tick
			c.updateCounter <- 1
			if c.sWin.Empty() { // If no unacknowledged messages, send heartbeat
				c.sendHeartbeat()
			}
		}
	}
}

func (c *client) sendHeartbeat() {
	select {
	case c.msgSendChan <- NewAck(c.connID, 0): // Heartbeat message
		log.Println("Sent heartbeat")
	default:
		log.Println("Skipped heartbeat, msgSendChan is full")
	}
}
