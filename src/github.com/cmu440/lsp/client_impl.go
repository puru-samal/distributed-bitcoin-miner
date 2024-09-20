// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
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
	// general
	state      ClientState
	serverAddr *lspnet.UDPAddr
	clientConn *lspnet.UDPConn

	// NewClient
	closeNewClient chan bool

	// Read | Write | ConnID
	processInternal chan *internalRequest
	getInternal     chan *internalRequest

	connID        int
	getConnID     chan int
	params        *Params
	counter       int
	updateCounter chan int
	closeConn     chan int
	msgSendChan   chan *Message
	msgRecvChan   chan *Message
	msgMap        map[int]*Message // map of UnackedMessages
	mapLB         int              // Lower bound (inclusive)
	mapUB         int              // Upper bound (exclusive)
	maxSize       int              // initially 1 (Connect), then MaxUnackedMessages
	currSn        int
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
		fmt.Println("Error: serverAddr un-resolved")
		return nil, err
	}

	conn, err := lspnet.DialUDP("udp", nil, serverAddr)
	if err != nil {
		fmt.Println("Error: Could not connect")
		return nil, err
	}

	c := &client{
		state:           Connect,
		serverAddr:      serverAddr,
		clientConn:      conn,
		closeNewClient:  make(chan bool),
		processInternal: make(chan *internalRequest),
		connID:          0,
		getConnID:       make(chan int),
		params:          params,
		counter:         0,
		updateCounter:   make(chan int),
		closeConn:       make(chan int),
		msgSendChan:     make(chan *Message),
		msgRecvChan:     make(chan *Message),
		msgMap:          make(map[int]*Message),
		mapLB:           initialSeqNum,
		mapUB:           initialSeqNum + 1,
		maxSize:         1,
		currSn:          0,
	}

	// Launch Main Routine
	// Launch Read Routine
	// Launch Epoch Timer
	go c.main()
	go c.reader()
	go c.epochTimer()

	// Signal main to send NewConnect message
	// Block until connID recv or EpochLimit reached
	c.msgSendChan <- NewConnect(initialSeqNum)
	status := <-c.closeNewClient
	if !status {
		return nil, errors.New("connection could not be established")
	}
	fmt.Println("Success: Connected!")
	return c, nil
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
		case msg := <-c.msgSendChan:
			// Check if msg allowable by sliding window protocol
			// Check if mapSize < (1 (Connect) || MaxUnackedMessages)
			// ^ If yes send message,
			fmt.Printf("message to send: %s\n", msg)
			if c.mapLB <= msg.SeqNum && msg.SeqNum < c.mapUB {
				if len(c.msgMap) < c.maxSize {

					// Send message
					// If Connect request or Data message
					// Add to map tracking unAck's messages

					fmt.Printf("sending message: %s\n", msg)

					byt, err := json.Marshal(msg)
					if err == nil {
						c.clientConn.Write(byt)
					}

					switch msg.Type {
					case MsgConnect, MsgData:
						c.msgMap[msg.SeqNum] = msg
						c.currSn++
					default:
						fmt.Println("Unhandled: MsgAck, MsgCAck")
					}

				}
			}
		case msg := <-c.msgRecvChan:
			fmt.Printf("recv message: %s\n", msg)
			switch c.state {
			case Connect:
				if msg.ConnID != 0 {
					_, exist := c.msgMap[msg.SeqNum]
					if exist {
						delete(c.msgMap, msg.SeqNum)
						c.connID = msg.ConnID
						c.state = Active
						c.mapLB = c.mapLB + 1
						c.mapUB = c.mapLB + c.params.WindowSize
						c.maxSize = c.params.MaxUnackedMessages
						c.closeNewClient <- true
					}
				}

			case Active:
				switch msg.Type {
				case MsgData:
					// send Ack to msgChan
					fmt.Println("Unhandled: MsgData")
				case MsgAck, MsgCAck:
					//fmt.Println("Unhandled: MsgAck, MsgCAck")
					c.msgSendChan <- msg
				}
			}

		case <-c.updateCounter: // Get Epoch tick
			c.counter++
			fmt.Println(".")
			switch c.state {
			case Connect:
				// If epoch limit is hit, drop connection
				if c.counter == c.params.EpochLimit {
					c.closeNewClient <- false
				}
			case Active:
			}

		}
	}

}

func (c *client) reader() {
	for {
		select {
		// handle termination due to server Close
		case <-c.closeConn:
			return
		default:
			buf := make([]byte, 2000)
			n, err := c.clientConn.Read(buf)
			if err == nil {
				var msg Message
				json.Unmarshal(buf[:n], &msg)
				// Check integrity
				if checkIntegrity(&msg) {
					fmt.Printf("read message: %s\n", &msg)
					c.msgRecvChan <- &msg
				}
			}
		}
	}
}

func (c *client) epochTimer() {
	timer := time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
	for {
		select {
		case <-c.closeConn:
			return
		case <-timer.C: // Epoch tick
			c.updateCounter <- 1
		}
	}
}

func checkIntegrity(msg *Message) bool {
	switch msg.Type {
	case MsgData:
		if len(msg.Payload) < msg.Size {
			return false
		}
		msg.Payload = msg.Payload[:msg.Size]
		return msg.Checksum == CalculateChecksum(msg.ConnID, msg.SeqNum, msg.Size, msg.Payload)
	default:
		return true
	}
}

// Assert function
func assert(condition bool, message string) {
	if !condition {
		panic(message)
	}
}
