// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
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

type internalMsg struct {
	mtype   InternalType
	payload []byte
	err     error
	id      int
	msg     *Message
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
	processRead      bool
	readReturnChan   chan *internalMsg
	connIDReturnChan chan *internalMsg
	writeReturnChan  chan *internalMsg
	closeReturnChan  chan *internalMsg
	processInternal  chan *internalMsg

	// TODO: timing
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
	log.Printf("Client ConnID\n")
	c.processInternal <- &internalMsg{mtype: ID}
	req := <-c.connIDReturnChan
	return req.id
}

func (c *client) Read() ([]byte, error) {
	log.Printf("Client Read\n")
	c.processInternal <- &internalMsg{mtype: Read}
	req := <-c.readReturnChan
	c.msgSendChan <- NewAck(c.connID, req.msg.SeqNum)
	return req.msg.Payload, req.err
}

func (c *client) Write(payload []byte) error {
	log.Printf("Client Write\n")
	c.processInternal <- &internalMsg{mtype: Write, payload: payload}
	resp := <-c.writeReturnChan
	c.msgSendChan <- resp.msg
	return resp.err
}

func (c *client) Close() error {
	c.processInternal <- &internalMsg{mtype: Close}
	req := <-c.closeReturnChan
	return req.err
}

func (c *client) main() {
	for {
		select {
		case <-c.returnAll:
			return
		case msg := <-c.msgSendChan:
			log.Printf("to send msg: %s\n", msg)
			switch c.state {
			case Connect:
				// Only message that can be sent is MsgConnect
				// Only the first MsgConnect will be put in sWin
				// Subsequent calls with not put anything since sWin maxSize = 1
				// When an Ack message matches and removes the MsgConnect
				// the connection is declared to be successful
				if msg.Type == MsgConnect {
					c.sWin.Reinit(msg.SeqNum, msg.SeqNum+1, 1)
					c.sWin.Put(msg.SeqNum, msg)
					sendToServer(c.clientConn, msg)
				}

			case Active:
				// Once a connection is successful
				// Attempt to put inside sliding window
				// send if not dropped
				// Ack, CAcks get sent directly
				switch msg.Type {
				case MsgData:
					log.Printf("sliding window: %s\n", c.sWin.String())
					if c.sWin.Put(msg.SeqNum, msg) {
						sendToServer(c.clientConn, msg)
						log.Printf("sent\n")
					} else {
						log.Printf("dropped\n")
					}
				case MsgAck, MsgCAck:
					// TODO
					if msg.SeqNum == 0 {
						log.Println("client heartbeat!")
					}
					sendToServer(c.clientConn, msg)
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
					_, exist := c.sWin.Remove(msg.SeqNum)
					if exist {
						c.connID = msg.ConnID
						c.state = Active
						log.Printf("Active sn: %d\n", c.currSeqNum)
						c.sWin.Reinit(c.currSeqNum+1, c.currSeqNum+c.params.WindowSize+1, c.params.MaxUnackedMessages)
						c.returnNewClient <- 1
					}
				}
			case Active:
				switch msg.Type {
				case MsgData:
					// attempt to put in priorityQueue
					// send Ack to msgChan
					//c.msgSendChan <- NewAck(c.connID, msg.SeqNum)
					c.pQueue.Insert(msg)
					if c.processRead {
						msg, err := c.pQueue.GetMin()
						log.Printf("pq msg: %s \n", msg)
						if err == nil && msg.SeqNum == c.currSeqNum {
							c.pQueue.RemoveMin()
							c.readReturnChan <- &internalMsg{mtype: Read, msg: msg, err: err}
							c.processRead = false
						}
					}
				case MsgAck, MsgCAck:
					if msg.SeqNum == 0 {
						// Heartbeat
						c.counter = 0
					} else {
						// Data Ack, CAck
						c.sWin.Remove(msg.SeqNum)
						log.Printf("sliding window: %s\n", c.sWin.String())
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
				// TODO: Handle non-nil error on lost connection
				c.currSeqNum++
				fmt.Printf("currSeqNum: %d\n", c.currSeqNum)
				checksum := CalculateChecksum(c.connID, c.currSeqNum, len(req.payload), req.payload)
				msg := NewData(c.connID, c.currSeqNum, len(req.payload), req.payload, checksum)
				c.writeReturnChan <- &internalMsg{mtype: Write, err: nil, msg: msg}

			case Close:
			}
			/*
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
			*/
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
		}
	}
}
