package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/cmu440/lspnet"
)

type internalMsg struct {
	mtype InternalType
	err   error
	id    int
	msg   *Message
}

func cLog(c *client, str string, lvl int) {
	if lvl <= c.logLvl {
		log.Print(str)
	}
}

// Helper funcs

func sendToServer(conn *lspnet.UDPConn, msg *Message) bool {
	byt, err := json.Marshal(msg)
	if err == nil {
		conn.Write(byt)
		return true
	}
	return false
}

func recvFromServer(conn *lspnet.UDPConn, msg *Message) error {
	buf := make([]byte, 2000)
	n, err := conn.Read(buf)
	if err == nil && checkIntegrity(msg) {
		json.Unmarshal(buf[:n], &msg)
		return nil
	}
	return err
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

// Sends NewConnect and inits client state before connect
// unAckedMsgs on init are 1-sized window with lowerbound currSeqNum
// An Ack message has to match and remove MsgConnect
// Only then, a connection is declared to be successful
func processSendConnect(c *client, msg *Message) bool {
	sent := false
	if !(c.state == Connect && msg.Type == MsgConnect) {
		return sent
	}
	LB := c.currSeqNum
	UB := c.currSeqNum + 1
	mSz := 1
	c.unAckedMsgs.Reinit(LB, UB, mSz)
	c.unAckedMsgs.Put(c.currSeqNum, msg)
	sent = sendToServer(c.clientConn, msg)
	return sent
}

// Only Active || Closing state can send data messages
// If message has not been Ack'd, send
// If message is new, new messages are dropped while Close has been called
//
//		 Attempt to put in sliding window
//	     if sucessful, send
//	     if dropped, restore currSeqNum state
//
// TODO: Resend messages in unAckedMsgs on epochFire acc to backoff rules
func processSendData(c *client, msg *Message) bool {
	sent := false
	if !(c.state == Active || c.state == Closing) {
		return sent
	}
	isUnAcked := c.unAckedMsgs.In(msg.SeqNum)
	if isUnAcked { // Old Message
		sent = sendToServer(c.clientConn, msg)
		cLog(c, fmt.Sprintf("retry msg sent: %s\n", msg), 4)
		c.processRetry <- 1
		cLog(c, "retry signaled", 4)
	} else { // New Data Message

		if c.state == Closing {
			return sent
		}
		isValid := c.unAckedMsgs.Put(msg.SeqNum, msg)
		if isValid {
			sent = sendToServer(c.clientConn, msg)
		} else { // Dropped, so restore
			c.currSeqNum--
		}
	}
	return sent
}

func processSendAcks(c *client, msg *Message) bool {
	sent := false
	if !(c.state == Active || c.state == Closing) {
		return sent
	}
	sent = sendToServer(c.clientConn, msg)
	if msg.SeqNum == 0 {
		cLog(c, "client: heartbeat!", 2)
		c.processRetry <- 1
	}
	return sent
}

// Handles possible duplicates based on currSeqNum + window
// If in the valid range, puts in priorityQueue
// If read has been called, get the highest priority message
// If the message sn matches currSeqNum, remove from priority queue
// and send to read and mark read as having been processed
func processRecvData(c *client, msg *Message) {
	if c.state != Active {
		return
	}
	LB, UB := c.currSeqNum, c.currSeqNum+c.params.WindowSize
	if LB <= msg.SeqNum && msg.SeqNum < UB {
		cLog(c, fmt.Sprintf("pq insert msg: %s\n", msg), 3)
		c.pendingRead.Insert(msg)
		c.resetEp <- 1
	}
	if c.processRead {
		pqmsg, exist := c.pendingRead.GetMin()
		if exist == nil && pqmsg.SeqNum == c.currSeqNum {
			_, err := c.pendingRead.RemoveMin()
			cLog(c, fmt.Sprintf("pq rmMin msg: %s\n", msg), 3)
			c.readReturnChan <- &internalMsg{mtype: Read, msg: pqmsg, err: err}
			c.processRead = false
		}
	}
}

// Handle server heartbeat by resetting the eps and epsLimit timers
// For non-heartbeatAcks, matches msg sn with unAck'd msgs sn.
// Removes the message from the unAck's msgs sWin if match
func processRecvAcks(c *client, msg *Message) {
	if c.state == Connect {
		if msg.ConnID != 0 {
			_, exist := c.unAckedMsgs.Remove(msg.SeqNum)
			if exist {
				initClientAfterConnect(c, msg)
			}
		}
	} else {
		cLog(c, fmt.Sprintf("sWin rm on ack/cack - msg: %s\n", msg), 4)
		cLog(c, fmt.Sprintf("pre-sWin state: %s\n", c.unAckedMsgs.String()), 4)
		c.unAckedMsgs.Remove(msg.SeqNum)
		cLog(c, fmt.Sprintf("post-sWin state: %s\n", c.unAckedMsgs.String()), 4)
		if msg.SeqNum == 0 {
			cLog(c, "server heartbeat!", 4)
		}

	}
	cLog(c, "EpochLimit reset", 3)
	c.resetEp <- 1
}

// Handles client state after recieving an Ack for a NewConnect meddage
// Resize sliding window to [currSeqNum+1, currSeqNum+1+window) (for next data message)
// Change client state and signal NewClient to return
func initClientAfterConnect(c *client, msg *Message) {
	c.connID = msg.ConnID
	c.state = Active
	LB := c.currSeqNum + 1
	UB := LB + c.params.WindowSize
	mSz := c.params.MaxUnackedMessages
	c.unAckedMsgs.Reinit(LB, UB, mSz)
	c.returnNewClient <- 1
}

func validateWriteInternal(c *client, req *internalMsg) {
	if c.state == Lost || c.state == Closing {
		req.err = errors.New("Server connection lost")
	}
	checksum := CalculateChecksum(c.connID, c.currSeqNum, len(req.msg.Payload), req.msg.Payload)
	req.msg.ConnID = c.connID
	req.msg.SeqNum = c.currSeqNum
	req.msg.Checksum = checksum
	req.err = nil
}

func cStateString(state ClientState) string {
	switch state {
	case Connect:
		return "Connect"
	case Active:
		return "Active"
	}
	return ""
}
