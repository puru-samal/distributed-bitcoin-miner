package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/cmu440/lspnet"
)

// Internal messages struct for passing data
// required for handling Read | ConnID | Write calls
type internalMsg struct {
	mtype InternalType
	err   error
	id    int
	msg   *Message
}

// A logging helper function
func cLog(c *client, str string, lvl int) {
	if lvl <= c.logLvl {
		log.Print(str)
	}
}

// condition for which read should return an error
func (c *client) readErrCond() bool {
	return (c.state == Lost && c.pendingRead.Empty() && c.unProcData.Empty() && c.toBeRead == nil) || (c.state == Closing)
}

// condition required for close to return
func (c *client) closeDoneCond() bool {
	return c.state == Closing && c.unAckedMsgs.Empty() && c.pendingWrite.Empty()
}

// function that sends a message to the server
func sendToServer(conn *lspnet.UDPConn, msg *Message) bool {
	byt, err := json.Marshal(msg)
	if err == nil {
		conn.Write(byt)
		return true
	}
	return false
}

// function that recv's a message from the server
func recvFromServer(conn *lspnet.UDPConn, msg *Message) error {
	buf := make([]byte, 2000)
	n, err := conn.Read(buf)
	if err == nil {
		json.Unmarshal(buf[:n], &msg)
		return nil
	}
	return err
}

// function that checks for both valid data size and checksum
func checkIntegrity(msg *Message) bool {
	if len(msg.Payload) < msg.Size {
		return false
	}
	msg.Payload = msg.Payload[:msg.Size]
	checksum := CalculateChecksum(msg.ConnID, msg.SeqNum, msg.Size, msg.Payload)
	return msg.Checksum == checksum
}

// function that inits client state after a connection has been established
func initClientAfterConnect(c *client, msg *Message) {
	c.connID = msg.ConnID
	c.state = Active
	LB := c.writeSeqNum + 1
	UB := LB + c.params.WindowSize
	mSz := c.params.MaxUnackedMessages
	c.unAckedMsgs.Reinit(LB, UB, mSz)
	rLB, rUB := c.readSeqNum, c.readSeqNum+c.params.WindowSize
	c.unProcData.Reinit(rLB, rUB, c.params.WindowSize)
	c.returnNewClient <- 1
}

// function that assigns a ConnID, SeqNum, Checksum to a message within a write request
func validateWriteInternal(c *client, req *internalMsg) {
	if c.state == Lost || c.state == Closing {
		req.err = errors.New("Server connection lost")
	}
	checksum := CalculateChecksum(c.connID, c.writeSeqNum, len(req.msg.Payload), req.msg.Payload)
	req.msg.ConnID = c.connID
	req.msg.SeqNum = c.writeSeqNum
	req.msg.Checksum = checksum
	req.err = nil
}

// function that returns all go routines
// used by NewClient if connection cant be established
func returnAll(c *client) {
	c.returnMain <- 1
	c.returnReader <- 1
}

// function that sends a initializes the SlidingWindow data structure
// during the connect state and puts a NewConnect message in.
func handleConnect(c *client, msg *Message) bool {
	sent := false
	if !(c.state == Connect) {
		return sent
	}
	LB := c.writeSeqNum
	UB := c.writeSeqNum + 1
	mSz := 1
	c.unAckedMsgs.Reinit(LB, UB, mSz)
	c.unAckedMsgs.Put(c.writeSeqNum, msg)
	sent = sendToServer(c.clientConn, msg)
	return sent
}

// function that sends a NewConnect to server
func processSendConnect(c *client) bool {
	msg := NewConnect(c.writeSeqNum)
	sent := sendToServer(c.clientConn, msg)
	return sent
}

// function that handles sending a data msg to the server
// first an attempt is made to Put a msg into sliding window
// if within bounds, send to server
// else put in pending pq for processing later
func processSendData(c *client, msg *Message) bool {
	sent := false
	if !(c.state == Active) || (c.state == Closing) {
		return sent
	}

	if c.state == Closing && c.unAckedMsgs.In(msg.SeqNum) {
		return sent
	}

	isValid := c.unAckedMsgs.Put(msg.SeqNum, msg)
	if isValid {
		sent = sendToServer(c.clientConn, msg)
	} else {
		c.pendingWrite.Insert(msg)
	}
	return sent
}

// function that processess epoch fires during the Active or Closing state
// updates backoffs of all the messages in unAckedMsgs
// if there are messages to be sent based on their currBackoff, send
// else send heartbeat instead
func processEpochFire(c *client) bool {
	isheartbeat := false
	if !(c.state == Active || c.state == Closing) {
		return isheartbeat
	}
	retryMsgs, exist := c.unAckedMsgs.UpdateBackoffs(c.params.MaxBackOffInterval)
	if exist {
		cLog(c, fmt.Sprintf("unacked pq: %v\n", retryMsgs.q), 3)
		for !retryMsgs.Empty() {
			msg, _ := retryMsgs.RemoveMin()
			sendToServer(c.clientConn, msg)
			cLog(c, fmt.Sprintf("[resent] %d\n", msg.SeqNum), 2)
		}
	} else {
		heartbeat := NewAck(c.connID, 0)
		isheartbeat = processSendAcks(c, heartbeat)
	}
	return isheartbeat
}

// function that just sends an Ack to the server
func processSendAcks(c *client, msg *Message) bool {
	sent := false
	if !(c.state == Active || c.state == Closing) {
		return sent
	}
	sent = sendToServer(c.clientConn, msg)
	if msg.SeqNum == 0 {
		cLog(c, "[HEARTBEAT: client]", 2)
	}
	return sent
}

// function that handles a recieved data message from the server
// (1) if the message has integrity, attempt to put in unProcData
// (2) if isValid, then Put was succesful => message is within window bounds
// (3) if the message sn matches read sn, mark it as the one to be read next (c.toBeRead)
// (2) else put in the pendingRead pq for processing later when the window shifts
// reset epochLimit
func processRecvData(c *client, msg *Message) {
	if c.state != Active {
		return
	}
	hasIntegrity := checkIntegrity(msg)
	if hasIntegrity {
		isValid := c.unProcData.Put(msg.SeqNum, msg)
		if isValid && msg.SeqNum == c.readSeqNum {
			if msg.SeqNum == c.readSeqNum {
				cLog(c, fmt.Sprintf("[toBeRead]: %d\n", msg.SeqNum), 2)
				c.toBeRead = &internalMsg{mtype: Read, msg: msg, err: nil}
			} else {
				cLog(c, fmt.Sprintf("[pendingRead]: %d\n", msg.SeqNum), 2)
			}
		} else {
			cLog(c, fmt.Sprintf("[OOB]: %d\n", msg.SeqNum), 2)
			c.pendingRead.Insert(msg)
		}
	}

	ackMsg := NewAck(c.connID, msg.SeqNum)
	processSendAcks(c, ackMsg)
	cLog(c, fmt.Sprintf("[DataAck]: %d\n", ackMsg.SeqNum), 2)
	cLog(c, "EpochLimit reset", 3)
	c.epLimitCounter = 0
}

// function that handles a recieved ack message from the server
// connect state: If valid ack for connect, init post-connect client state & signal NewClient to return
// of close condition met: stop timer, signal reader to return and signal close to return for cleanup
// other : If ack matches seqNum in sliding window => that data has been recv'd, remove from window
// if remove successful => bounds may have changed,  so attempt to put to as many items in the pendingWrite pq as possible
// reset epochLimit
func processRecvAcks(c *client, msg *Message) {
	if c.state == Connect {
		if msg.ConnID != 0 {
			_, exist := c.unAckedMsgs.Remove(msg.SeqNum)
			if exist {
				initClientAfterConnect(c, msg)
			}
		}
	} else if c.closeDoneCond() {
		c.epochTimer.Stop()
		close(c.returnReader)
		c.closeReturnChan <- &internalMsg{mtype: Close, err: nil}
	} else {

		if msg.SeqNum == 0 {
			cLog(c, "[HEARTBEAT: server]", 2)
		}
		_, success := c.unAckedMsgs.Remove(msg.SeqNum)
		if success {
			for {
				pqmsg, exist := c.pendingWrite.GetMin()
				if exist != nil {
					break
				}
				isValid := c.unAckedMsgs.Put(pqmsg.SeqNum, pqmsg)
				if !isValid {
					break
				}
				sendToServer(c.clientConn, pqmsg)
				c.pendingWrite.RemoveMin()
				cLog(c, fmt.Sprintf("[pendingWrite]: %d\n", pqmsg.SeqNum), 2)
			}
		}
	}
	cLog(c, "EpochLimit reset", 3)
	c.epLimitCounter = 0
}

// function that queues up the next message to be processed by read if available
// if c.toBeRead == nil, no pending message is available currently for processing, return
// c.toBeRead has just been read, remove from sliding window if it exists in there
// The sliding window bounds may have changed so attempt to put as many msgs pendingRead as possible
// Set the next c.toBeRead if it exists
func processRead(c *client) {
	if c.state == Active || (c.state == Lost && c.toBeRead != nil) {
		if c.toBeRead.msg == nil {
			return
		}

		cLog(c, fmt.Sprintf("[!Read] %d\n", c.toBeRead.msg.SeqNum), 1)
		c.unProcData.Remove(c.toBeRead.msg.SeqNum)
		for {
			pqmsg, exist := c.pendingRead.GetMin()
			if exist != nil {
				break
			}
			isValid := c.unProcData.Put(pqmsg.SeqNum, pqmsg)
			if !isValid {
				break
			}
			c.pendingRead.RemoveMin()
			cLog(c, fmt.Sprintf("[OOB] -> [pendingRead]: %d\n", pqmsg.SeqNum), 2)
		}

		c.toBeRead = nil
		c.readSeqNum++

		if rmsg, exist := c.unProcData.Get(c.readSeqNum); exist {
			c.unProcData.Remove(c.readSeqNum)
			c.toBeRead = &internalMsg{mtype: Read, msg: rmsg, err: nil}
			cLog(c, fmt.Sprintf("[toBeRead]: %d\n", rmsg.SeqNum), 2)
		}
	}
}

// function that handles client state when epoch limit is reached
// during connect: signal NewClient to return an error
// during other: connection has been lost, stop timer, signal reader to return
func processEpochLimit(c *client) {
	if c.state == Connect {
		c.connFailed <- 1
	} else {
		c.state = Lost
		c.epochTimer.Stop()
		close(c.returnReader)
	}
}
