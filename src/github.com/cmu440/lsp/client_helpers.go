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

// General Utility Helpers

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
	if err == nil {
		json.Unmarshal(buf[:n], &msg)
		return nil
	}
	return err
}

func checkIntegrity(msg *Message) bool {
	if len(msg.Payload) < msg.Size {
		return false
	}
	msg.Payload = msg.Payload[:msg.Size]
	checksum := CalculateChecksum(msg.ConnID, msg.SeqNum, msg.Size, msg.Payload)
	return msg.Checksum == checksum
}

func initClientAfterConnect(c *client, msg *Message) {
	c.connID = msg.ConnID
	c.state = Active
	LB := c.writeSeqNum + 1
	UB := LB + c.params.WindowSize
	mSz := min(c.params.MaxUnackedMessages, c.params.WindowSize)
	c.unAckedMsgs.Reinit(LB, UB, mSz)
	c.returnNewClient <- 1
}

// Assigns a ConnID, SeqNum, Checksum to a message sent my write request
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

func returnAll(c *client) {
	c.returnMain <- 1
	c.returnReader <- 1
	c.returnTimer <- 1
}

// Message Sending

// Sent a NewConnect Message
func processSendConnect(c *client, msg *Message) bool {
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

// Attempt to put in sliding window,
// If valid, send, else put in pendingWrite
func processSendNewData(c *client, msg *Message) bool {
	sent := false
	if !(c.state == Active) || (c.state == Closing) {
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

// Resend NewConnect to server
func processReSendConnect(c *client) bool {
	msg := NewConnect(c.writeSeqNum)
	sent := sendToServer(c.clientConn, msg)
	return sent
}

// Update backoffs and get a priority queue of messages to be send
// if priority queue is empty, send heartbeat instead
func processReSendDataOrHeartbeat(c *client) bool {
	sent := false
	if !(c.state == Active || c.state == Closing) {
		return sent
	}
	retryMsgs, exist := c.unAckedMsgs.UpdateBackoffs(c.params.MaxBackOffInterval)
	if exist {
		cLog(c, fmt.Sprintf("unacked pq: %v\n", retryMsgs.q), 2)
		for !retryMsgs.Empty() {
			msg, _ := retryMsgs.RemoveMin()
			sent = sendToServer(c.clientConn, msg)
			cLog(c, "retry processing..", 2)
		}
		cLog(c, "retry processed", 2)
	} else {
		heartbeat := NewAck(c.connID, 0)
		sent = processSendAcks(c, heartbeat)
	}
	return sent
}

// Just sent an ack to server
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

// Message Receiving

// Check data message's validity, and either
// Mark as toBeRead for processing when read is called
// Or put in pendingRead
// Reset epochLimit
func processRecvData(c *client, msg *Message) {
	if c.state != Active {
		return
	}

	LB, UB := c.readSeqNum, c.readSeqNum+c.params.WindowSize
	if LB <= msg.SeqNum && msg.SeqNum < UB {
		hasIntegrity := checkIntegrity(msg)
		if hasIntegrity {
			if msg.SeqNum == c.readSeqNum {
				cLog(c, fmt.Sprintf("set [toBeRead]: %s\n", msg), 2)
				c.toBeRead = &internalMsg{mtype: Read, msg: msg, err: nil}
			} else {
				c.pendingRead.Insert(msg)
			}
		}
	}

	ackMsg := NewAck(c.connID, msg.SeqNum)
	processSendAcks(c, ackMsg)
	c.resetEpLimit <- 1
}

// Connect : If valid ack for connect, init sliding window, signal NewClient to return
// Active  : If ack matches seqNum in sliding window => data has been recvd, remove from window
// Attempt to put the lowest priority item in pendingWrite into the sliding window
// Reset epochLimit
func processRecvAcks(c *client, msg *Message) {
	if c.state == Connect {
		if msg.ConnID != 0 {
			_, exist := c.unAckedMsgs.Remove(msg.SeqNum)
			if exist {
				initClientAfterConnect(c, msg)
			}
		}
	} else {
		if msg.SeqNum == 0 {
			cLog(c, "[HEARTBEAT: server]", 2)
		}
		c.unAckedMsgs.Remove(msg.SeqNum)
		pqmsg, exist := c.pendingWrite.GetMin()
		if exist == nil && c.unAckedMsgs.Put(pqmsg.SeqNum, pqmsg) {
			c.pendingWrite.RemoveMin()
			cLog(c, fmt.Sprintf("preparing (pending write): %s\n", pqmsg), 2)
		}

	}
	cLog(c, "EpochLimit reset", 3)
	c.resetEpLimit <- 1
}
