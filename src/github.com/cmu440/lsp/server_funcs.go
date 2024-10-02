package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/cmu440/lspnet"
)

// logging function based on log level
func sLog(s *server, str string, lvl int) {
	if lvl == s.logLvl {
		log.Print(str)
	}
}

// marshal and send message to client
func (s *server) sendMessage(message *Message, addr *lspnet.UDPAddr) error {
	marshalMessage, err := json.Marshal(message)
	if err != nil {
		return err
	}
	_, err = s.conn.WriteToUDP(marshalMessage, addr)
	return err
}

// check if the connection is already established
// if not, establish a new connection by initializing the clientInfo and sending a newAck
func (s *server) checkConnection(clientMsg *clientMessage, clientAddr *lspnet.UDPAddr) bool {
	alreadyConnected := false

	for _, client := range s.clientInfo {
		if client.addr.String() == clientAddr.String() {
			alreadyConnected = true
			break
		}
	}

	if alreadyConnected {
		return true
	}

	newConnID := s.nextConnectionID
	newClient := &clientInfo{
		addr:            clientAddr,
		pendingPayload:  make(map[int][]byte),
		readSeqNum:      clientMsg.message.SeqNum + 1,
		writeSeqNum:     clientMsg.message.SeqNum,
		unAckedMsgs:     NewSWM(0, 1, 1),
		pendingMsgs:     NewPQ(),
		hasReceivedData: true,
		hasSentData:     true,
		isClosed:        false,
	}
	s.clientInfo[newConnID] = newClient

	newAck := NewAck(s.nextConnectionID, clientMsg.message.SeqNum)
	err := s.sendMessage(newAck, clientAddr)

	if err != nil {
		sLog(s, err.Error(), 2)
	} else {
		sLog(s, fmt.Sprintln("Sent newAck", newAck), 4)
	}

	LB := clientMsg.message.SeqNum + 1
	UB := LB + s.params.WindowSize
	mSz := s.params.MaxUnackedMessages
	newClient.unAckedMsgs.Reinit(LB, UB, mSz)
	newClient.writeSeqNum += 1
	s.nextConnectionID += 1

	return false
}

// Data handler
// check if the message is corrupted or not
// if not truncate the payload to message size and save it into the pendingPayload of the client for future read requests
// also send NewAck for the received message
func (s *server) processDataHandler(clientMsg *clientMessage, clientAddr *lspnet.UDPAddr, connID int) {
	payload := clientMsg.message.Payload
	// if the size of the received data is shorter than the given size included in the message
	// if the messages is corrupted
	checkSum := CalculateChecksum(connID, clientMsg.message.SeqNum, len(payload), payload)
	if clientMsg.message.Size > len(payload) || (clientMsg.message.Size == len(payload) && checkSum != clientMsg.message.Checksum) {
		client, ok := s.clientInfo[connID]
		if ok {
			client.hasReceivedData = true
			client.unReceivedNum = 0
		}
		return
	}
	// in case the size of the data is longer than the given size included in the message
	// truncate the payload to the given size
	clientMsg.message.Payload = clientMsg.message.Payload[:clientMsg.message.Size]

	client := s.clientInfo[connID]
	client.pendingPayload[clientMsg.message.SeqNum] = clientMsg.message.Payload

	ack := NewAck(connID, clientMsg.message.SeqNum)
	err := s.sendMessage(ack, clientAddr)

	if err != nil {
		sLog(s, err.Error(), 2)
	} else {
		sLog(s, fmt.Sprintln("[DataHandler] Sent Ack SeqNum", ack.SeqNum, "of", connID), 4)
	}
	client.hasSentData = true
}

// Acknowledgement handler
// remove the message from the unAckedMsgs of the client
// if the server is closing and there are no pending messages and unAcked messages, delete the client
// if there are no clients left, send a signal to the shutdownCompleteChan
func (s *server) processAckHandler(clientMsg *clientMessage, connID int, closing bool) bool {
	acknowledged := false

	// if it is a heartbeat message
	if clientMsg.message.SeqNum == 0 {
		return acknowledged
	}
	client := s.clientInfo[connID]
	_, success := client.unAckedMsgs.Remove(clientMsg.message.SeqNum)

	if success {
		sLog(s, fmt.Sprintln("Removed message from unAckedMsgs", clientMsg.message.SeqNum, "of", clientMsg.message.ConnID), 4)
		sLog(s, fmt.Sprintln("Remaining unAckedMsgs", len(client.unAckedMsgs.mp), "of", clientMsg.message.ConnID), 4)
		sLog(s, fmt.Sprintln("Remaining pendingMsgs", len(client.pendingMsgs.q), "of", clientMsg.message.ConnID), 4)
	}
	if !success {
		sLog(s, "[AckHandler] Error removing message from unAckedMsgs", 2)
	}

	if closing && client.pendingMsgs.Empty() && client.unAckedMsgs.Empty() {
		delete(s.clientInfo, connID)
		if len(s.clientInfo) == 0 {
			s.shutdownCompleteChan <- true
			acknowledged = true
		}
	}
	return acknowledged
}

// read request handler
// send readResponseChan to Read()
func (s *server) readRequest() {
	for id, client := range s.clientInfo {
		res, ok := client.pendingPayload[client.readSeqNum]
		// if the client is closed and there are no pending messages
		if client.isClosed && len(client.pendingPayload) == 0 {
			readRes := &readResponse{
				connID:  id,
				payload: nil,
			}
			s.readResponseChan <- readRes
			return
		} else if ok {
			readRes := &readResponse{
				connID:  id,
				payload: res,
			}
			delete(client.pendingPayload, client.readSeqNum)
			client.readSeqNum += 1
			s.readResponseChan <- readRes
			return
		}
	}
	readRes := &readResponse{
		connID:  -1,
		payload: nil,
	}
	s.readResponseChan <- readRes
}

// write request handler
// write request is added to the pendingMsgs of the client
func (s *server) writeRequest(writeMsg *clientWriteRequest) {
	client, ok := s.clientInfo[writeMsg.connID]
	if !ok || client.isClosed {
		s.writeResponseChan <- errors.New("connection not found")
	} else {
		checkSum := CalculateChecksum(writeMsg.connID, client.writeSeqNum, len(writeMsg.payload), writeMsg.payload)
		newDataMessage := NewData(writeMsg.connID, client.writeSeqNum, len(writeMsg.payload), writeMsg.payload, checkSum)

		isValid := client.unAckedMsgs.Put(newDataMessage.SeqNum, newDataMessage)
		if isValid {
			s.sendMessage(newDataMessage, client.addr)
			sLog(s, fmt.Sprintln("Sent new message: ", newDataMessage.SeqNum, "of", newDataMessage.ConnID), 4)
		} else {
			client.pendingMsgs.Insert(newDataMessage)
		}

		client.writeSeqNum += 1
		s.writeResponseChan <- nil
	}
}

// resend unacknowledged/dropped messages
// currBackoff: number of epochs we wait before resending the data (that did not receive ACK)
// maxBackOffInterval: maximum amount of epochs we wait w/o retrying to transmit the data
func (s *server) resendUAckedMessages() {
	for _, client := range s.clientInfo {

		retryMsgs, exist := client.unAckedMsgs.UpdateBackoffs(s.params.MaxBackOffInterval)
		if exist {
			for !retryMsgs.Empty() {
				msg, _ := retryMsgs.RemoveMin()
				err := s.sendMessage(msg, client.addr)

				if err != nil {
					sLog(s, err.Error(), 2)
				} else {
					sLog(s, fmt.Sprintln("Resent unAcked message: ", msg.SeqNum, "of", msg.ConnID), 4)
				}
			}
		}

		client.hasSentData = exist

	}
}

// detect when the client has lost connection (timeout)
// (1) if the client is connected and the number of epochs that the client hasn't sent
// any data exceeds the EpochLimit, close the connection
// (2) if the client is connected and the server has not sent any data message to it in the last epoch
// send a heartbeat message to the client
func (s *server) sendHeartbeatMessages() {
	for _, client := range s.clientInfo {
		if !client.hasReceivedData && !client.isClosed {
			client.unReceivedNum += 1
			// Close client due to EpochLimit
			if client.unReceivedNum >= s.params.EpochLimit {
				client.isClosed = true
			}
		}
		client.hasReceivedData = false
	}

	for connID, client := range s.clientInfo {
		if !client.hasSentData && !client.isClosed {
			s.sendMessage(NewAck(connID, 0), client.addr)
		}
		client.hasSentData = false
	}

}

// if the minimum sequence number of the unAckedMsgs of the client is less than or equal to the sequence number of the clientMsg
// remove the messages from the unAckedMsgs queue to update the lower bound of the sliding window
// if the server is closing and there are no pending messages and unAcked messages, delete the client
func (s *server) processCAckHandler(clientMsg *clientMessage, connID int, closing bool) bool {
	cacknowledged := false
	// if it is a heartbeat message
	if clientMsg.message.SeqNum == 0 {
		return cacknowledged
	}
	client := s.clientInfo[connID]
	for !client.unAckedMsgs.Empty() {
		if client.unAckedMsgs.MinKey() <= clientMsg.message.SeqNum {
			client.unAckedMsgs.Remove(client.unAckedMsgs.MinKey())
		} else {
			break
		}
	}
	if closing && client.pendingMsgs.Empty() && client.unAckedMsgs.Empty() {
		delete(s.clientInfo, connID)
		sLog(s, fmt.Sprintf("[Close Check] unack: %s\n", client.unAckedMsgs.String()), 4)
		sLog(s, fmt.Sprintf("[Close Check] pendM: %v\n", client.pendingMsgs.q), 4)
		if len(s.clientInfo) == 0 {
			s.shutdownCompleteChan <- true
			cacknowledged = true
		}
	}
	return cacknowledged
}
