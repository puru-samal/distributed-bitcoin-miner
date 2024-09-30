package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/cmu440/lspnet"
)

func sLog(s *server, str string, lvl int) {
	if lvl == s.logLvl {
		log.Print(str)
	}
}

func (s *server) sendMessage(message *Message, addr *lspnet.UDPAddr) error {
	marshalMessage, err := json.Marshal(message)
	if err != nil {
		return err
	}
	_, err = s.conn.WriteToUDP(marshalMessage, addr)
	return err
}

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
		closed:          false,
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

func (s *server) DataHandler(clientMsg *clientMessage, clientAddr *lspnet.UDPAddr, connID int) {
	payload := clientMsg.message.Payload
	checkSum := CalculateChecksum(connID, clientMsg.message.SeqNum, len(payload), payload)
	if clientMsg.message.Size > len(payload) || (clientMsg.message.Size == len(payload) && checkSum != clientMsg.message.Checksum) {
		client, ok := s.clientInfo[connID]
		if ok {
			client.hasReceivedData = true
			client.unReceivedNum = 0
		}
		return
	}
	// truncate the payload
	clientMsg.message.Payload = clientMsg.message.Payload[:clientMsg.message.Size]

	client := s.clientInfo[connID]
	client.pendingPayload[clientMsg.message.SeqNum] = clientMsg.message.Payload

	ack := NewAck(connID, clientMsg.message.SeqNum)
	err := s.sendMessage(ack, clientAddr)

	if err != nil {
		sLog(s, err.Error(), 2)
	} else {
		sLog(s, fmt.Sprintln("[DataHandler] Sent SeqNum", ack.SeqNum, "of", connID), 4)
	}
	client.hasSentData = true
}

func (s *server) AckHandler(clientMsg *clientMessage, connID int, closing bool) bool {
	acknowledged := false
	if clientMsg.message.SeqNum == 0 {
		return acknowledged
	}
	client := s.clientInfo[connID]
	_, success := client.unAckedMsgs.Remove(clientMsg.message.SeqNum)

	if success {
		sLog(s, fmt.Sprintln("Removed message from unAckedMsgs", clientMsg.message.SeqNum, "of", clientMsg.message.ConnID), 4)
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

func (s *server) readRequest() {
	for id, client := range s.clientInfo {
		res, ok := client.pendingPayload[client.readSeqNum]

		if client.closed && len(client.pendingPayload) == 0 {
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

func (s *server) writeRequest(writeMsg *clientWriteRequest) {
	client, ok := s.clientInfo[writeMsg.connID]
	if !ok || client.closed {
		s.writeResponseChan <- errors.New("connection not found")
	} else {
		checkSum := CalculateChecksum(writeMsg.connID, client.writeSeqNum, len(writeMsg.payload), writeMsg.payload)
		newDataMessage := NewData(writeMsg.connID, client.writeSeqNum, len(writeMsg.payload), writeMsg.payload, checkSum)
		client.pendingMsgs.Insert(newDataMessage)

		client.writeSeqNum += 1
		s.writeResponseChan <- nil
	}
}

// func (s *server) defaultActions() {
// 	for _, client := range s.clientInfo {
// 		if client.pendingMsgs.Size() > 0 {
// 			msg, _ := client.pendingMsgs.RemoveMin()
// 			if client.isValidMessage(msg.SeqNum) || client.unAckedMsgs.Empty() {
// 				err := s.sendMessage(msg, client.addr)
// 				if err != nil {
// 					log.Println(err)
// 				}
// 				insertFail := client.unAckedMsgs.Put(msg.SeqNum, msg)
// 				if !insertFail {
// 					sLog(s, "[defaultActions] Error inserting into unAckedMsgs", 2)
// 				} else {
// 					sLog(s, fmt.Sprintln("[defaultActions] Inserted into unAckedMsgs: ", msg.SeqNum), 4)
// 				}
// 			} else {
// 				client.pendingMsgs.Insert(msg)
// 			}
// 		}
// 	}
// }

func (s *server) resendUnAckedMessages() {
	for _, client := range s.clientInfo {
		for _, item := range client.unAckedMsgs.mp {
			if item.unAckedCounter == item.currBackoff {
				msg, _ := client.unAckedMsgs.Get(item.msg.SeqNum)
				err := s.sendMessage(msg, client.addr)
				if err != nil {
					sLog(s, err.Error(), 2)
				} else {
					sLog(s, fmt.Sprintln("Resent unAcked message: ", msg.SeqNum, "of", msg.ConnID), 4)
				}
				client.hasSentData = true
				if item.currBackoff == 0 {
					item.currBackoff = 1
				} else {
					item.currBackoff = item.currBackoff * 2
				}
				item.unAckedCounter = 0
				item.currBackoff = min(item.currBackoff, s.params.MaxBackOffInterval)
			} else {
				item.unAckedCounter += 1
			}
		}

	}
}

func (s *server) sendHeartbeatMessages() {
	for _, client := range s.clientInfo {
		if !client.hasReceivedData && !client.closed {
			client.unReceivedNum += 1
			if client.unReceivedNum >= s.params.EpochLimit {
				client.closed = true
			}
		}
		client.hasReceivedData = false
	}

	for connID, client := range s.clientInfo {
		if !client.hasSentData && !client.closed {
			s.sendMessage(NewAck(connID, 0), client.addr)
		}
		client.hasSentData = false
	}

}

func (s *server) CAckHandler(clientMsg *clientMessage, connID int, closing bool) bool {
	cacknowledged := false
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
		if len(s.clientInfo) == 0 {
			s.shutdownCompleteChan <- true
			cacknowledged = true
		}
	}
	return cacknowledged
}
