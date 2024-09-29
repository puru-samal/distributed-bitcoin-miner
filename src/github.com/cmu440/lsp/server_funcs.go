package lsp

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/cmu440/lspnet"
)

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
		log.Println(err)
	}
	LB := clientMsg.message.SeqNum + 1
	UB := LB + s.params.WindowSize
	mSz := s.params.MaxUnackedMessages
	newClient.unAckedMsgs.Reinit(LB, UB, mSz)

	log.Println("[Check Connection] newClient.unAckedMsgs: ", newClient.unAckedMsgs.LB, newClient.unAckedMsgs.UB, newClient.unAckedMsgs.maxSize)

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
		log.Println("[DataHandler] Error sending ack: ", err)
	}

	client.hasSentData = true
}

func (s *server) AckHandler(clientMsg *clientMessage, connID int, closing bool) bool {
	acknowledged := false
	if clientMsg.message.SeqNum == 0 {
		return acknowledged
	}
	client := s.clientInfo[connID]
	_, exist := client.unAckedMsgs.Remove(clientMsg.message.SeqNum)
	log.Println("[AckHandler] client.unAckedMsgs: ", client.unAckedMsgs.LB, client.unAckedMsgs.UB, client.unAckedMsgs.maxSize)
	if !exist {
		return acknowledged
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
	return
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

func (s *server) defaultActions() {
	for _, client := range s.clientInfo {
		if !client.pendingMsgs.Empty() {
			item, _ := client.pendingMsgs.RemoveMin()
			if client.isValidMessage(item.SeqNum) || client.unAckedMsgs.Empty() {
				err := s.sendMessage(item, client.addr)
				if err != nil {
					log.Println(err)
				}
				client.unAckedMsgs.Put(item.SeqNum, item)
				//log.Println("[defaultActions] client.unAckedMsgs: ", len(client.unAckedMsgs.mp), client.unAckedMsgs.maxSize)
			} else {
				client.pendingMsgs.Insert(item)
			}
		}
	}
}

func (s *server) resendUnAckedMessages() {
	for _, client := range s.clientInfo {
		for _, item := range client.unAckedMsgs.mp {
			if item.unAckedCounter == item.currBackoff {
				err := s.sendMessage(item.msg, client.addr)
				if err != nil {
					log.Println(err)
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
