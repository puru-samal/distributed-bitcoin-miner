package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

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
		unAckedMsgs:     NewPQ(),
		pendingMsgs:     NewPQ(),
		hasReceivedData: true,
		hasSentData:     true,
		closed:          false,
	}
	s.clientInfo[newConnID] = newClient
	newAck := NewAck(s.nextConnectionID, clientMsg.message.SeqNum)
	err := s.sendMessage(newAck, clientAddr)

	fmt.Println("New connection")

	if err != nil {
		fmt.Println("Error sending message")
		fmt.Println(err)
	}
	newClient.writeSeqNum += 1
	s.nextConnectionID += 1

	return false
}

func (s *server) DataHandler(clientMsg *clientMessage, clientAddr *lspnet.UDPAddr, connID int) {
	payload := clientMsg.message.Payload
	checkSum := CalculateChecksum(connID, clientMsg.message.SeqNum, len(payload), payload)
	if clientMsg.message.Size > len(payload) || (clientMsg.message.Size == len(payload) && checkSum != clientMsg.message.Checksum) {
		if client, ok := s.clientInfo[connID]; ok {
			client.hasReceivedData = true
		}
		return
	}
	// truncate the payload
	clientMsg.message.Payload = clientMsg.message.Payload[:clientMsg.message.Size]

	fmt.Println("Data Received: ", clientMsg.message)

	client := s.clientInfo[connID]
	client.pendingPayload[clientMsg.message.SeqNum] = clientMsg.message.Payload
	ack := NewAck(connID, clientMsg.message.SeqNum)
	err := s.sendMessage(ack, clientAddr)
	if err != nil {
		log.Println(err)
	}
	client.hasSentData = true

}

func (s *server) AckHandler(clientMsg *clientMessage, connID int, closing bool) bool {
	acknowledged := false
	if clientMsg.message.SeqNum == 0 {
		return acknowledged
	}
	client := s.clientInfo[connID]
	exist := client.unAckedMsgs.Remove(clientMsg.message.SeqNum)
	if !exist {
		return acknowledged
	}
	if closing && len(client.pendingMsgs.q) == 0 && len(client.unAckedMsgs.q) == 0 {
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

		fmt.Println("New Data Message:", newDataMessage)
		client.pendingMsgs.Insert(newDataMessage)
		// // send pending messages to the client
		// for _, msg := range client.pendingMsgs {
		// 	err := s.sendMessage(msg, client.addr)
		// 	if err != nil {
		// 		fmt.Println(err)
		// 	}
		// }
		client.writeSeqNum += 1
		s.writeResponseChan <- nil
	}
}

func (c *clientInfo) validMessage(seqNum int, params *Params) bool {
	if len(c.unAckedMsgs.q) == 0 {
		return true
	}
	return false
}

func (s *server) defaultActions() {
	for _, client := range s.clientInfo {
		if len(client.pendingMsgs.q) > 0 {
			item, err := client.pendingMsgs.RemoveMin()
			if client.validMessage(item.SeqNum, s.params) && err == nil {
				err := s.sendMessage(item, client.addr)
				if err != nil {
					log.Println(err)
				}
				client.unAckedMsgs.Insert(item)
			} else {
				client.pendingMsgs.Insert(item)
			}
		}
	}
	time.Sleep(time.Millisecond)

}
