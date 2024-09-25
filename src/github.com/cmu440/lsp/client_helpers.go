package lsp

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/cmu440/lspnet"
)

type internalMsg struct {
	mtype InternalType
	err   error
	id    int
	msg   *Message
}

// Helper funcs

func sendToServer(conn *lspnet.UDPConn, msg *Message, logging bool) {
	byt, err := json.Marshal(msg)
	if err == nil {
		conn.Write(byt)
		if logging {
			log.Println("sent'")
		}
	}
}

func recvFromServer(conn *lspnet.UDPConn, msg *Message, logging bool) error {
	buf := make([]byte, 2000)
	n, err := conn.Read(buf)
	if err == nil && checkIntegrity(msg) {
		json.Unmarshal(buf[:n], &msg)
		if logging {
			log.Println("recv'd")
		}
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

// Assert function
func assert(condition bool, message string) {
	if !condition {
		panic(message)
	}
}
