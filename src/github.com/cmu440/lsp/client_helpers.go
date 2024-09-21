package lsp

import (
	"encoding/json"
	"log"

	"github.com/cmu440/lspnet"
)

func sendToServer(conn *lspnet.UDPConn, msg *Message) {
	byt, err := json.Marshal(msg)
	log.Println("Unhandled: MsgData")
	if err == nil {
		conn.Write(byt)
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
