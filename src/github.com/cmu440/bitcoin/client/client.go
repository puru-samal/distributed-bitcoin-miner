package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

// variables for logging
var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "clientLog.txt"
		flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF := log.New(file, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)

	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}
	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Printf("%s is not a number.\n", os.Args[3])
		return
	}
	seed := rand.NewSource(time.Now().UnixNano())
	isn := rand.New(seed).Intn(int(math.Pow(2, 8)))

	client, err := lsp.NewClient(hostport, isn, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return
	}

	LOGF.Printf("[Client[id %d] Connected]\n", client.ConnID())

	defer client.Close()

	// Create and send a new request
	request := bitcoin.NewRequest(message, 0, maxNonce)
	payload, _ := json.Marshal(request)
	client.Write(payload)
	LOGF.Printf("[Client[id %d] MsgSend]: %s\n", client.ConnID(), request.String())

	// Block until result is recieved
	result, err := client.Read()

	// Print result/disonnect and exit
	if err != nil {
		LOGF.Printf("[Client[id %d] Disconnect]\n", client.ConnID())
		printDisconnected()
		return
	} else {
		var msg bitcoin.Message
		error := json.Unmarshal(result, &msg)
		LOGF.Printf("[Client[id %d] MsgRecv]: %s\n", client.ConnID(), msg.String())
		// error when unmarshalling
		if error != nil {
			LOGF.Printf("[Client[id %d] Error]: %s\n", client.ConnID(), error.Error())
			return
		}
		printResult(msg.Hash, msg.Nonce)
	}
}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
