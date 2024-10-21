package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	// You will need this for randomized isn
	seed := rand.NewSource(time.Now().UnixNano())
	isn := rand.New(seed).Intn(int(math.Pow(2, 8)))

	// Create a New Client
	client, err := lsp.NewClient(hostport, isn, lsp.NewParams())
	if err != nil {
		return client, err
	}

	// Send join request
	request := bitcoin.NewJoin()
	payload, _ := json.Marshal(request)
	client.Write(payload)
	return client, err
}

// Helper function that handles miner processing.
// Reads a request from a server, does its work and sends result to server
// If a non-nil error is encountered during Read/Write, it returns
func processMiner(miner lsp.Client, LOGF *log.Logger) {
	for {

		// will block until request sent by server
		payload, err := miner.Read()

		// miner loses contact with the server -> shut down
		if err != nil {
			LOGF.Printf("[Miner[id %d] Disconnect]\n", miner.ConnID())
			return
		}

		// initialize with lower hash
		// search from [lower+1, upper]
		// for lowest hash and corresponding nonce

		var request bitcoin.Message
		json.Unmarshal(payload, &request)
		LOGF.Printf("[Miner[id %d] MsgRecv]: %s\n", miner.ConnID(), request.String())

		hash := bitcoin.Hash(request.Data, request.Lower)
		nonce := request.Lower

		for n := request.Lower + 1; n <= request.Upper; n++ {
			h := bitcoin.Hash(request.Data, n)
			if hash > h {
				hash = h
				nonce = n
			}
		}

		result := bitcoin.NewResult(hash, nonce)
		rpayload, _ := json.Marshal(result)

		// miner loses contact with the server -> shut down
		rerr := miner.Write(rpayload)
		LOGF.Printf("[Miner[id %d] MsgSend]: %s\n", miner.ConnID(), request.String())

		if rerr != nil {
			LOGF.Printf("[Miner[id %d] Disconnect]\n", miner.ConnID())
			return
		}
	}
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "minerLog.txt"
		flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF := log.New(file, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	LOGF.Printf("[Miner[id %d] Joined]\n", miner.ConnID())

	defer miner.Close()

	// TODO: implement this!
	processMiner(miner, LOGF)

}
