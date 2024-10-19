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

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "minerLog.txt"
		flag = os.O_RDWR | os.O_CREATE
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
		LOGF.Printf("[Miner[id %d] Join Fail]\n", miner.ConnID())
		fmt.Println("Failed to join with server:", err)
		return
	}

	LOGF.Printf("[Miner[id %d] Joined]\n", miner.ConnID())

	defer miner.Close()

	// TODO: implement this!
	bitcoin.ProcessMiner(miner, LOGF)

}
