package bitcoin

import (
	"encoding/json"
	"log"

	"github.com/cmu440/lsp"
)

// Miner  _______________________________________________________________________________

// Helper function that handles miner processing.
// Reads a request from a server, does its work and sends result to server
// If a non-nil error is encountered during Read/Write, it returns
func ProcessMiner(miner lsp.Client, logger *log.Logger) {
	for {

		// will block until request sent by server
		payload, err := miner.Read()

		// miner loses contact with the server -> shut down
		if err != nil {
			logger.Printf("[Miner[id %d] Disconnect]\n", miner.ConnID())
			return
		}

		// initialize with lower hash
		// search from [lower+1, upper]
		// for lowest hash and corresponding nonce

		var request Message
		json.Unmarshal(payload, &request)
		logger.Printf("[Miner[id %d] MsgRecv]: %s\n", miner.ConnID(), request.String())

		hash := Hash(request.Data, request.Lower)
		nonce := request.Lower

		for n := request.Lower + 1; n <= request.Upper; n++ {
			h := Hash(request.Data, n)
			if hash > h {
				hash = h
				nonce = n
			}
		}

		result := NewResult(hash, nonce)
		rpayload, _ := json.Marshal(result)

		// miner loses contact with the server -> shut down
		rerr := miner.Write(rpayload)
		logger.Printf("[Miner[id %d] MsgSend]: %s\n", miner.ConnID(), request.String())

		if rerr != nil {
			logger.Printf("[Miner[id %d] Disconnect]\n", miner.ConnID())
			return
		}
	}
}

// Client _______________________________________________________________________________

// Server _______________________________________________________________________________

// Misc.  _______________________________________________________________________________
