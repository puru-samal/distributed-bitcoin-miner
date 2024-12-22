# Distributed Bitcoin Miner

This project implements a Live Sequence Protocol (LSP) and a distributed Bitcoin miner that uses the LSP to communicate with the server and other miners.

## The Live Sequence Protocol (LSP)

The Live Sequence Protocol (LSP) is a homegrown protocol for providing reliable communication with simple client and server APIs on top of the Internet UDP protocol. LSP provides features that lie somewhere between UDP and TCP, but it also has features not found in either protocol:

- Unlike UDP or TCP, it supports a client-server communication model.
- The server maintains connections between a number of clients, each of which is identified by a numeric connection identifier.
- Communication between the server and a client consists of a sequence of discrete messages in each direction.
- Message sizes are limited to fit within single UDP packets (around 1,000 bytes).
- Messages are sent reliably: a message sent over the network must be received exactly once, and messages must be received in the same order they were sent.
- Message integrity is ensured: a message sent over the network will be rejected if modified in transit.
- The server and clients monitor the status of their connections and detect when the other side has become disconnected.

A more detailed specification of the LSP protocol can be found in Part A of the [Writeup](p1_24.pdf).

### Testing using `srunner` & `crunner`

The repository contains two simple echo server/client programs called `srunner` and `crunner` that import the `github.com/cmu440/lsp` package.

To compile, build, and run these programs, use the `go run` command from inside the directory storing the file.

```bash
go run srunner.go
```

The `srunner` and `crunner` programs may be customized using command line flags. For more
information, specify the `-h` flag at the command line. For example,

```bash
$ go run srunner.go -h
Usage of bin/srunner:
  -elim=5: epoch limit
  -ems=2000: epoch duration (ms)
  -port=9999: port number
  -rdrop=0: network read drop percent
  -v=false: show srunner logs
  -wdrop=0: network write drop percent
  -wsize=1: window
  -maxUnackMessages=1: maximum unacknowledged messages allowed
  -maxBackoff: maximum interval epoch
```

As an example, to start an echo server on port `6060` on an AFS cluster machine, execute the following command:

```sh
<path_to_p1>/bin/linux/srunner_sols -port=6060
```

### Running the tests

Some tests are provided in the `lsp/` directory. To run the tests, execute the following command from inside the
`p1/src/github.com/cmu440/lsp` directory for each of the tests (where `TestName` is the
name of one of the 61 test cases, such as `TestBasic6` or `TestWindow1`):

```sh
go test -run=TestName
```

To run the tests with the race detector enabled, execute the following command:

```sh
go test -race -run=TestName
```

Also, there are test scripts mocks in `sh/` which can be used to test the implementation of the LSP. Simply run the corresponding script with ` sh path/to/run_test.sh` inside the `p1/src/github.com/cmu440/lsp` directory.

## Distributed Bitcoin Miner

The project additionally implements a distributed Bitcoin miner, a simple distributed system using the LSP implementation to harness the power of multiple processors to speed up a compute-intensive task while being capable of recovering from sudden machine failures.

### System Architecture

The distributed system will consist of the following three components:

- Client: An LSP client that sends a user-specified request to the server, receives and prints the result, and then exits.
- Miner: An LSP client that continually accepts requests from the server, exhaustively computes all hashes over a specified range of nonces, and then sends the server the final result.
- Server: An LSP server that manages the entire Bitcoin cracking enterprise. At any time, the server can have any number of workers available, and can receive any number of requests from any number of clients. For each client request, it splits the request into multiple smaller jobs and distributes these jobs to its available miners. The server waits for each worker to respond before generating and sending the final result back to the client.

Each of the three components communicate with one another using a set of pre-defined messages. Each type of message is declared in the message.go file as a Go struct. Each message is marshalled into a sequence of bytes using Go’s json package.

A more detailed specification of the distributed system can be found in Part B of the [Writeup](p1_24.pdf).

### Compiling the `client`, `miner` & `server` programs

To compile the `client`, `miner`, and `server` programs, use the `go install` command
as follows:

```bash
# Compile the client, miner, and server programs. The resulting binaries
# will be located in the $GOPATH/bin directory.
go install github.com/cmu440/bitcoin/client
go install github.com/cmu440/bitcoin/miner
go install github.com/cmu440/bitcoin/server

# Start the server, specifying the port to listen on.
$HOME/go/bin/server 6060

# Start a miner, specifying the server's host:port.
$HOME/go/bin/miner localhost:6060

# Start the client, specifying the server's host:port, the message
# "bradfitz", and max nonce 9999.
$HOME/go/bin/client localhost:6060 bradfitz 9999
```

### Run Sanity Tests

Some _basic_ tests are provided for the miner and client implementations. Extra more-intensive tests are implemented in `src/github.com/cmu440/Tester.py`. To run these tests, you need to ensure you have compiled version of `client` and `miner` in the same directories as their respective source files. Do this by running the following commands in the each directory:

```bash
$ cd <path-to-p1>/src/github.com/cmu440/bitcoin/miner
$ go build miner.go
$ cd ../client
$ go build client.go
```

You can then run your code by navigating to the directory for your specific device and running the binaries from there.
On an Arm Mac, for example, the command would be:

```bash
$ cd <path-to-p1>/bin/darwin/arm64
$ ./ctest
$ ./mtest
```

`Tester.py` is a Python script that automates this process and includes a number of more-intensive tests. Run the script with `python3 Tester.py --test=<test_number>`. For more information, run `python3 Tester.py --help`.

## Miscellaneous

### Reading the API Documentation

You can read the documentation in a browser:

1. Install `godoc` globally, by running the following command **outside** the `src/github.com/cmu440` directory:

```sh
go install golang.org/x/tools/cmd/godoc@latest
```

2. Start a godoc server by running the following command **inside** the `src/github.com/cmu440` directory:

```sh
godoc -http=:6060
```

3. While the server is running, navigate to [localhost:6060/pkg/github.com/cmu440](http://localhost:6060/pkg/github.com/cmu440) in a browser.
