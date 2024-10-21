import subprocess
import os
import time
import getopt, sys
import random

# To kill a process running on a port
# sudo lsof -i udp:6060
# sudo kill -9 <PID>

# Define the paths to the Go programs
gopath_bin = os.path.expanduser("~/go/bin")
server_path = os.path.join(gopath_bin, "server")
miner_path = os.path.join(gopath_bin, "miner")
client_path = os.path.join(gopath_bin, "client")

print(os.getcwd())


def run_command(command):
    """Helper function to run a shell command."""
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        print(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        print(e.stderr.decode())


def compile_programs():
    """Step 1: Compile the Go programs."""
    print("[TESTER]: Compiling the Go programs...")
    # run_command(f"go install {os.getcwd()}/bitcoin/client/client.go")
    # run_command(f"go install {os.getcwd()}/bitcoin/miner/miner.go")
    # run_command(f"go install {os.getcwd()}/bitcoin/server/server.go")

    # Rachell: use the go install command to compile the programs
    run_command(f"go install github.com/cmu440/bitcoin/client")
    run_command(f"go install github.com/cmu440/bitcoin/miner")
    run_command(f"go install github.com/cmu440/bitcoin/server")
    print("Compilation completed.")


def start_server():
    """Step 2: Start the server."""
    print("[TESTER]: Starting the server on port 6060...")
    server_process = subprocess.Popen([server_path, "6060"])
    time.sleep(2)  # Wait for server to start
    return server_process


def start_miner():
    """Step 3: Start the miner."""
    print("[TESTER]: Starting the miner and connecting to server...")
    miner_process = subprocess.Popen([miner_path, "localhost:6060"])
    time.sleep(2)  # Wait for miner to connect
    return miner_process


def start_client(message, nonce):
    """Step 4: Start the client."""
    print("[TESTER]: Starting the client and sending a request to the server...")

    client_process = subprocess.Popen(
        [client_path, "localhost:6060", message, str(nonce)],
        stdout=subprocess.PIPE,
    )

    # Q) how to disconnect client while communicating with server? (need to check if the result is ignored)

    try:
        output, _ = client_process.communicate(
            timeout=1
        )  # Wait for client to finish and get output
        print(f"[TESTER]: Client Output:\n{output.decode()}")
    except subprocess.TimeoutExpired:
        print("[TESTER]: Timeout occurred while waiting for client to finish.")
        client_process.terminate()


def main():
    """Main function to orchestrate the steps.
    -c: number of clients,
    -m: number of miners,
    -n: nonce values for each client (comma separated),
    -M: messages for each client (comma separated)

    Example (PASS):
        python Tester.py -c 1 -m 1 -n 9999 -msg bradfitz
        python Tester.py -c 1 -m 1 -n 5000 -msg bradfitz
        python Tester.py -c 1 -m 1 -n 201234 -msg bradfitz
        python Tester.py -c 1 -m 10 -n 201234 -msg bradfitz
        python Tester.py -c 2 -m 2 -n 201234,5000 -M bradfitz,abcdef
        python Tester.py -c 5 -m 2 -n 201234,5000,1000,50000,1 -M bradfitz,abcdef,jklfdsafs,jrieonfk,lsjin
    """
    compile_programs()

    argumentList = sys.argv[1:]

    options = "c:m:n:M:"

    long_options = ["client=", "miner=", "nonce=", "message="]

    clients = 1
    miners = 1
    nonces = []
    messages = []

    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)
        print(arguments)
        for currentArgument, currentValue in arguments:

            if currentArgument in ("-c", "--client"):
                clients = int(currentValue)

            elif currentArgument in ("-m", "--miner"):
                miners = int(currentValue)

            elif currentArgument in ("-n", "--nonce"):
                nonce_ls = currentValue.split(",")
                for n in nonce_ls:
                    nonces.append(int(n))

            elif currentArgument in ("-M", "--message"):
                message_list = currentValue.split(",")

                for message in message_list:
                    messages.append(message)

    except getopt.error as err:
        # output error, and return with an error code
        print(str(err))

    print(clients, miners, nonces, messages)

    if clients != len(nonces) or clients != len(messages):
        print("Number of clients, nonces and messages must be the same")
        return

    server_process = start_server()

    for i in range(miners):
        miner_process = start_miner()

    for i in range(clients):
        message = messages[i]
        nonce = nonces[i]

        # Q) how to check if the server is waiting for a new miner to join?

        # randomly terminate the miner (less than 20% chance)
        if random.random() < 0.2:
            print("[TESTER]: Randomly Terminating the miner...")
            miner_process.terminate()
            miners -= 1

        # randomly add the miner (less than 20% chance)
        if random.random() < 0.2:
            print("[TESTER]: Randomly Adding a new miner...")
            miner_process = start_miner()
            miners += 1

        start_client(message, nonce)

    # Cleanup processes
    print("[TESTER]: Stopping processes...")

    server_process.terminate()

    for i in range(miners):
        miner_process.terminate()


if __name__ == "__main__":

    main()
