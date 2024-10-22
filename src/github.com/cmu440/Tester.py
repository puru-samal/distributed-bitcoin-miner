import subprocess
import os
import time
import getopt, sys
import random
import argparse

# To kill a process running on a port
# sudo lsof -i udp:6060
# sudo kill -9 <PID>
# for pid in {889..1017}; do sudo kill -9 $pid; done

# Define the paths to the Go programs
gopath_bin = os.path.expanduser("~/go/bin")
server_path = os.path.join(gopath_bin, "server")
miner_path = os.path.join(gopath_bin, "miner")
client_path = os.path.join(gopath_bin, "client")


def generate_random_message(length=8):
    """Generate a random string of fixed length."""
    letters = "abcdefghijklmnopqrstuvwxyz"
    return ''.join(random.choice(letters) for i in range(length))

def generate_random_nonce(min_nonce, max_nonce):
    """Generate a random nonce (integer)."""
    return random.randint(min_nonce, max_nonce)


def generate_random_latency(min_latency, max_latency):
    """Generate a random nonce (integer)."""
    return random.randint(min_latency, max_latency)


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


def create_miner_args(latency:int):
    return [miner_path, "localhost:6060", f"{latency}"]


def start_miner(miner_arg):
    print("[TESTER]: Starting the miner and connecting to server...")
    miner_process = subprocess.Popen(miner_arg)
    return miner_process


def create_client_args(message, nonce):
    return [client_path, "localhost:6060", message, str(nonce)]


def start_client(client_args):
    print("[TESTER]: Starting the client and sending a request to the server...")
    client_process = subprocess.Popen(client_args, stdout=subprocess.PIPE)
    return client_process


def main(num_clients, num_miners, min_nonce, max_nonce, min_latency, max_latency, client_startp, client_dropp, miner_startp, miner_dropp):
    """
    Main function to orchestrate the steps.
    """

    # Generate random nonces and messages for each client
    nonces    = [generate_random_nonce(min_nonce, max_nonce) for _ in range(num_clients)]
    messages  = [generate_random_message() for _ in range(num_clients)]
    latencies = [generate_random_latency(min_latency, max_latency) for _ in range(num_miners)]

    # Compile
    compile_programs()
    print(f"[TESTER]: Clients: {num_clients}, Miners: {num_miners}, Nonces: {nonces}, Messages: {messages}")
    
    # Start the server process
    server_process = start_server()

    # generate miner_args, dont start!
    miner_args  = []
    for i in range(num_miners):
        miner_args.append(create_miner_args(latencies[i]))

    # generate random client args, dont start!
    client_args = []
    for i in range(num_clients):
        message = messages[i]
        nonce = nonces[i]
        client_args.append(create_client_args(message, nonce))

    
    running_clients = []
    running_miners  = []
    active_clients = 0
    active_miners  = 0

    print(len(client_args))

    for arg in client_args:
        print(arg)
        print("[Starting Client] :", arg)
        client_process = start_client(arg)
        running_clients.append(client_process)
        active_clients += 1

    for arg in miner_args:
        random_miner_arg = arg
        print("[Starting Miner] :", arg)
        miner_process = start_miner(random_miner_arg)
        running_miners.append(miner_process)
        active_miners += 1


    t = 0

    #while client_args or running_clients:
    while running_clients:
        t += 1
        print("t=",t)
        
        ''''
        if random.random() < client_startp:
            if len(client_args) != 0:
                random_client_arg = random.choice(client_args)
                print("[Starting Client] :", random_client_arg)
                client_process = start_client(random_client_arg)
                client_args.remove(random_client_arg)
                running_clients.append(client_process)
                active_clients += 1
            else:
                print("[No Pending Clients to start]")
        '''

        if random.random() < client_dropp:
            try:
                client_process = running_clients.pop()
                client_process.terminate()
                active_clients -= 1
                print("[Dropping Active Client]")
            except:
                print("[No Active Clients]")
                pass
                
        '''
        if random.random() < miner_startp:
            if len(miner_args) != 0:
                random_miner_arg = random.choice(miner_args)
                print("[Starting Miner] :", random_miner_arg)
                miner_process = start_miner(random_miner_arg)
                miner_args.remove(random_miner_arg)
                running_miners.append(miner_process)
                active_miners += 1
            else:
                print("[No Pending Miners to start]")
        '''
        
        if random.random() < miner_dropp:
            try:
                miner_process = running_miners.pop()
                miner_process.terminate()
                active_miners -= 1
                print("[Dropping Active Miner]")
            except:
                print("[No Active Miners]")
                pass

        for client_process in running_clients:
            if client_process.poll() is not None:              # Process has finished
                stdout, stderr = client_process.communicate()  # Capture the output
                if stdout:
                    print(f"Output: {stdout.decode().strip()}")
                if stderr:
                    print(f"Error: {stderr.decode().strip()}")
                running_clients.remove(client_process)
                active_clients -= 1
                print(f"Process {client_process.pid} finished with return code {client_process.returncode}")
            
        for miner_process in running_miners:
            if miner_process.poll() is not None:              # Process has finished
                stdout, stderr = miner_process.communicate()  # Capture the output
                if stdout:
                    print(f"\033[92mOutput: {stdout.decode().strip()}\033[0m")
                if stderr:
                    print(f"\033[31mError: {stderr.decode().strip()}\033[0m")
                running_miners.remove(miner_process)
                active_miners -= 1
                print(f"Process {miner_process.pid} finished with return code {miner_process.returncode}")
            
        print("[Active Clients]: ", active_clients)
        print("[Active Miners] : ", active_miners)
        time.sleep(1)

    # Cleanup: Stop server and miners
    print("[TESTER]: Stopping processes...")
    if len(running_miners) != 0:
        for miner_process in running_miners:
            miner_process.terminate()
    
    server_process.terminate()



if __name__ == "__main__":
    main(num_clients=200, 
         num_miners=50, 
         min_nonce=100000, 
         max_nonce=100000,
         min_latency=0, 
         max_latency=0, 
         client_startp=1.0, 
         client_dropp=0.0, 
         miner_startp=1.0, 
         miner_dropp=0.0)


