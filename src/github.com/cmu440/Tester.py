import subprocess
import os
import time
import random
import signal

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
    return "".join(random.choice(letters) for i in range(length))


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


def create_miner_args(latency: int):
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

def stest5_load_balancing_3Miners():
    num_clients = 2
    num_miners  = 3
    nonces = [999999, 999]
    messages = [generate_random_message() for _ in range(num_clients)]

    # Compile
    compile_programs()
    print(
        f"[TESTER]: Clients: {num_clients}, Miners: {num_miners}, Nonces: {nonces}, Messages: {messages}"
    )

    # Start the server process
    server_process = start_server()

    # generate miner_args, dont start!
    miner_args = []
    for i in range(num_miners):
        miner_args.append(create_miner_args(0))

    # generate random client args, dont start!
    client_args = []
    for i in range(num_clients):
        message = messages[i]
        nonce = nonces[i]
        client_args.append(create_client_args(message, nonce))

    running_clients = []
    running_miners = []
    active_clients = 0
    active_miners = 0

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

    while running_clients:
        for client_process in running_clients:
            if client_process.poll() is not None:  # Process has finished
                stdout, stderr = client_process.communicate()  # Capture the output
                if stdout:
                    print(f"Output: {stdout.decode().strip()}")
                if stderr:
                    print(f"Error: {stderr.decode().strip()}")
                running_clients.remove(client_process)
                active_clients -= 1
                print(
                    f"[CLIENT]:Process {client_process.pid} finished with return code {client_process.returncode}"
                )

        for miner_process in running_miners:
            if miner_process.poll() is not None:  # Process has finished
                stdout, stderr = miner_process.communicate()  # Capture the output
                if stdout:
                    print(f"\033[92mOutput: {stdout.decode().strip()}\033[0m")
                if stderr:
                    print(f"\033[31mError: {stderr.decode().strip()}\033[0m")
                running_miners.remove(miner_process)
                active_miners -= 1
                print(
                    f"[MINER]:Process {miner_process.pid} finished with return code {miner_process.returncode}"
                )

        print("[Active Clients]: ", active_clients)
        print("[Active Miners] : ", active_miners)

    # Cleanup: Stop server and miners
    print("[TESTER]: Stopping processes...")
    if len(running_miners) != 0:
        for miner_process in running_miners:
            miner_process.terminate()

    server_process.terminate()


def GenTest(
    num_clients,
    num_miners,
    nonces,
    min_latency,
    max_latency,
    client_startp,
    client_dropp,
    miner_startp,
    miner_dropp,
):
    """
    Main function to orchestrate the steps.
    """
    assert len(nonces) == num_clients
    messages = [generate_random_message() for _ in range(num_clients)]
    latencies = [generate_random_latency(min_latency, max_latency) for _ in range(num_miners)]

    # Compile
    compile_programs()
    print(
        f"[TESTER]: Clients: {num_clients}, Miners: {num_miners}, Nonces: {nonces}, Messages: {messages}"
    )

    # Start the server process
    server_process = start_server()

    # generate miner_args, dont start!
    miner_args = []
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
    active_clients  = 0
    active_miners   = 0

    for arg in client_args:
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
    # while client_args or running_clients:
    while running_clients:
        t += 1

        if random.random() < client_startp:
            try:
                random_client_arg = client_args.pop()
                print("[Starting Client] :", random_client_arg)
                client_process = start_client(random_client_arg)
                running_clients.append(client_process)
                active_clients += 1
            except:
                print("[No Pending Clients to start]")

        if random.random() < client_dropp:
            try:
                client_process = running_clients.pop()
                client_process.terminate()
                active_clients -= 1
                print("[Dropping Active Client]")
            except:
                print("[No Active Clients]")
                pass

        if random.random() < miner_startp:
            try:
                random_miner_arg = miner_args.pop()
                print("[Starting Miner] :", random_miner_arg)
                miner_process = start_miner(random_miner_arg)
                miner_args.remove(random_miner_arg)
                running_miners.append(miner_process)
                active_miners += 1
            except:
                print("[No Pending Miners to start]")

        if random.random() < miner_dropp:
            try:
                miner_process = running_miners.pop()
                # how to kill the miner and restart it
                miner_process.terminate()
                active_miners -= 1
                print("[Dropping Active Miner]")
            except:
                print("[No Active Miners]")
                pass

        for client_process in running_clients:
            if client_process.poll() is not None:  # Process has finished
                stdout, stderr = client_process.communicate()  # Capture the output
                if stdout:
                    print(f"Output: {stdout.decode().strip()}")
                if stderr:
                    print(f"Error: {stderr.decode().strip()}")
                running_clients.remove(client_process)
                active_clients -= 1
                print(
                    f"[CLIENT]:Process {client_process.pid} finished with return code {client_process.returncode}"
                )

        for miner_process in running_miners:
            if miner_process.poll() is not None:  # Process has finished
                stdout, stderr = miner_process.communicate()  # Capture the output
                if stdout:
                    print(f"\033[92mOutput: {stdout.decode().strip()}\033[0m")
                if stderr:
                    print(f"\033[31mError: {stderr.decode().strip()}\033[0m")
                running_miners.remove(miner_process)
                active_miners -= 1
                print(
                    f"[MINER]:Process {miner_process.pid} finished with return code {miner_process.returncode}"
                )

        print("[Active Clients]: ", active_clients)
        print("[Active Miners] : ", active_miners)
        time.sleep(0.1)

    # Cleanup: Stop server and miners
    print("[TESTER]: Stopping processes...")
    if len(running_miners) != 0:
        for miner_process in running_miners:
            miner_process.terminate()

    server_process.terminate()

def multiple_requests_miner_killed_restarted(num_miners, num_miners_dropped, miner_startp):
    nonces   = [9999, 9999, 9999, 9999]
    messages = [generate_random_message() for _ in range(len(nonces))]

    # Compile
    compile_programs()
    print(
        f"[TESTER]: Clients: {len(nonces)}, Miners: {num_miners}, Nonces: {nonces}, Messages: {messages}"
    )

    # Start the server process
    server_process = start_server()

    # generate miner_args, dont start!
    miner_args = []
    for i in range(num_miners):
        miner_args.append(create_miner_args(0))

    # generate random client args, dont start!
    client_args = []
    for i in range(len(nonces)):
        message = messages[i]
        nonce = nonces[i]
        client_args.append(create_client_args(message, nonce))

    running_clients = []
    running_miners  = []
    active_clients  = 0
    active_miners   = 0

    # start all miners
    for arg in miner_args:
        random_miner_arg = arg
        print("[Starting Miner] :", arg)
        miner_process = start_miner(random_miner_arg)
        running_miners.append(miner_process)
        active_miners += 1

    # wait for miners to connect
    time.sleep(5.0)

    killed_miner_pids = []
    # kill some miners
    while len(running_miners) > (num_miners - num_miners_dropped):
        try:
            miner_process = running_miners.pop()
            pid = miner_process.pid
            # how to kill the miner and restart it
            os.kill(pid, signal.SIGSTOP)
            print("[Suspend Miner]: ", pid)
            killed_miner_pids.append(pid)
        except:
            print("[No Active Miners]")
            pass
    
    time.sleep(5.0)

    # start all clients
    for arg in client_args:
        print("[Starting Client] :", arg)
        client_process = start_client(arg)
        running_clients.append(client_process)
        active_clients += 1

    print("[Active Clients]: ", active_clients)
    print("[Active Miners] : ", active_miners)
    
    while running_clients:

        # randomly start miners
        if random.random() < miner_startp:
            try:
                pid = killed_miner_pids.pop()
                print("[Resuming] :", pid)
                os.kill(pid, signal.SIGCONT)
            except:
                pass
            

        for client_process in running_clients:
            if client_process.poll() is not None:  # Process has finished
                stdout, stderr = client_process.communicate()  # Capture the output
                if stdout:
                    print(f"Output: {stdout.decode().strip()}")
                if stderr:
                    print(f"Error: {stderr.decode().strip()}")
                running_clients.remove(client_process)
                active_clients -= 1
                print(
                    f"[CLIENT]:Process {client_process.pid} finished with return code {client_process.returncode}"
                )
                print("[Active Clients]: ", active_clients)
                print("[Active Miners] : ", active_miners)

        for miner_process in running_miners:
            if miner_process.poll() is not None:  # Process has finished
                stdout, stderr = miner_process.communicate()  # Capture the output
                if stdout:
                    print(f"\033[92mOutput: {stdout.decode().strip()}\033[0m")
                if stderr:
                    print(f"\033[31mError: {stderr.decode().strip()}\033[0m")
                running_miners.remove(miner_process)
                active_miners -= 1
                print(
                    f"[MINER]:Process {miner_process.pid} finished with return code {miner_process.returncode}"
                )
                print("[Active Clients]: ", active_clients)
                print("[Active Miners] : ", active_miners)



    # Cleanup: Stop server and miners
    print("[TESTER]: Stopping processes...")
    if len(running_miners) != 0:
        for miner_process in running_miners:
            miner_process.terminate()

    server_process.terminate()




def basicTest():
     GenTest(
        num_clients=1,
        num_miners=1,
        nonces=[9999],
        min_latency=0,
        max_latency=0,
        client_startp=0.0,
        client_dropp=0.0,
        miner_startp=0.0,
        miner_dropp=0.0,
    )

def multiple_requests_requests_miners_killed(num_miners, client_dropp, miner_dropp):
    nonces = [99999, 9999, 9999, 9999, 99999, 9999, 999, 9999] 
    GenTest(num_clients=len(nonces), 
            num_miners=num_miners, 
            nonces=nonces, 
            min_latency=0, 
            max_latency=0, 
            client_startp=0.0, 
            client_dropp=client_dropp, 
            miner_startp=0.0, 
            miner_dropp=miner_dropp)
    
     
    
if __name__ == "__main__":
   
    #basicTest()
    #stest5_load_balancing_3Miners()
    #multiple_requests_requests_miners_killed(num_miners=5, client_dropp=0.25, miner_dropp=0.25)
    multiple_requests_miner_killed_restarted(num_miners=5, num_miners_dropped=5, miner_startp=0.001)
    '''
    main(
        num_clients=4,
        num_miners=1,
        min_nonce=9999,
        max_nonce=9999,
        min_latency=0,
        max_latency=0,
        client_startp=1.0,
        client_dropp=0.0,
        miner_startp=1.0,
        miner_dropp=1.0,
        restart=True,
    )
    '''