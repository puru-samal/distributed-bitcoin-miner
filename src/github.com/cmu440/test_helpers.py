import subprocess
import os
import time
import random
import signal
from typing import List, Tuple, Literal


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
    print('*' * 10)
    print("[TESTER]: Compiling the Go programs...")
    run_command(f"go install github.com/cmu440/bitcoin/client")
    run_command(f"go install github.com/cmu440/bitcoin/miner")
    run_command(f"go install github.com/cmu440/bitcoin/server")
    print("Compilation completed.")
    print('*' * 10)


def start_server(server_path):
    """Step 2: Start the server."""
    print("[TESTER]: Starting the server on port 6060...")
    server_process = subprocess.Popen([server_path, "6060"])
    time.sleep(2)  # Wait for server to start
    return server_process


def create_miner_args(miner_path, latency:int=0):
    '''Create the argument to start a miner'''
    return [miner_path, "localhost:6060"]


def start_miner(miner_arg):
    print("[TESTER]: Starting the Miner and joining the server...")
    miner_process = subprocess.Popen(miner_arg)
    return miner_process


def create_client_args(client_path, message, nonce):
    return [client_path, "localhost:6060", message, str(nonce)]


def start_client(client_args):
    print("[TESTER]: Starting the Client and sending a request to the server...")
    client_process = subprocess.Popen(client_args, stdout=subprocess.PIPE)
    return client_process

def generate_client_arg_list(client_path, nonces:list) -> List[List]:
    '''Generate a list of client arguments based on supplied nonces'''
    client_args = []
    for nonce in nonces:
        message = generate_random_message()
        client_args.append(create_client_args(client_path, message, nonce))
    return client_args

def generate_miner_arg_list(miner_path ,latencies:list) -> List[List]:
    '''Generate a list of miner arguments based on supplied latencies each miner should have'''
    miner_args = []
    for latency in latencies:
        miner_args.append(create_miner_args(miner_path, latency))
    return miner_args

def start_some_processes(args:list, args_to_start:list, process_type:Literal['Client', 'Miner']) -> Tuple[list]:
    '''Start a subset of clients and return a tuple of a list of client processes 
    that have started and the client_args filterd by those processes that have started'''
    running_processes = []
    for i in args_to_start:
        print(f"[Starting {process_type}] :", args[i])
        if process_type == "Client":
            process = start_client(args[i])
        elif process_type == "Miner":
            process = start_miner(args[i])
        running_processes.append(process)
    remaining_args = [arg for i, arg in enumerate(args) if i not in args_to_start]
    return running_processes, remaining_args

def poll_process(process):
    ''' Poll a process and return the stdout, stderr. They will be none if no output is available'''
    stdout, stderr = None, None
    if process.poll() is None:
        return stdout, stderr
    # Process has finished
    stdout, stderr = process.communicate()  # Capture the output
    return stdout, stderr

def cleanup(server_process, running_client_process, running_miner_process):
    '''Cleanup: kill server, clients and miners'''
    print("[TESTER]: Stopping processes...")
    for client_process in running_client_process:
        client_process.kill()

    for miner_process in running_miner_process:
        miner_process.kill()
    server_process.kill()

