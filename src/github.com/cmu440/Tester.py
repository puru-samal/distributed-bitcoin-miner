import subprocess
import os
import time

# Define the paths to the Go programs
gopath_bin  = os.path.expanduser("~/go/bin")
server_path = os.path.join(gopath_bin, "server")
miner_path  = os.path.join(gopath_bin, "miner")
client_path = os.path.join(gopath_bin, "client")

def run_command(command):
    """Helper function to run a shell command."""
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        print(e.stderr.decode())

def compile_programs():
    """Step 1: Compile the Go programs."""
    print("Compiling the Go programs...")
    run_command("go install github.com/cmu440/bitcoin/client")
    run_command("go install github.com/cmu440/bitcoin/miner")
    run_command("go install github.com/cmu440/bitcoin/server")
    print("Compilation completed.")

def start_server():
    """Step 2: Start the server."""
    print("Starting the server on port 6060...")
    server_process = subprocess.Popen([server_path, "6060"])
    time.sleep(2)  # Wait for server to start
    return server_process

def start_miner():
    """Step 3: Start the miner."""
    print("Starting the miner and connecting to server...")
    miner_process = subprocess.Popen([miner_path, "localhost:6060"])
    time.sleep(2)  # Wait for miner to connect
    return miner_process

def start_client():
    """Step 4: Start the client."""
    print("Starting the client and sending a request to the server...")
    client_process = subprocess.Popen([client_path, "localhost:6060", "bradfitz", "9999"], stdout=subprocess.PIPE)
    output, _ = client_process.communicate()  # Wait for client to finish and get output
    print(f"Client Output:\n{output.decode()}")

def main():
    """Main function to orchestrate the steps."""
    compile_programs()
    server_process = start_server()
    miner_process = start_miner()
    
    start_client()

    # Cleanup processes
    print("Stopping processes...")
    server_process.terminate()
    miner_process.terminate()
    
if __name__ == "__main__":
    main()
