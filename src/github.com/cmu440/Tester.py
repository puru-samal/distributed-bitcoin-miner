import subprocess
import os
import tqdm
from test_helpers import *

# To kill a process running on a port
# sudo lsof -i udp:6060
# sudo kill -9 <PID>
# for pid in {889..1017}; do sudo kill -9 $pid; done

# Define the paths to the Go programs
gopath_bin = os.path.expanduser("~/go/bin")
server_path = os.path.join(gopath_bin, "server")
miner_path = os.path.join(gopath_bin, "miner")
client_path = os.path.join(gopath_bin, "client")

def BasicTest(nonces=[9999], latencies=[0], clients_to_start=[0], miners_to_start=[0]):
    '''A basic test. All clients and miners will be started. No drops. All client requests should be processed.'''

    client_args = generate_client_arg_list(client_path, nonces=nonces)
    miner_args  = generate_miner_arg_list(miner_path, latencies=latencies)
    
    # Compile
    compile_programs()
    print(f"[TESTER]: BasicTest1")

    # Start the server process
    server_process = start_server(server_path)

    # Start all clients
    running_clients, client_args = start_some_processes(client_args, args_to_start=clients_to_start, process_type='Client')
    assert len(client_args) == len(nonces) - len(clients_to_start)
    assert len(running_clients) == len(clients_to_start)

    # Start all miners 
    running_miners, miner_args = start_some_processes(miner_args, args_to_start=miners_to_start, process_type='Miner')
    assert len(miner_args) == len(latencies) - len(miners_to_start)
    assert len(running_miners) == len(miners_to_start)

    active_clients   = len(running_clients)
    active_miners    = len(running_miners)
    coutputs         = []
    cerrs            = []
    moutputs         = []
    merrs            = []

    print("[Active Clients]: ", active_clients)
    print("[Active Miners] : ", active_miners)


    while active_clients != 0:
        for client_process in running_clients:
            stdout, stderr = poll_process(client_process)
            if stdout:
                output = f"[Client{client_process.pid}]: " + stdout.decode().strip()
                coutputs.append(output)
            if stderr:
                err = f"[Client{client_process.pid}]: " + stderr.decode().strip()
            if client_process.returncode is not None:
                active_clients -= 1
                running_clients.remove(client_process)
                print("[Active Clients]: ", active_clients)
        
        for miner_process in running_miners:
            stdout, stderr = poll_process(miner_process)
            if stdout:
                output = f"[Miner{miner_process.pid}]: " + stdout.decode().strip()
                moutputs.append(output)
            if stderr:
                err = f"[Miner{miner_process.pid}]: " + stderr.decode().strip()
                merrs.append(err)    
            if miner_process.returncode is not None:
                active_miners -= 1
                running_miners.remove(miner_process)
                print("[Active Miners] : ", active_miners)

    # Cleanup: Stop server and miners
    print("[TESTER]: Stopping processes...")
    for client_process in running_clients:
        client_process.terminate()

    for miner_process in running_miners:
        miner_process.terminate()

    server_process.terminate()

    # check:
    print("Captured Client Outputs: ")
    for output in coutputs:
        print(output)

    print("Captured Client Errs: ")
    for err in cerrs:
        print(err)

    print("Captured Miner Outputs: ")
    for output in moutputs:
        print(output)

    print("Captured Miner Errs: ")
    for err in merrs:
        print(err)

def killTests(nonces):
    pass



if __name__ == "__main__":
    # Basic1: 1 job, 1 miner, 1 chunk
    # BasicTest(nonces=[9999], latencies=[0], clients_to_start=[0], miners_to_start=[0])
    # Basic2: 1 job, 1 miner, 10 chunk
    # BasicTest(nonces=[99999], latencies=[0], clients_to_start=[0], miners_to_start=[0])
    # Basic3: 2 jobs (one small, one large), 1 miner
    # BasicTest(nonces=[999, 99999], latencies=[0], clients_to_start=[0, 1], miners_to_start=[0])
    # Basic: 4 jobs, 2 miners
    # BasicTest(nonces=[999, 99999, 9999, 9999], latencies=[0, 0], clients_to_start=[0, 1, 2, 3], miners_to_start=[0, 1])
      
      killTests()