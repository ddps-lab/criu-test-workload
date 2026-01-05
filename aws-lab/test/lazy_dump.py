import os
import time
import paramiko
from scp import SCPClient
import boto3

def ssh_connect(host, username):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=username)
    return client

def run_ssh_command(client, command):
    stdin, stdout, stderr = client.exec_command(command)
    return stdout.read().decode(), stderr.read().decode()

def run_ssh_background_command(client, command):
    client.exec_command(command)

def scp_upload(ssh_client, local_path, remote_path):
    with SCPClient(ssh_client.get_transport()) as scp:
        scp.put(local_path, remote_path, recursive=True)

ec2 = ""

def main():
    global ec2
    SSH_USER_NAME = "ubuntu"
    WORKING_DIR = f"/home/{SSH_USER_NAME}/migration_test"
    REGION = os.getenv('REGION')
    PAGE_SERVER_PORT = 22222
    # PRE_DUMP_NUM = 8 # Removed

    ec2 = boto3.client('ec2', region_name=REGION)

    AZ_A_INSTANCES_IPs = list(os.getenv('AZ_A_INSTANCES_IP').split(" "))
    AZ_A_INSTANCES_IDs = list(os.getenv('AZ_A_INSTANCES_ID').split(" "))
    AZ_C_INSTANCES_IPs = list(os.getenv('AZ_C_INSTANCES_IP').split(" "))
    AZ_C_INSTANCES_IDs = list(os.getenv('AZ_C_INSTANCES_ID').split(" "))

    src_srv_ip = AZ_A_INSTANCES_IPs[0]
    dst_srv_ip = AZ_C_INSTANCES_IPs[0]
    # src_srv_id = AZ_A_INSTANCES_IDs[0] # Unused
    # dst_srv_id = AZ_A_INSTANCES_IDs[1] # Unused

    # create ssh client
    src_srv_ssh_client = ssh_connect(src_srv_ip, SSH_USER_NAME)
    dst_srv_ssh_client = ssh_connect(dst_srv_ip, SSH_USER_NAME)

    # Clean up & set up directories
    run_ssh_command(src_srv_ssh_client, f"rm -rf {WORKING_DIR}")
    run_ssh_command(src_srv_ssh_client, f"mkdir -p {WORKING_DIR}")
    scp_upload(src_srv_ssh_client, "increase_memory.py", f"{WORKING_DIR}/increase_memory.py")

    run_ssh_command(dst_srv_ssh_client, f"rm -rf {WORKING_DIR}")
    run_ssh_command(dst_srv_ssh_client, f"mkdir -p {WORKING_DIR}")
    # scp_upload(dst_srv_ssh_client, "increase_memory.py", f"{WORKING_DIR}/increase_memory.py") # Not in shell script logic for dst

    # Start memory increase process
    print("Starting increase memory")
    run_ssh_command(src_srv_ssh_client, f"touch {WORKING_DIR}/checkpoint_flag") # checkpoint_flag used by increase_memory.py
    run_ssh_background_command(src_srv_ssh_client, f"cd {WORKING_DIR} && python3 increase_memory.py --mb_size 256 --interval 5 --max_memory_mb 8192 --check_lazy_loading True &")

    # Get pid of the process
    src_pid, stderr = run_ssh_command(src_srv_ssh_client, "ps -ef | grep 'python3 increase_memory.py' | grep -v grep | awk '{print $2}' | tail -n 1")
    src_pid = src_pid.strip()
    if not src_pid:
        print("Error: Could not get PID of increase_memory.py")
        return

    # Define rsync destination parent directory for clarity
    rsync_dest_parent_dir = os.path.dirname(WORKING_DIR)

    total_start_time = time.time()

    # Sleep 90 seconds before dump
    print("Sleep 90 Seconds")
    time.sleep(90)

    # CRIU dump with lazy pages (no pre-dump)
    print("Starting CRIU dump...")
    dump_start_time = time.time()
    # Dump directly into WORKING_DIR
    criu_dump_command = f"sudo criu dump -D {WORKING_DIR} -t {src_pid} --shell-job --lazy-pages --address 0.0.0.0 --port {PAGE_SERVER_PORT}"
    run_ssh_background_command(src_srv_ssh_client, f"{criu_dump_command} &")
    
    # Script to check when CRIU dump is complete (files are no longer changing)
    check_script = f"""
    latest_change_time=$(date +%s)
    while true
    do
        # Check for dump completion by monitoring file modification times in WORKING_DIR
        latest_mod_time=$(find {WORKING_DIR} -type f -exec stat --format='%Y' {{}} + | sort -nr | head -n 1 2>/dev/null)
        if [[ -z "$latest_mod_time" ]]; then # Handle case where find returns nothing initially
            sleep 0.5
            continue
        fi

        if [[ $latest_mod_time -gt $latest_change_time ]]; then
            latest_change_time=$latest_mod_time
        fi

        current_time=$(date +%s)
        idle_time=$((current_time - latest_change_time))

        # Shell script uses idle_time >= 3, Python script used >=2. Aligning with shell.
        if [[ $idle_time -ge 3 ]]; then
            break
        fi
        sleep 0.5 # Check periodically
    done
    """
    stdout, stderr = run_ssh_command(src_srv_ssh_client, check_script)
    if stderr:
        print(f"Error during dump check_script: {stderr}")
    dump_duration = time.time() - dump_start_time

    # Rsync to destination server directly
    print("Starting rsync transfer...")
    total_rsync_start_time = time.time()
    rsync_command = f"rsync -av --ignore-existing {WORKING_DIR} {SSH_USER_NAME}@{dst_srv_ip}:{rsync_dest_parent_dir}/"
    stdout, stderr = run_ssh_command(src_srv_ssh_client, rsync_command)
    if stderr:
        print(f"Rsync stderr: {stderr}")
    # print(stdout) # Potentially very verbose
    run_ssh_command(dst_srv_ssh_client, f"rm -rf {WORKING_DIR}/checkpoint_flag")
    total_rsync_duration = time.time() - total_rsync_start_time

    # Start page server on destination
    print("Starting page server...")
    page_server_start_time = time.time()
    # Use WORKING_DIR for images_dir on destination
    page_server_command = f"sudo criu lazy-pages --images-dir {WORKING_DIR} --page-server --address {src_srv_ip} --port {PAGE_SERVER_PORT}"
    run_ssh_background_command(dst_srv_ssh_client, f"{page_server_command} &")
    page_server_duration = time.time() - page_server_start_time # Time to launch, not for it to finish

    # CRIU restore on destination
    print("Starting CRIU restore...")
    restore_start_time = time.time()
    # Use WORKING_DIR for restore path on destination
    restore_command = f"sudo criu restore -D {WORKING_DIR} --shell-job --lazy-pages"
    stdout, stderr = run_ssh_command(dst_srv_ssh_client, restore_command)
    if stderr:
        print(f"Restore stderr: {stderr}")
    # print(stdout) # Potentially verbose
    restore_duration = time.time() - restore_start_time
    
    total_duration = time.time() - total_start_time

    print("-------------------------------------")
    print(f"Total execution time: {total_duration:.2f} seconds")
    print(f" - CRIU dump time: {dump_duration:.2f} seconds")
    print(f" - RSYNC transfer time: {total_rsync_duration:.2f} seconds") # Renamed for clarity
    print(f" - CRIU restore time: {restore_duration:.2f} seconds")
    # page_server_duration is time to start it, not its operational time, so may not be as relevant here.
    print("-------------------------------------")


if __name__ == "__main__":
    main()
