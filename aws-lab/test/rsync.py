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
    # PAGE_SERVER_PORT = 22222 # Removed as it's not needed for normal dump
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

    # CRIU dump (normal dump, no pre-dump)
    print("Starting CRIU dump...")
    dump_start_time = time.time()
    # Dump directly into WORKING_DIR
    criu_dump_command = f"sudo criu dump -D {WORKING_DIR} -t {src_pid} --shell-job" # Removed lazy-pages options
    run_ssh_command(src_srv_ssh_client, f"{criu_dump_command}")
    
    # Script to check when CRIU dump is complete (files are no longer changing) - REMOVED
    # check_script = f\"\"\" ... \"\"\" # Removed
    # stdout, stderr = run_ssh_command(src_srv_ssh_client, check_script) # Removed
    # if stderr: # Removed
    #     print(f"Error during dump check_script: {stderr}") # Removed
    dump_duration = time.time() - dump_start_time # This will now be very short

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

    # Page server section removed as it's not used for normal dump

    # CRIU restore on destination
    print("Starting CRIU restore...")
    restore_start_time = time.time()
    # Use WORKING_DIR for restore path on destination
    restore_command = f"sudo criu restore -D {WORKING_DIR} --shell-job" # Removed --lazy-pages
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
    print("-------------------------------------")


if __name__ == "__main__":
    main()
