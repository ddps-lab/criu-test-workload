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

def detach_volume(volume_id):
    global ec2
    ec2.detach_volume(VolumeId=volume_id)
    while True:
        volume = ec2.describe_volumes(VolumeIds=[volume_id])['Volumes'][0]
        state = volume['State']
        if state == 'available':
            break

def attach_volume(volume_id, instance_id):
    global ec2
    ec2.attach_volume(VolumeId=volume_id, InstanceId=instance_id, Device='/dev/sdf')
    while True:
        volume = ec2.describe_volumes(VolumeIds=[volume_id])['Volumes'][0]
        attachments = volume.get('Attachments', [])
        if attachments and attachments[0]['State'] == 'attached':
            break

ec2 = ""

def main():
    global ec2
    SSH_USER_NAME = "ubuntu"
    WORKING_DIR = f"/home/{SSH_USER_NAME}/migration_test"
    EBS_DIR = "/mnt/ebs_test"
    EBS_WORKING_DIR = "/mnt/ebs_test/migration_test"
    REGION = os.getenv('REGION')
    PAGE_SERVER_PORT = 22222
    PRE_DUMP_NUM = 8

    ec2 = boto3.client('ec2', region_name=REGION)

    AZ_A_INSTANCES_IPs = list(os.getenv('AZ_A_INSTANCES_IP').split(" "))
    AZ_A_INSTANCES_IDs = list(os.getenv('AZ_A_INSTANCES_ID').split(" "))
    AZ_A_VOLUME_IDs = list(os.getenv('AZ_A_VOLUME_ID').split(" "))

    src_srv_ip = AZ_A_INSTANCES_IPs[0]
    dst_srv_ip = AZ_A_INSTANCES_IPs[1]
    src_srv_id = AZ_A_INSTANCES_IDs[0]
    dst_srv_id = AZ_A_INSTANCES_IDs[1]
    volume_id = AZ_A_VOLUME_IDs[0]

    # create ssh client
    src_srv_ssh_client = ssh_connect(src_srv_ip, SSH_USER_NAME)
    dst_srv_ssh_client = ssh_connect(dst_srv_ip, SSH_USER_NAME)

    # Attach EBS Volume on Src Server
    print("Attaching EBS Volume on Src Server...")
    try:
        detach_volume(volume_id)
    except:
        pass
    attach_volume(volume_id, src_srv_id)

    # Clean up & set up directories
    run_ssh_command(src_srv_ssh_client, f"sudo umount {EBS_DIR}")
    run_ssh_command(src_srv_ssh_client, f"sudo umount {EBS_DIR}")
    run_ssh_command(src_srv_ssh_client, f"rm -rf {WORKING_DIR}")
    run_ssh_command(src_srv_ssh_client, f"mkdir -p {WORKING_DIR}")
    run_ssh_command(src_srv_ssh_client, f"sudo mkdir -p {EBS_DIR}")
    run_ssh_command(src_srv_ssh_client, f"yes | sudo mkfs -t ext4 /dev/nvme1n1")
    run_ssh_command(src_srv_ssh_client, f"sudo mount /dev/nvme1n1 {EBS_DIR}")
    run_ssh_command(src_srv_ssh_client, f"sudo chown {SSH_USER_NAME}:{SSH_USER_NAME} -R {EBS_DIR}")
    run_ssh_command(src_srv_ssh_client, f"rm -rf {EBS_DIR}/*")
    scp_upload(src_srv_ssh_client, "increase_memory.py", f"{WORKING_DIR}/increase_memory.py")

    run_ssh_command(dst_srv_ssh_client, f"sudo umount {EBS_DIR}")
    run_ssh_command(dst_srv_ssh_client, f"sudo umount {EBS_DIR}")
    run_ssh_command(dst_srv_ssh_client, f"rm -rf {WORKING_DIR}")
    run_ssh_command(dst_srv_ssh_client, f"mkdir -p {WORKING_DIR}")
    run_ssh_command(dst_srv_ssh_client, f"sudo mkdir -p {EBS_DIR}")
    run_ssh_command(dst_srv_ssh_client, f"sudo chown {SSH_USER_NAME}:{SSH_USER_NAME} -R {EBS_DIR}")
    scp_upload(dst_srv_ssh_client, "increase_memory.py", f"{WORKING_DIR}/increase_memory.py")

    # Start memory increase process
    print("Starting increase memory")
    run_ssh_command(src_srv_ssh_client, f"touch {WORKING_DIR}/checkpoint_flag")
    run_ssh_background_command(src_srv_ssh_client, f"cd {WORKING_DIR} && python3 increase_memory.py --mb_size 256 --interval 5 --max_memory_mb 8192 --check_lazy_loading True &")

    # Get pid of the process
    src_pid, stderr = run_ssh_command(src_srv_ssh_client, "ps -ef | grep 'python3 increase_memory.py' | grep -v grep | awk '{print $2}' | tail -n 1")
    src_pid = src_pid.rstrip()

    # Start CRIU pre-dump
    total_pre_dump_duration = 0
    total_pre_dump_rsync_duration = 0
    checkpoint_num = 1

    print("Starting CRIU pre-dump...")
    time.sleep(10)

    total_start_time = time.time()
    while checkpoint_num <= PRE_DUMP_NUM:
        start_time = time.time()
        print(f"Pre-dump iteration {checkpoint_num}")

        run_ssh_command(src_srv_ssh_client, f"mkdir -p {WORKING_DIR}/{checkpoint_num}")
        criu_command = f"sudo criu pre-dump -D {WORKING_DIR}/{checkpoint_num} -t {src_pid} --shell-job --track-mem"
        if checkpoint_num > 1:
            criu_command += f" --prev-images-dir ../{checkpoint_num - 1}"

        pre_dump_start_time = time.time()
        stdout, stderr = run_ssh_command(src_srv_ssh_client, criu_command)
        pre_dump_duration = time.time() - pre_dump_start_time
        total_pre_dump_duration += pre_dump_duration

        rsync_start_time = time.time()
        stdout, stderr = run_ssh_command(src_srv_ssh_client, f"rsync -av --update --inplace --links {WORKING_DIR} {EBS_DIR}")
        print(stdout, stderr)
        rsync_duration = time.time() - rsync_start_time
        total_pre_dump_rsync_duration += rsync_duration

        checkpoint_num += 1

        elapsed_time = time.time() - start_time
        if elapsed_time < 10 and elapsed_time > 0:
            time.sleep(10 - elapsed_time)

    # CRIU dump with lazy pages
    print("Performing final CRIU dump with lazy pages...")
    dump_start_time = time.time()
    run_ssh_command(src_srv_ssh_client, f"mkdir -p {WORKING_DIR}/{checkpoint_num}")
    run_ssh_background_command(src_srv_ssh_client, f"sudo criu dump -D {WORKING_DIR}/{checkpoint_num} -t {src_pid} --shell-job --prev-images-dir ../{checkpoint_num - 1} --track-mem --lazy-pages --address 0.0.0.0 --port {PAGE_SERVER_PORT} &")
    check_script = f"""
    latest_change_time=$(date +%s)
    while true
    do
        latest_mod_time=$(find ${WORKING_DIR} -type f -exec stat --format='%Y' {{}} + | sort -nr | head -n 1)
        if [[ $latest_mod_time -gt $latest_change_time ]]; then
            latest_change_time=$latest_mod_time
        fi

        current_time=$(date +%s)
        idle_time=$((current_time - latest_change_time))

        if [[ $idle_time -ge 2 ]]; then
            break
        fi
    done
    """
    stdout, stderr = run_ssh_command(src_srv_ssh_client, check_script)
    dump_duration = time.time() - dump_start_time

    total_rsync_start_time = time.time()
    stdout, stderr = run_ssh_command(src_srv_ssh_client, f"rsync -av --update --inplace --links {WORKING_DIR} {EBS_DIR}")
    print(stdout)
    total_rsync_duration = time.time() - total_rsync_start_time

    # Detach EBS Volume on Src Server
    print("Detaching EBS Volume on Src Server...")
    ebs_volume_detach_start_time = time.time()
    run_ssh_command(src_srv_ssh_client, f"sudo umount {EBS_DIR}")
    detach_volume(volume_id)
    ebs_volume_detach_duration = time.time() - ebs_volume_detach_start_time

    # Attach EBS Volume on Dst Server
    print("Attaching EBS Volume on Dst Server...")
    ebs_volume_attach_start_time = time.time()    
    attach_volume(volume_id, dst_srv_id)
    print("Mounting EBS Volume on Dst Server...")
    run_ssh_command(dst_srv_ssh_client, f"sudo mount /dev/nvme1n1 {EBS_DIR}")
    run_ssh_command(src_srv_ssh_client, f"sudo chown {SSH_USER_NAME}:{SSH_USER_NAME} -R {EBS_DIR}")
    ebs_volume_attach_duration = time.time() - ebs_volume_attach_start_time

    # Start page server
    print("Starting page server...")
    page_server_start_time = time.time()
    run_ssh_background_command(dst_srv_ssh_client, f"sudo criu lazy-pages --images-dir {EBS_WORKING_DIR}/{checkpoint_num} --page-server --address {src_srv_ip} --port {PAGE_SERVER_PORT} &")
    page_server_duration = time.time() - page_server_start_time

    # CRIU restore
    print("Starting CRIU restore...")
    restore_start_time = time.time()
    stdout, stderr = run_ssh_command(dst_srv_ssh_client, f"sudo criu restore -D {EBS_WORKING_DIR}/{checkpoint_num} --shell-job --lazy-pages")
    restore_duration = time.time() - restore_start_time
    total_duration = time.time() - total_start_time

    print("-------------------------------------")
    print(f"Total execution time: {total_duration:.2f} seconds")
    print(f" - CRIU pre-dump time: {total_pre_dump_duration:.2f} seconds")
    print(f" - CRIU pre-dump rsync time: {total_pre_dump_rsync_duration:.2f} seconds")
    print(f" - CRIU dump time: {dump_duration:.2f} seconds")
    print(f" - CRIU dump rsync time: {total_rsync_duration:.2f} seconds")
    print(f" - EBS Volume Detach time: {ebs_volume_detach_duration:.2f} seconds")
    print(f" - EBS Volume Attach time: {ebs_volume_attach_duration:.2f} seconds")
    print(f" - CRIU restore time: {restore_duration:.2f} seconds")
    print("-------------------------------------")


if __name__ == "__main__":
    main()
