import paramiko
import time
import boto3
import os

def execute_ssh_command(host, username, commands):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, username=username, key_filename='/home/ec2-user/.ssh/id_ed25519')

    results = []
    for command in commands:
        print(f"Running: {command}")
        stdin, stdout, stderr = ssh.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()
        results.append((command, stdout.read().decode(), stderr.read().decode(), exit_status))
        if exit_status != 0:
            print(f"Failed: {command}, Error: {stderr.read().decode()}")

    ssh.close()
    return results

def create_file_on_host(host, username):
    commands = [
        'sudo mkfs -t ext4 /dev/sdf || true',
        'sudo mkdir -p /mnt/ebs_test',
        'sudo mount /dev/sdf /mnt/ebs_test || true',
        'sudo dd if=/dev/urandom of=/mnt/ebs_test/testfile bs=1M count=8192',
    ]
    results = execute_ssh_command(host, username, commands)
    for cmd, out, err, status in results:
        if status != 0:
            print(f"Failed to create file: {cmd}, error: {err}")
            return
    print("File create completed.")

def detach_volume(volume_id):
    global ec2
    start_time = time.time()
    ec2.detach_volume(VolumeId=volume_id)
    while True:
        volume = ec2.describe_volumes(VolumeIds=[volume_id])['Volumes'][0]
        state = volume['State']
        if state == 'available':
            break
    end_time = time.time()
    print(f"Detach Time: {end_time - start_time:.2f} seconds")

def attach_volume(volume_id, instance_id):
    global ec2
    start_time = time.time()
    ec2.attach_volume(VolumeId=volume_id, InstanceId=instance_id, Device='/dev/sdf')
    while True:
        volume = ec2.describe_volumes(VolumeIds=[volume_id])['Volumes'][0]
        attachments = volume.get('Attachments', [])
        if attachments and attachments[0]['State'] == 'attached':
            break
    end_time = time.time()
    print(f"Attach Time: {end_time - start_time:.2f} seconds")

def performance_test_on_host(host, username):
    commands = [
        'sudo mkdir -p /mnt/ebs_test',
        'sudo mount /dev/sdf /mnt/ebs_test || true',
        'sudo fio --filename=/mnt/ebs_test/testfile --direct=1 --rw=read --bs=1M --size=8G --numjobs=1 --name=readtest --iodepth=64'
    ]
    results = execute_ssh_command(host, username, commands)
    for cmd, out, err, status in results:
        if status != 0:
            print(f"Failed to benchmark: {cmd}, error: {err}")
            return
        if "fio" in cmd:
            print("Benchmark result : ")
            print(out)

ec2 = ""

def main():
    global ec2
    REGION = os.getenv('REGION')
    ec2 = boto3.client('ec2', region_name=REGION)

    AZ_A_INSTANCES_IPs = list(os.getenv('AZ_A_INSTANCES_IP').split(" "))
    AZ_A_INSTANCES_IDs = list(os.getenv('AZ_A_INSTANCES_ID').split(" "))
    AZ_A_VOLUMES_IDs = list(os.getenv("AZ_A_VOLUME_ID").split(" "))

    host_a_ip = AZ_A_INSTANCES_IPs[0]
    host_a_id = AZ_A_INSTANCES_IDs[0]
    host_b_ip = AZ_A_INSTANCES_IPs[1]
    host_b_id = AZ_A_INSTANCES_IDs[1]

    volume_id = AZ_A_VOLUMES_IDs[0]

    username = 'ec2-user'

    print("Attaching EBS Volume on Host A...")
    attach_volume(volume_id, host_a_id)

    print("Creating File on Host A...")
    create_file_on_host(host_a_ip, username)

    print("Benchmarking EBS on Host A")
    performance_test_on_host(host_a_ip, username)

    print("Detaching EBS Volume on Host A...")
    detach_volume(volume_id)

    print("Attaching EBS Volume on Host B...")
    attach_volume(volume_id, host_b_id)

    print("Benchmarking EBS on Host B")
    performance_test_on_host(host_b_ip, username)

if __name__ == "__main__":
    main()
