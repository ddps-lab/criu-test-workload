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
        'sudo dd if=/dev/urandom of=/mnt/efs/testfile bs=1M count=8192',
    ]
    results = execute_ssh_command(host, username, commands)
    for cmd, out, err, status in results:
        if status != 0:
            print(f"Failed to create file: {cmd}, error: {err}")
            return
    print("File create completed.")

def performance_test_on_host(host, username):
    commands = [
        'sudo fio --filename=/mnt/efs/testfile --direct=1 --rw=read --bs=1M --size=8G --numjobs=1 --name=readtest --iodepth=64'
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
    AZ_C_INSTANCES_IPs = list(os.getenv('AZ_C_INSTANCES_IP').split(" "))

    host_a_az_a_ip = AZ_A_INSTANCES_IPs[0]
    host_b_az_a_ip = AZ_A_INSTANCES_IPs[1]

    host_a_az_c_ip = AZ_C_INSTANCES_IPs[0]

    username = 'ec2-user'

    print("Creating File on Host A (On AZ a)...")
    create_file_on_host(host_a_az_a_ip, username)

    print("Benchmarking EFS on Host A (On AZ a)")
    performance_test_on_host(host_b_az_a_ip, username)

    print("Benchmarking EFS on Host B (On AZ a)")
    performance_test_on_host(host_b_az_a_ip, username)

    print("Benchmarking EFS on Host A (On AZ c)")
    performance_test_on_host(host_a_az_c_ip, username)

if __name__ == "__main__":
    main()
