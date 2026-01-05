import time
import boto3
import os

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
    return end_time - start_time

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
    return end_time - start_time

ec2 = ""

def main():
    global ec2
    REGION = os.getenv('REGION')
    ec2 = boto3.client('ec2', region_name=REGION)

    AZ_A_INSTANCES_IDs = list(os.getenv('AZ_A_INSTANCES_ID').split(" "))
    AZ_A_VOLUMES_IDs = list(os.getenv("AZ_A_VOLUME_ID").split(" "))

    host_a_id = AZ_A_INSTANCES_IDs[0]
    host_b_id = AZ_A_INSTANCES_IDs[1]

    volume_id = AZ_A_VOLUMES_IDs[0]

    host_a_attach_time = []
    host_a_detach_time = []
    host_b_attach_time = []
    host_b_detach_time = []
    for _ in range(10):
        host_a_attach_time.append(attach_volume(volume_id, host_a_id))
        host_a_detach_time.append(detach_volume(volume_id))
        host_b_attach_time.append(attach_volume(volume_id, host_b_id))
        host_b_detach_time.append(detach_volume(volume_id))
    
    all_attach_time = host_a_attach_time + host_b_attach_time
    all_detach_time = host_a_detach_time + host_b_detach_time

    print("Host A Attach Time: ")
    print(host_a_attach_time)
    print("Host A Detach Time: ")
    print(host_a_detach_time)

    print("Host B Attach Time: ")
    print(host_b_attach_time)
    print("Host B Detach Time: ")
    print(host_b_detach_time)

    print("All Attach Time: ")
    print(all_attach_time)
    print("All Detach Time: ")
    print(all_detach_time)

    print(f"Average Attach Time: {sum(all_attach_time) / len(all_attach_time):.2f} seconds")
    print(f"Average Detach Time: {sum(all_detach_time) / len(all_detach_time):.2f} seconds")

if __name__ == "__main__":
    main()
