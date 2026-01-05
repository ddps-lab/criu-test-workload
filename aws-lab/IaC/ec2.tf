resource "tls_private_key" "ssh_key" { 
  algorithm = "ED25519"
}

resource "aws_security_group" "ec2_sg" {
    name        = "${var.prefix}-ec2-sg"
    description = "Allow inbound traffic"
    vpc_id      = module.vpc.vpc_id
    
    ingress = [{
        from_port   = 22
        to_port     = 22
        protocol    = "tcp"
        description = ""
        ipv6_cidr_blocks = []
        prefix_list_ids = []
        security_groups = []
        self = false
        cidr_blocks = ["0.0.0.0/0"]
    },
    {
        description = ""
        ipv6_cidr_blocks = []
        prefix_list_ids = []
        security_groups = []
        from_port   = 0
        to_port     = 0
        protocol    = -1
        cidr_blocks = []
        self        = true
    }]
    egress = [{
        description = ""
        ipv6_cidr_blocks = []
        prefix_list_ids = []
        security_groups = []
        self = false
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }]
}

resource "aws_instance" "bastion_ec2" {
  ami = var.ami_type == "Ubuntu" ? data.aws_ami.ubuntu_ami.id : data.aws_ami.al2023_ami.id
  instance_type = "t3.medium"
  key_name = var.key_name
  subnet_id = module.vpc.public_subnets[0]
  vpc_security_group_ids = [aws_security_group.ec2_sg.id]
  tags = {
    Name = "${var.prefix}-bastion-ec2"
  }
  iam_instance_profile = aws_iam_instance_profile.ec2_instance_profile.name
  user_data = <<-EOF
  #!/bin/bash
  sudo su

  %{ if var.ami_type == "Ubuntu" }
  export USERNAME="ubuntu"
  %{ else }
  export USERNAME="ec2-user"
  %{ endif }

  %{ if var.ami_type == "Ubuntu" }
  apt update
  apt install -y fio unzip python3-pip htop nfs-common libprotobuf-dev libprotobuf-c-dev protobuf-c-compiler protobuf-compiler python3-protobuf libbsd-dev libnftables-dev libcap-dev libaio-dev libnet-dev libnl-3-dev libgnutls28-dev libdrm-dev libssl-dev iproute2 nftables 
  pip3 install future ipaddress --break-system-packages
  snap install aws-cli --classic
  aws s3 cp s3://${var.criu_bucket}/criu /usr/local/sbin/criu
  chmod 755 /usr/local/sbin/criu
  %{ else }
  yum update
  yum install -y amazon-efs-utils rsync fio python3-pip criu htop
  %{ endif }

  echo "${tls_private_key.ssh_key.public_key_openssh}" >> /home/$USERNAME/.ssh/authorized_keys
  echo "${tls_private_key.ssh_key.public_key_openssh}" >> /root/.ssh/authorized_keys
  echo "${tls_private_key.ssh_key.private_key_openssh}" >> /home/$USERNAME/.ssh/id_ed25519
  echo "${tls_private_key.ssh_key.private_key_openssh}" >> /root/.ssh/id_ed25519
  chown $USERNAME:$USERNAME /home/$USERNAME/.ssh -R
  chown root:root /root/.ssh -R
  chmod 600 /home/$USERNAME/.ssh/id_ed25519
  chmod 600 /root/.ssh/id_ed25519
  echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config
  systemctl restart sshd
  cat <<FOE >> /home/$USERNAME/.ssh/config
  Host *
      StrictHostKeyChecking no
      UserKnownHostsFile=/dev/null
  FOE
  echo "export AZ_A_INSTANCES_IP='${join(" ", aws_instance.az_a_ec2[*].private_ip)}'" >> /home/$USERNAME/.bashrc
  echo "export AZ_A_INSTANCES_ID='${join(" ", aws_instance.az_a_ec2[*].id)}'" >> /home/$USERNAME/.bashrc
  echo "export AZ_C_INSTANCES_IP='${join(" ", aws_instance.az_c_ec2[*].private_ip)}'" >> /home/$USERNAME/.bashrc
  echo "export AZ_C_INSTANCES_ID='${join(" ", aws_instance.az_c_ec2[*].id)}'" >> /home/$USERNAME/.bashrc
  echo "export AZ_A_VOLUME_ID='${join(" ", aws_ebs_volume.az_a_volume[*].id)}'" >> /home/$USERNAME/.bashrc
  echo "export AZ_C_VOLUME_ID='${join(" ", aws_ebs_volume.az_c_volume[*].id)}'" >> /home/$USERNAME/.bashrc
  echo "export REGION='${var.region}'" >> /home/$USERNAME/.bashrc
  %{ if var.enable_efs_module == true }
  mkdir /mnt/efs
  %{ if var.ami_type == "Ubuntu" }
  echo "export EFS_ID='${module.efs[0].id}'" >> /home/$USERNAME/.bashrc
  mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 ${module.efs[0].dns_name}:/ /mnt/efs
  %{ else }
  echo "export EFS_ID='${module.efs[0].id}'" >> /home/$USERNAME/.bashrc
  mount -t efs -o tls ${module.efs[0].id}:/ /mnt/efs
  %{ endif}
  chown $USERNAME:$USERNAME /mnt/efs -R
  %{ else }
  echo "export EFS_ID=''" >> /home/$USERNAME/.bashrc
  %{ endif }

  su - $USERNAME
  pip3 install paramiko boto3 argparse scp --break-system-packages
  EOF
  
  depends_on = [ aws_instance.az_a_ec2, aws_instance.az_c_ec2, module.efs ]
}

resource "aws_instance" "az_a_ec2" {
  count = var.az_a_ec2_count
  ami = var.ami_type == "Ubuntu" ? data.aws_ami.ubuntu_ami.id : data.aws_ami.al2023_ami.id
  instance_type = var.instance_type
  key_name = var.key_name
  subnet_id = module.vpc.public_subnets[0]
  vpc_security_group_ids = [aws_security_group.ec2_sg.id]
  root_block_device {
    volume_size = 50
  }
  dynamic "ephemeral_block_device" {
    for_each = var.enable_ephemeral_block_device ? [1] : []

    content {
      device_name = "/dev/sdb"
      virtual_name = "ephemeral0"
    }
  }
  tags = {
    Name = "${var.prefix}-az-a-ec2-${count.index}"
  }
  iam_instance_profile = aws_iam_instance_profile.ec2_instance_profile.name
  user_data = <<-EOF
  #!/bin/bash
  sudo su

  %{ if var.ami_type == "Ubuntu" }
  export USERNAME="ubuntu"
  %{ else }
  export USERNAME="ec2-user"
  %{ endif }

  %{ if var.enable_ephemeral_block_device == true }
  mkfs.ext4 /dev/nvme1n1
  mkdir /mnt/ephemeral
  mount /dev/nvme1n1 /mnt/ephemeral
  chmod 777 /mnt/ephemeral -R
  %{ endif }

  %{ if var.ami_type == "Ubuntu" }
  apt update
  apt install -y fio rsync unzip python3-pip htop nfs-common libprotobuf-dev libprotobuf-c-dev protobuf-c-compiler protobuf-compiler python3-protobuf libbsd-dev libnftables-dev libcap-dev libaio-dev libnet-dev libnl-3-dev libgnutls28-dev libdrm-dev libssl-dev iproute2 nftables 
  pip3 install future ipaddress --break-system-packages
  snap install aws-cli --classic
  aws s3 cp s3://${var.criu_bucket}/criu /usr/local/sbin/criu
  chmod 755 /usr/local/sbin/criu
  %{ else }
  yum update
  yum install -y amazon-efs-utils fio python3-pip criu htop
  %{ endif }

  echo "${tls_private_key.ssh_key.public_key_openssh}" >> /home/$USERNAME/.ssh/authorized_keys
  echo "${tls_private_key.ssh_key.public_key_openssh}" >> /root/.ssh/authorized_keys
  echo "${tls_private_key.ssh_key.private_key_openssh}" >> /home/$USERNAME/.ssh/id_ed25519
  echo "${tls_private_key.ssh_key.private_key_openssh}" >> /root/.ssh/id_ed25519
  chown $USERNAME:$USERNAME /home/$USERNAME/.ssh -R
  chown root:root /root/.ssh -R
  chmod 600 /home/$USERNAME/.ssh/id_ed25519
  chmod 600 /root/.ssh/id_ed25519
  echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config
  systemctl restart sshd
  cat <<FOE >> /home/$USERNAME/.ssh/config
  Host *
      StrictHostKeyChecking no
      UserKnownHostsFile=/dev/null
  FOE

  pip3 install paramiko boto3 argparse scp --break-system-packages

  %{ if var.enable_efs_module == true }
  mkdir /mnt/efs
  %{ if var.ami_type == "Ubuntu" }
  echo "export EFS_ID='${module.efs[0].id}'" >> /home/$USERNAME/.bashrc
  mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 ${module.efs[0].dns_name}:/ /mnt/efs
  %{ else }
  echo "export EFS_ID='${module.efs[0].id}'" >> /home/$USERNAME/.bashrc
  mount -t efs -o tls ${module.efs[0].id}:/ /mnt/efs
  %{ endif}
  chown $USERNAME:$USERNAME /mnt/efs -R
  %{ else }
  echo "export EFS_ID=''" >> /home/$USERNAME/.bashrc
  %{ endif }
  EOF

  depends_on = [ module.efs ]
}

resource "aws_instance" "az_c_ec2" {
  count = var.az_c_ec2_count
  ami = var.ami_type == "Ubuntu" ? data.aws_ami.ubuntu_ami.id : data.aws_ami.al2023_ami.id
  instance_type = var.instance_type
  key_name = var.key_name
  subnet_id = module.vpc.public_subnets[1]
  vpc_security_group_ids = [aws_security_group.ec2_sg.id]
  tags = {
    Name = "${var.prefix}-az-c-ec2-${count.index}"
  }
  root_block_device {
    volume_size = 50
  }
  dynamic "ephemeral_block_device" {
    for_each = var.enable_ephemeral_block_device ? [1] : []

    content {
      device_name = "/dev/sdb"
      virtual_name = "ephemeral0"
    }
  }
  iam_instance_profile = aws_iam_instance_profile.ec2_instance_profile.name
  user_data = <<-EOF
  #!/bin/bash
  sudo su

  %{ if var.ami_type == "Ubuntu" }
  export USERNAME="ubuntu"
  %{ else }
  export USERNAME="ec2-user"
  %{ endif }

  %{ if var.enable_ephemeral_block_device == true }
  mkfs.ext4 /dev/nvme1n1
  mkdir /mnt/ephemeral
  mount /dev/nvme1n1 /mnt/ephemeral
  %{ endif }

  %{ if var.ami_type == "Ubuntu" }
  apt update
  apt install -y fio rsync unzip python3-pip htop nfs-common libprotobuf-dev libprotobuf-c-dev protobuf-c-compiler protobuf-compiler python3-protobuf libbsd-dev libnftables-dev libcap-dev libaio-dev libnet-dev libnl-3-dev libgnutls28-dev libdrm-dev libssl-dev iproute2 nftables 
  pip3 install future ipaddress --break-system-packages
  cd /tmp
  curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
  unzip awscliv2.zip
  sudo ./aws/install
  snap install aws-cli --classic
  aws s3 cp s3://${var.criu_bucket}/criu /usr/local/sbin/criu
  chmod 755 /usr/local/sbin/criu
  %{ else }
  yum update
  yum install -y amazon-efs-utils fio python3-pip criu htop
  %{ endif }

  echo "${tls_private_key.ssh_key.public_key_openssh}" >> /home/$USERNAME/.ssh/authorized_keys
  echo "${tls_private_key.ssh_key.public_key_openssh}" >> /root/.ssh/authorized_keys
  echo "${tls_private_key.ssh_key.private_key_openssh}" >> /home/$USERNAME/.ssh/id_ed25519
  echo "${tls_private_key.ssh_key.private_key_openssh}" >> /root/.ssh/id_ed25519
  chown $USERNAME:$USERNAME /home/$USERNAME/.ssh -R
  chown root:root /root/.ssh -R
  chmod 600 /home/$USERNAME/.ssh/id_ed25519
  chmod 600 /root/.ssh/id_ed25519
  echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config
  systemctl restart sshd
  cat <<FOE >> /home/$USERNAME/.ssh/config
  Host *
      StrictHostKeyChecking no
      UserKnownHostsFile=/dev/null
  FOE

  pip3 install paramiko boto3 argparse scp --break-system-packages

  %{ if var.enable_efs_module == true }
  mkdir /mnt/efs
  %{ if var.ami_type == "Ubuntu" }
  echo "export EFS_ID='${module.efs[0].id}'" >> /home/$USERNAME/.bashrc
  mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 ${module.efs[0].dns_name}:/ /mnt/efs
  %{ else }
  echo "export EFS_ID='${module.efs[0].id}'" >> /home/$USERNAME/.bashrc
  mount -t efs -o tls ${module.efs[0].id}:/ /mnt/efs
  %{ endif}
  chown $USERNAME:$USERNAME /mnt/efs -R
  %{ else }
  echo "export EFS_ID=''" >> /home/$USERNAME/.bashrc
  %{ endif }
  EOF

  depends_on = [ module.efs ]
}