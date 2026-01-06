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
  ami = var.ami_id
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
# Bastion EC2 userdata
# Note: Most dependencies are pre-installed in the AMI (ami_setup.sh)
# This script only handles dynamic configuration (SSH keys, env vars, mounts)

export USERNAME="ubuntu"

# SSH key setup (dynamically generated)
echo "${tls_private_key.ssh_key.public_key_openssh}" >> /home/$USERNAME/.ssh/authorized_keys
echo "${tls_private_key.ssh_key.public_key_openssh}" >> /root/.ssh/authorized_keys
echo "${tls_private_key.ssh_key.private_key_openssh}" >> /home/$USERNAME/.ssh/id_ed25519
echo "${tls_private_key.ssh_key.private_key_openssh}" >> /root/.ssh/id_ed25519
chown $USERNAME:$USERNAME /home/$USERNAME/.ssh -R
chown root:root /root/.ssh -R
chmod 600 /home/$USERNAME/.ssh/id_ed25519
chmod 600 /root/.ssh/id_ed25519
echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config
systemctl restart ssh || systemctl restart sshd
cat <<'FOE' >> /home/$USERNAME/.ssh/config
Host *
    StrictHostKeyChecking no
    UserKnownHostsFile=/dev/null
FOE

# Environment variables for experiment scripts
echo "export AZ_A_INSTANCES_IP='${join(" ", aws_instance.az_a_ec2[*].private_ip)}'" >> /home/$USERNAME/.bashrc
echo "export AZ_A_INSTANCES_ID='${join(" ", aws_instance.az_a_ec2[*].id)}'" >> /home/$USERNAME/.bashrc
echo "export AZ_C_INSTANCES_IP='${join(" ", aws_instance.az_c_ec2[*].private_ip)}'" >> /home/$USERNAME/.bashrc
echo "export AZ_C_INSTANCES_ID='${join(" ", aws_instance.az_c_ec2[*].id)}'" >> /home/$USERNAME/.bashrc
echo "export AZ_A_VOLUME_ID='${join(" ", aws_ebs_volume.az_a_volume[*].id)}'" >> /home/$USERNAME/.bashrc
echo "export AZ_C_VOLUME_ID='${join(" ", aws_ebs_volume.az_c_volume[*].id)}'" >> /home/$USERNAME/.bashrc
echo "export REGION='${var.region}'" >> /home/$USERNAME/.bashrc

# EFS mount (if enabled)
%{ if var.enable_efs_module == true }
mkdir -p /mnt/efs
echo "export EFS_ID='${module.efs[0].id}'" >> /home/$USERNAME/.bashrc
mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 ${module.efs[0].dns_name}:/ /mnt/efs
chown $USERNAME:$USERNAME /mnt/efs -R
%{ else }
echo "export EFS_ID=''" >> /home/$USERNAME/.bashrc
%{ endif }

# Update criu_workload repo
export HOME=/root
git config --global --add safe.directory /opt/criu_workload
cd /opt/criu_workload && git pull origin main || true
chown -R $USERNAME:$USERNAME /opt/criu_workload

# Generate servers.yaml with instance IPs
cat <<FOE > /opt/criu_workload/config/servers.yaml
# Auto-generated server configuration
# AZ-A instances: ${join(", ", aws_instance.az_a_ec2[*].private_ip)}
# AZ-C instances: ${join(", ", aws_instance.az_c_ec2[*].private_ip)}

nodes:
  ssh_user: "ubuntu"
  ssh_key: "~/.ssh/id_ed25519"

  source:
    ip: "${length(aws_instance.az_a_ec2) > 0 ? aws_instance.az_a_ec2[0].private_ip : ""}"
    name: "az-a-node-0"

  destination:
    ip: "${length(aws_instance.az_a_ec2) > 1 ? aws_instance.az_a_ec2[1].private_ip : (length(aws_instance.az_c_ec2) > 0 ? aws_instance.az_c_ec2[0].private_ip : "")}"
    name: "${length(aws_instance.az_a_ec2) > 1 ? "az-a-node-1" : (length(aws_instance.az_c_ec2) > 0 ? "az-c-node-0" : "")}"

# All available nodes
all_nodes:
  az_a:
%{ for idx, ip in aws_instance.az_a_ec2[*].private_ip ~}
    - ip: "${ip}"
      name: "az-a-node-${idx}"
%{ endfor ~}
  az_c:
%{ for idx, ip in aws_instance.az_c_ec2[*].private_ip ~}
    - ip: "${ip}"
      name: "az-c-node-${idx}"
%{ endfor ~}
FOE
chown $USERNAME:$USERNAME /opt/criu_workload/config/servers.yaml
EOF

  depends_on = [ aws_instance.az_a_ec2, aws_instance.az_c_ec2, module.efs ]
}

resource "aws_instance" "az_a_ec2" {
  count = var.az_a_ec2_count
  ami = var.ami_id
  instance_type = var.instance_type
  key_name = var.key_name
  subnet_id = module.vpc.public_subnets[0]
  vpc_security_group_ids = [aws_security_group.ec2_sg.id]
  root_block_device {
    volume_size = 100
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
# AZ-A EC2 userdata
# Note: Most dependencies are pre-installed in the AMI (ami_setup.sh)
# This script only handles dynamic configuration (SSH keys, mounts)

export USERNAME="ubuntu"

# Ephemeral storage mount (if enabled)
%{ if var.enable_ephemeral_block_device == true }
mkfs.ext4 /dev/nvme1n1
mkdir -p /mnt/ephemeral
mount /dev/nvme1n1 /mnt/ephemeral
chmod 777 /mnt/ephemeral -R
%{ endif }

# SSH key setup (dynamically generated)
echo "${tls_private_key.ssh_key.public_key_openssh}" >> /home/$USERNAME/.ssh/authorized_keys
echo "${tls_private_key.ssh_key.public_key_openssh}" >> /root/.ssh/authorized_keys
echo "${tls_private_key.ssh_key.private_key_openssh}" >> /home/$USERNAME/.ssh/id_ed25519
echo "${tls_private_key.ssh_key.private_key_openssh}" >> /root/.ssh/id_ed25519
chown $USERNAME:$USERNAME /home/$USERNAME/.ssh -R
chown root:root /root/.ssh -R
chmod 600 /home/$USERNAME/.ssh/id_ed25519
chmod 600 /root/.ssh/id_ed25519
echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config
systemctl restart ssh || systemctl restart sshd
cat <<'FOE' >> /home/$USERNAME/.ssh/config
Host *
    StrictHostKeyChecking no
    UserKnownHostsFile=/dev/null
FOE

# EFS mount (if enabled)
%{ if var.enable_efs_module == true }
mkdir -p /mnt/efs
echo "export EFS_ID='${module.efs[0].id}'" >> /home/$USERNAME/.bashrc
mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 ${module.efs[0].dns_name}:/ /mnt/efs
chown $USERNAME:$USERNAME /mnt/efs -R
%{ else }
echo "export EFS_ID=''" >> /home/$USERNAME/.bashrc
%{ endif }

# Update criu_workload repo
export HOME=/root
git config --global --add safe.directory /opt/criu_workload
cd /opt/criu_workload && git pull origin main || true
chown -R $USERNAME:$USERNAME /opt/criu_workload
EOF

  depends_on = [ module.efs ]
}

resource "aws_instance" "az_c_ec2" {
  count = var.az_c_ec2_count
  ami = var.ami_id
  instance_type = var.instance_type
  key_name = var.key_name
  subnet_id = module.vpc.public_subnets[1]
  vpc_security_group_ids = [aws_security_group.ec2_sg.id]
  tags = {
    Name = "${var.prefix}-az-c-ec2-${count.index}"
  }
  root_block_device {
    volume_size = 100
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
# AZ-C EC2 userdata
# Note: Most dependencies are pre-installed in the AMI (ami_setup.sh)
# This script only handles dynamic configuration (SSH keys, mounts)

export USERNAME="ubuntu"

# Ephemeral storage mount (if enabled)
%{ if var.enable_ephemeral_block_device == true }
mkfs.ext4 /dev/nvme1n1
mkdir -p /mnt/ephemeral
mount /dev/nvme1n1 /mnt/ephemeral
chmod 777 /mnt/ephemeral -R
%{ endif }

# SSH key setup (dynamically generated)
echo "${tls_private_key.ssh_key.public_key_openssh}" >> /home/$USERNAME/.ssh/authorized_keys
echo "${tls_private_key.ssh_key.public_key_openssh}" >> /root/.ssh/authorized_keys
echo "${tls_private_key.ssh_key.private_key_openssh}" >> /home/$USERNAME/.ssh/id_ed25519
echo "${tls_private_key.ssh_key.private_key_openssh}" >> /root/.ssh/id_ed25519
chown $USERNAME:$USERNAME /home/$USERNAME/.ssh -R
chown root:root /root/.ssh -R
chmod 600 /home/$USERNAME/.ssh/id_ed25519
chmod 600 /root/.ssh/id_ed25519
echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config
systemctl restart ssh || systemctl restart sshd
cat <<'FOE' >> /home/$USERNAME/.ssh/config
Host *
    StrictHostKeyChecking no
    UserKnownHostsFile=/dev/null
FOE

# EFS mount (if enabled)
%{ if var.enable_efs_module == true }
mkdir -p /mnt/efs
echo "export EFS_ID='${module.efs[0].id}'" >> /home/$USERNAME/.bashrc
mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 ${module.efs[0].dns_name}:/ /mnt/efs
chown $USERNAME:$USERNAME /mnt/efs -R
%{ else }
echo "export EFS_ID=''" >> /home/$USERNAME/.bashrc
%{ endif }

# Update criu_workload repo
export HOME=/root
git config --global --add safe.directory /opt/criu_workload
cd /opt/criu_workload && git pull origin main || true
chown -R $USERNAME:$USERNAME /opt/criu_workload
EOF

  depends_on = [ module.efs ]
}