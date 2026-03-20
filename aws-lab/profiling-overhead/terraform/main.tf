provider "aws" {
  region = var.region
}

# S3 bucket for results
resource "aws_s3_bucket" "results" {
  bucket        = var.s3_bucket
  force_destroy = true
}

# IAM role for EC2 → S3 upload
resource "aws_iam_role" "experiment" {
  name = "criu-experiment-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "s3_upload" {
  name = "s3-upload"
  role = aws_iam_role.experiment.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject", "s3:GetObject"]
      Resource = "${aws_s3_bucket.results.arn}/*"
    }]
  })
}

resource "aws_iam_instance_profile" "experiment" {
  name = "criu-experiment-profile"
  role = aws_iam_role.experiment.name
}

# Security group (SSH only)
resource "aws_security_group" "experiment" {
  name = "criu-experiment-sg"
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# EC2 instances — one per experiment
resource "aws_instance" "experiment" {
  count                  = length(var.experiments)
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = var.key_name
  iam_instance_profile   = aws_iam_instance_profile.experiment.name
  vpc_security_group_ids = [aws_security_group.experiment.id]

  root_block_device {
    volume_size = 30
  }

  user_data = templatefile("${path.module}/userdata.tpl", {
    experiment_name = var.experiments[count.index].name
    workload        = var.experiments[count.index].workload
    configs         = var.experiments[count.index].configs
    extra_args      = var.experiments[count.index].extra
    s3_bucket       = var.s3_bucket
    region          = var.region
  })

  tags = {
    Name = "criu-exp-${var.experiments[count.index].name}"
  }

  # Auto-terminate after experiment
  instance_initiated_shutdown_behavior = "terminate"
}
