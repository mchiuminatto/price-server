terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── Data sources ─────────────────────────────────────────────────────

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# ── Networking ───────────────────────────────────────────────────────

resource "aws_security_group" "pipeline" {
  name_prefix = "${var.project_name}-pipeline-"
  description = "Pipeline VM — SSH only, Redis internal"

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.ssh_allow_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.project_name}-pipeline-sg"
    Environment = var.environment
  }
}

# ── EC2 instance ─────────────────────────────────────────────────────

resource "aws_instance" "pipeline" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = var.key_pair_name
  vpc_security_group_ids = [aws_security_group.pipeline.id]

  root_block_device {
    volume_size = var.data_volume_size_gb
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = file("${path.module}/../../deploy/setup-vm.sh")

  tags = {
    Name        = "${var.project_name}-pipeline"
    Environment = var.environment
    Project     = var.project_name
  }
}

# ── Optional: Elastic IP for stable address ──────────────────────────

resource "aws_eip" "pipeline" {
  count    = var.assign_elastic_ip ? 1 : 0
  instance = aws_instance.pipeline.id
  domain   = "vpc"

  tags = {
    Name        = "${var.project_name}-pipeline-eip"
    Environment = var.environment
  }
}
