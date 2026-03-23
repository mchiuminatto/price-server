variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev | staging | prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "price-server"
}

variable "instance_type" {
  description = "EC2 instance type — t3.small is the budget sweet-spot (~$15/mo)"
  type        = string
  default     = "t3.small"
}

variable "key_pair_name" {
  description = "Name of an existing EC2 key pair for SSH access"
  type        = string
  default     = "price-server-key"
}

variable "data_volume_size_gb" {
  description = "Root EBS volume size in GB (holds /data for raw + processed files)"
  type        = number
  default     = 20
}

variable "ssh_allow_cidrs" {
  description = "CIDR blocks allowed to SSH into the pipeline VM. Leave empty (default) to auto-detect your current public IP at apply time."
  type        = list(string)
  default     = []  # empty = auto-detect caller IP via checkip.amazonaws.com
}

variable "assign_elastic_ip" {
  description = "Whether to assign an Elastic IP (stable public address)"
  type        = bool
  default     = true
}