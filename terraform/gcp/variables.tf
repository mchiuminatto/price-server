variable "gcp_project" {
  description = "GCP project ID"
  type        = string
}

variable "gcp_region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "gcp_zone" {
  description = "GCP zone"
  type        = string
  default     = "us-central1-a"
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

variable "machine_type" {
  description = "GCE machine type — e2-small is the budget pick (~$13/mo)"
  type        = string
  default     = "e2-small"
}

variable "ssh_public_key" {
  description = "SSH public key to add to the VM (e.g. ssh-ed25519 AAAA...)"
  type        = string
}

variable "data_volume_size_gb" {
  description = "Boot disk size in GB (holds /data for raw + processed files)"
  type        = number
  default     = 20
}

variable "ssh_allow_cidrs" {
  description = "CIDR blocks allowed to SSH into the pipeline VM"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}
