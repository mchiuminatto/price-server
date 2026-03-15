variable "do_token" {
  description = "DigitalOcean API token"
  type        = string
  sensitive   = true
}

variable "do_region" {
  description = "DigitalOcean region"
  type        = string
  default     = "nyc3"
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

variable "droplet_size" {
  description = "Droplet size — s-1vcpu-2gb is the budget pick (~$12/mo)"
  type        = string
  default     = "s-1vcpu-2gb"
}

variable "ssh_key_name" {
  description = "Name of an existing SSH key in your DO account"
  type        = string
}

variable "data_volume_size_gb" {
  description = "Block storage volume size in GB for /data"
  type        = number
  default     = 50
}

variable "ssh_allow_cidrs" {
  description = "CIDR blocks allowed to SSH into the pipeline Droplet"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}
