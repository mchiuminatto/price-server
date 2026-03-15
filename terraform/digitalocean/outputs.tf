output "droplet_ip" {
  description = "Public IP of the pipeline Droplet"
  value       = digitalocean_droplet.pipeline.ipv4_address
}

output "ssh_command" {
  description = "SSH command to connect to the pipeline Droplet"
  value       = "ssh root@${digitalocean_droplet.pipeline.ipv4_address}"
}

output "data_volume_name" {
  description = "Block storage volume device name (mount to /data)"
  value       = digitalocean_volume.data.name
}
