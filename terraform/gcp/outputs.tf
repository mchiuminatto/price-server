output "instance_ip" {
  description = "Public IP of the pipeline VM"
  value       = google_compute_instance.pipeline.network_interface[0].access_config[0].nat_ip
}

output "ssh_command" {
  description = "SSH command to connect to the pipeline VM"
  value       = "ssh ubuntu@${google_compute_instance.pipeline.network_interface[0].access_config[0].nat_ip}"
}

output "instance_name" {
  description = "Compute Engine instance name"
  value       = google_compute_instance.pipeline.name
}
