output "instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.pipeline.id
}

output "public_ip" {
  description = "Public IP of the pipeline VM (use EIP for stability)"
  value       = var.assign_elastic_ip ? aws_eip.pipeline[0].public_ip : aws_instance.pipeline.public_ip
}

output "ssh_command" {
  description = "SSH command to connect to the pipeline VM"
  value       = "ssh -i <key>.pem ubuntu@${var.assign_elastic_ip ? aws_eip.pipeline[0].public_ip : aws_instance.pipeline.public_ip}"
}
