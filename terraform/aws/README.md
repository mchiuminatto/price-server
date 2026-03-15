# terraform/aws/README.md
#
# AWS deployment — single EC2 instance running Docker Compose.
#
# This replaces the previous EKS/ElastiCache setup with a much simpler
# (and cheaper) architecture:
#
#   - 1 × EC2 t3.small (~$15/mo)
#   - 1 × 20 GB EBS gp3 volume for /data ($1.60/mo)
#   - Redis runs as a Docker container on the same instance
#   - Optional: S3 buckets for remote storage (pay per use)
#
# The same pipeline Docker images run here as they would on GCP or DigitalOcean.
#
# Usage:
#   cd terraform/aws
#   terraform init
#   terraform plan
#   terraform apply
#
# After apply:
#   ssh -i <key> ubuntu@<public_ip>
#   # VM is pre-bootstrapped via user_data (setup-vm.sh)
#   cd /opt/pipeline/app
#   docker compose -f deploy/docker-compose.prod.yml up -d
