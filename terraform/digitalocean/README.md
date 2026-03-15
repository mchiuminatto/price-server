# terraform/digitalocean/README.md
#
# DigitalOcean deployment — single Droplet running Docker Compose.
#
#   - 1 × Droplet s-1vcpu-2gb (~$12/mo)
#   - 50 GB Block Storage for /data ($5/mo)
#   - Redis runs as a Docker container on the Droplet
#   - Optional: Spaces (S3-compatible) for remote storage
#
# Usage:
#   export DIGITALOCEAN_TOKEN="dop_v1_..."
#   cd terraform/digitalocean
#   terraform init
#   terraform plan
#   terraform apply
