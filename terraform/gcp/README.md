# terraform/gcp/README.md
#
# GCP deployment — single Compute Engine VM running Docker Compose.
#
#   - 1 × e2-small (~$13/mo)
#   - 20 GB persistent disk for /data (included in VM)
#   - Redis runs as a Docker container on the VM
#   - Optional: GCS buckets for remote storage (pay per use)
#
# Usage:
#   cd terraform/gcp
#   terraform init
#   terraform plan -var="gcp_project=my-project-id" -var="ssh_public_key=ssh-ed25519 AAAA..."
#   terraform apply
