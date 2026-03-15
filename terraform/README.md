# Terraform — Cloud-Agnostic Deployment

The pipeline runs on **any Linux VM with Docker** — no managed Kubernetes,
no managed Redis, no cloud-specific services.

Pick your cloud provider and `cd` into the matching directory:

```
terraform/
├── aws/              # EC2 t3.small (~$15/mo)
├── gcp/              # Compute Engine e2-small (~$13/mo)
└── digitalocean/     # Droplet s-1vcpu-2gb (~$12/mo)
```

## Architecture (same on every cloud)

```
┌─────────────────────────────────────────────┐
│  Linux VM (Ubuntu 22.04)                    │
│                                             │
│  ┌─────────────────────────────────────┐    │
│  │  Docker Compose                     │    │
│  │                                     │    │
│  │  ┌──────────┐  ┌─────────────────┐  │    │
│  │  │  Redis 7  │  │ pipeline-worker │  │    │
│  │  │ (streams) │◄─│ (all 6 steps)  │  │    │
│  │  └──────────┘  └─────────────────┘  │    │
│  │                ┌─────────────────┐  │    │
│  │                │   serializer    │  │    │
│  │                │  (--serialize)  │  │    │
│  │                └─────────────────┘  │    │
│  └─────────────────────────────────────┘    │
│                                             │
│  /data/raw/          ← CSV input            │
│  /data/processed/    ← Parquet output       │
└─────────────────────────────────────────────┘
```

## Quick start

```bash
# 1. Choose a cloud
cd terraform/aws            # or gcp/ or digitalocean/

# 2. Init + apply
terraform init
terraform plan
terraform apply

# 3. SSH into the VM
ssh -i <key> ubuntu@<ip>

# 4. Clone the repo
git clone <repo-url> /opt/pipeline/app
cd /opt/pipeline/app

# 5. Edit environment
nano /opt/pipeline/.env

# 6. Start the pipeline
docker compose -f deploy/docker-compose.prod.yml up -d

# 7. Trigger processing
docker compose -f deploy/docker-compose.prod.yml exec redis \
  redis-cli XADD pipeline_trigger_queue '*' data \
  '{"storage_path":"/data/raw","data_type":"tick","config_path":"/app/config/pipeline_config.yaml"}'
```

## Storage options

The pipeline storage backend is set via `PIPELINE_STORAGE_BACKEND` in `.env`:

| Backend | Value | Path format | Use case |
|---------|-------|-------------|----------|
| Local disk | `local` | `/data/raw` | Default, cheapest |
| AWS S3 | `s3` | `s3://bucket/raw` | Durable, shareable |
| GCS | `gcs` | `gcs://bucket/raw` | GCP native |
| DO Spaces | `s3` | `s3://space/raw` | S3-compatible (set `PIPELINE_S3_ENDPOINT_URL`) |

## Monthly cost comparison

| Cloud | VM | Storage | Total |
|-------|-----|---------|-------|
| AWS EC2 | t3.small $15 | 20GB EBS $1.60 | **~$17/mo** |
| GCP | e2-small $13 | 20GB pd-std $0.80 | **~$14/mo** |
| DigitalOcean | 2GB Droplet $12 | 50GB volume $5 | **~$17/mo** |

Compare to the old EKS stack: ~$73/mo (control plane) + $15 (node) + $12 (ElastiCache) = **$100+/mo**.
