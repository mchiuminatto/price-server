terraform {
  required_version = ">= 1.5.0"
  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = "~> 2.0"
    }
  }
}

provider "digitalocean" {
  # Set DIGITALOCEAN_TOKEN env var or pass via -var
  token = var.do_token
}

# ── SSH key ──────────────────────────────────────────────────────────

data "digitalocean_ssh_key" "pipeline" {
  name = var.ssh_key_name
}

# ── Firewall ─────────────────────────────────────────────────────────

resource "digitalocean_firewall" "pipeline" {
  name        = "${var.project_name}-pipeline-fw"
  droplet_ids = [digitalocean_droplet.pipeline.id]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = var.ssh_allow_cidrs
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

# ── Droplet ──────────────────────────────────────────────────────────

resource "digitalocean_droplet" "pipeline" {
  name     = "${var.project_name}-pipeline"
  image    = "ubuntu-22-04-x64"
  size     = var.droplet_size
  region   = var.do_region
  ssh_keys = [data.digitalocean_ssh_key.pipeline.id]

  user_data = file("${path.module}/../../deploy/setup-vm.sh")

  tags = [var.project_name, var.environment]
}

# ── Block storage for /data ──────────────────────────────────────────

resource "digitalocean_volume" "data" {
  region                  = var.do_region
  name                    = "${var.project_name}-data"
  size                    = var.data_volume_size_gb
  initial_filesystem_type = "ext4"
  description             = "Pipeline data volume (raw CSV + processed Parquet)"

  tags = [var.project_name, var.environment]
}

resource "digitalocean_volume_attachment" "data" {
  droplet_id = digitalocean_droplet.pipeline.id
  volume_id  = digitalocean_volume.data.id
}
