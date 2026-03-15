terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.gcp_project
  region  = var.gcp_region
  zone    = var.gcp_zone
}

# ── Firewall — SSH only ──────────────────────────────────────────────

resource "google_compute_firewall" "pipeline_ssh" {
  name    = "${var.project_name}-pipeline-ssh"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = var.ssh_allow_cidrs
  target_tags   = ["pipeline"]
}

# ── Compute Engine VM ────────────────────────────────────────────────

resource "google_compute_instance" "pipeline" {
  name         = "${var.project_name}-pipeline"
  machine_type = var.machine_type
  zone         = var.gcp_zone

  tags = ["pipeline"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = var.data_volume_size_gb
      type  = "pd-standard"
    }
  }

  network_interface {
    network = "default"
    access_config {
      # Ephemeral public IP — use a static IP resource if needed
    }
  }

  metadata = {
    ssh-keys = "ubuntu:${var.ssh_public_key}"
  }

  metadata_startup_script = file("${path.module}/../../deploy/setup-vm.sh")

  labels = {
    project     = var.project_name
    environment = var.environment
  }
}
