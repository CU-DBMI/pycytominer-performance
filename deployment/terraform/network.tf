# ---------------------------------------------------------------------------------------------------
# --- network/firewall config
# ---------------------------------------------------------------------------------------------------

resource "google_compute_network" "network" {
  name                    = "${var.resource_prefix}network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnetwork" {
  name          = "${var.resource_prefix}subnetwork"
  ip_cidr_range = "10.128.0.0/9"
  region        = "us-central1"
  network       = google_compute_network.network.id
}

resource "google_compute_firewall" "fw_allow_ssh" {
  name    = "${var.resource_prefix}fw-allow-ssh"
  network = google_compute_network.network.id

  source_ranges = ["0.0.0.0/0"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_firewall" "fw_local" {
  name    = "${var.resource_prefix}fw-allow-local"
  network = google_compute_network.network.id

  source_ranges = [
    google_compute_subnetwork.subnetwork.ip_cidr_range
  ]

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }
}

resource "google_compute_firewall" "fw_http_tag" {
  name    = "${var.resource_prefix}fw-allow-http"
  network = google_compute_network.network.id

  source_ranges = ["0.0.0.0/0"]

  allow {
    protocol = "tcp"
    ports    = ["80"]
  }
  target_tags = ["http-server"]
}

resource "google_compute_firewall" "fw_https_tag" {
  name    = "${var.resource_prefix}fw-allow-https"
  network = google_compute_network.network.id

  source_ranges = ["0.0.0.0/0"]

  allow {
    protocol = "tcp"
    ports    = ["443"]
  }
  target_tags = ["https-server"]
}
