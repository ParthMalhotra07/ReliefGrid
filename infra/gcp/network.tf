# D5: public nodes (external IPs for egress: image pulls, no Cloud NAT), in a dedicated VPC so
# none of the default network's permissive rules apply. GKE adds its own rules (control plane
# to nodes, and the LoadBalancer's loadBalancerSourceRanges) at a higher priority than the
# explicit deny below.

resource "google_compute_network" "reliefgrid" {
  name                    = "reliefgrid"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.enabled]
}

resource "google_compute_subnetwork" "nodes" {
  name          = "reliefgrid-nodes"
  region        = var.region
  network       = google_compute_network.reliefgrid.id
  ip_cidr_range = "10.10.0.0/20"

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.20.0.0/16"
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.30.0.0/20"
  }
}

# Nothing from the internet reaches a node unless GKE opened it for a Service.
resource "google_compute_firewall" "deny_internet_ingress" {
  name          = "reliefgrid-deny-internet-ingress"
  network       = google_compute_network.reliefgrid.id
  direction     = "INGRESS"
  priority      = 65000
  source_ranges = ["0.0.0.0/0"]

  deny {
    protocol = "all"
  }
}

# Traefik's LoadBalancer IP: stable across reinstalls, so the sslip.io name and the
# Let's Encrypt certificate stay valid (D6). Regional, removed by terraform destroy.
resource "google_compute_address" "ingress" {
  name         = "reliefgrid-ingress"
  region       = var.region
  address_type = "EXTERNAL"
  depends_on   = [google_project_service.enabled]
}
