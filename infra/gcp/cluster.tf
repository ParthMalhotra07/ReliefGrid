# Zonal GKE Standard (plan §2): node pools with the kind roles (role=data, role=infra with the
# role=infra:NoSchedule taint), Dataplane V2 for NetworkPolicy, Workload Identity on.

locals {
  node_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
}

resource "google_container_cluster" "reliefgrid" {
  name     = var.cluster_name
  location = var.zone

  network    = google_compute_network.reliefgrid.id
  subnetwork = google_compute_subnetwork.nodes.id

  # No default node pool at all (provider 8.x: remove_default_node_pool is deprecated), so no
  # node ever boots with the default Compute service account; the two pools below are the nodes.
  initial_node_count = 0

  # terraform destroy must be able to remove it (gke-teardown.sh).
  deletion_protection = false

  # Dataplane V2 (Cilium) enforces NetworkPolicy; only settable at creation.
  datapath_provider = "ADVANCED_DATAPATH"
  networking_mode   = "VPC_NATIVE"
  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  release_channel {
    channel = "REGULAR"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  master_authorized_networks_config {
    dynamic "cidr_blocks" {
      for_each = var.admin_cidrs
      content {
        cidr_block   = cidr_blocks.value
        display_name = "operator"
      }
    }
  }

  addons_config {
    # NodeLocal DNSCache sends DNS to 169.254.x, outside the coordinator's kube-dns egress
    # rule (#40), so it stays off; kube-dns (not Cloud DNS) is the cluster DNS.
    dns_cache_config {
      enabled = false
    }
    # Traefik is the only ingress controller (§31): no GKE Ingress controller beside it.
    http_load_balancing {
      disabled = true
    }
    gce_persistent_disk_csi_driver_config {
      enabled = true
    }
  }

  # Within the free Cloud Logging/Monitoring allotment.
  logging_config {
    enable_components = ["SYSTEM_COMPONENTS"]
  }
  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS"]
    managed_prometheus {
      enabled = false
    }
  }

  maintenance_policy {
    daily_maintenance_window {
      start_time = "21:30" # UTC: 03:00 IST
    }
    maintenance_exclusion {
      exclusion_name = "s51-demo-window"
      start_time     = var.maintenance_exclusion_start
      end_time       = var.maintenance_exclusion_end
      exclusion_options {
        scope = "NO_MINOR_OR_NODE_UPGRADES"
      }
    }
  }

  depends_on = [google_project_service.enabled]
}

resource "google_container_node_pool" "data" {
  name       = "data"
  location   = var.zone
  cluster    = google_container_cluster.reliefgrid.name
  node_count = 3

  management {
    auto_repair  = true
    auto_upgrade = true # required on a release channel; the exclusion above holds it
  }

  node_config {
    machine_type    = var.data_machine_type
    disk_size_gb    = 30
    disk_type       = "pd-balanced"
    image_type      = "COS_CONTAINERD"
    service_account = google_service_account.nodes.email
    oauth_scopes    = local.node_scopes
    labels = {
      role = "data"
    }
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
  }
}

resource "google_container_node_pool" "infra" {
  name       = "infra"
  location   = var.zone
  cluster    = google_container_cluster.reliefgrid.name
  node_count = 1

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type    = var.infra_machine_type
    disk_size_gb    = 30
    disk_type       = "pd-balanced"
    image_type      = "COS_CONTAINERD"
    service_account = google_service_account.nodes.email
    oauth_scopes    = local.node_scopes
    labels = {
      role = "infra"
    }
    taint {
      key    = "role"
      value  = "infra"
      effect = "NO_SCHEDULE"
    }
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
  }
}
