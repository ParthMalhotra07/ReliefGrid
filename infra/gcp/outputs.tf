# Read by scripts/gke-deploy.sh and scripts/gke-teardown.sh (terraform output -json) and
# saved as evidence. None of them is a credential.

output "project_id" {
  value = var.project_id
}

output "region" {
  value = var.region
}

output "zone" {
  value = var.zone
}

output "cluster_name" {
  value = google_container_cluster.reliefgrid.name
}

output "kube_context" {
  description = "The context gcloud container clusters get-credentials writes."
  value       = "gke_${var.project_id}_${var.zone}_${google_container_cluster.reliefgrid.name}"
}

output "registry" {
  description = "Image prefix: <registry>/<name>@sha256:..."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.reliefgrid.repository_id}"
}

output "ingress_ip" {
  description = "Traefik's LoadBalancer IP; the public host is <ip>.sslip.io (D6)."
  value       = google_compute_address.ingress.address
}

output "wif_provider" {
  description = "Set as the repository variable GCP_WORKLOAD_IDENTITY_PROVIDER while a session is up."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "ci_service_account" {
  description = "Set as the repository variable GCP_CI_SERVICE_ACCOUNT while a session is up."
  value       = google_service_account.ci.email
}
