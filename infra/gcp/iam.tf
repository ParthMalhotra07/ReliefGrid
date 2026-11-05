# Identities (plan §6). No service-account keys anywhere.

# --- Nodes: a dedicated, minimal service account instead of the default Compute (Editor) one.
resource "google_service_account" "nodes" {
  account_id   = "reliefgrid-nodes"
  display_name = "ReliefGrid GKE nodes"
  depends_on   = [google_project_service.enabled]
}

# Logs, metrics and metadata for the node agents: GKE's predefined minimal node role.
resource "google_project_iam_member" "nodes_default" {
  project = var.project_id
  role    = "roles/container.defaultNodeServiceAccount"
  member  = "serviceAccount:${google_service_account.nodes.email}"
}

resource "google_artifact_registry_repository_iam_member" "nodes_pull" {
  location   = google_artifact_registry_repository.reliefgrid.location
  repository = google_artifact_registry_repository.reliefgrid.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.nodes.email}"
}

# --- CI: GitHub Actions on main only, keyless through Workload Identity Federation. It may
# push to the one registry repository and nothing else (D7: CI does not deploy).
resource "google_service_account" "ci" {
  account_id   = "reliefgrid-ci"
  display_name = "ReliefGrid CI (GitHub Actions, main only)"
  depends_on   = [google_project_service.enabled]
}

resource "google_artifact_registry_repository_iam_member" "ci_push" {
  location   = google_artifact_registry_repository.reliefgrid.location
  repository = google_artifact_registry_repository.reliefgrid.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.ci.email}"
}

resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github"
  display_name              = "GitHub Actions"
  depends_on                = [google_project_service.enabled]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-oidc"
  display_name                       = "GitHub OIDC (main)"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }
  # Tokens from any other repository or ref are rejected before any impersonation.
  attribute_condition = "assertion.repository == '${var.github_repository}' && assertion.ref == 'refs/heads/main'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account_iam_member" "ci_wif" {
  service_account_id = google_service_account.ci.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repository}"
}
