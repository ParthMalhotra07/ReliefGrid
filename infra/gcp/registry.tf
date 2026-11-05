# D4: images are built once and pushed to GHCR by CI (the source of truth); while a session is
# up, CI copies each one by digest into this repository, so the digest pinned in the Helm
# values is unchanged. Deleted by terraform destroy.
resource "google_artifact_registry_repository" "reliefgrid" {
  repository_id = "reliefgrid"
  location      = var.region
  format        = "DOCKER"
  description   = "ReliefGrid images, copied by digest from GHCR"

  docker_config {
    immutable_tags = true
  }

  depends_on = [google_project_service.enabled]
}
