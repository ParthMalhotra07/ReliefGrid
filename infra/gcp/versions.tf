terraform {
  required_version = ">= 1.17.1, < 2.0.0" # tools.mk TERRAFORM_VERSION

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.10"
    }
  }

  # Remote state in a GCS bucket made once by hand (README.md): uniform access, versioning on,
  # only the two owners as members. The bucket is given at init, never committed:
  #   terraform init -backend-config="bucket=<state bucket>"
  backend "gcs" {
    prefix = "reliefgrid/s51"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone

  # The Billing Budgets API needs a quota project when called with user credentials.
  user_project_override = true
  billing_project       = var.project_id
}
