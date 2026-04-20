terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.28.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "gcs-bucket" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition { age = 1 }
    action { type = "AbortIncompleteMultipartUpload" }
  }
}

resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "raw"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "staging" {
  dataset_id                 = "staging"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "mart" {
  dataset_id                 = "mart"
  location                   = var.region
  delete_contents_on_destroy = true
}
