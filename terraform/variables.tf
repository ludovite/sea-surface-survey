variable "project" {
  description = "GCP project ID (set via TF_VAR_project / PROJECT_NAME)"
  type        = string
}

variable "region" {
  description = "GCP region (set via TF_VAR_region / GCP_LOCATION)"
  type        = string
}

variable "gcs_bucket_name" {
  description = "GCS bucket name (set via TF_VAR_gcs_bucket_name / GCP_BUCKET)"
  type        = string
}

