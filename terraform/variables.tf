variable "project" {
  description = "Project"
  type = string
  default     = "global-sea-survey"
}

variable "region" {
  description = "Region"
  type = string
  default     = "europe-west9" # Paris, low CO₂
}

variable "gcs_bucket_name" {
  description = "Storage Bucket Name"
  type = string
  default     = "global-sea-survey-bucket"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  type = string
  default     = "global_sea_survey_bq_dataset"
}
