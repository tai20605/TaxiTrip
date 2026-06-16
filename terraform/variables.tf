variable "project_id" {
  description = "The GCP Project ID where resources will be created"
  type        = string
}

variable "region" {
  description = "The GCP Region for GCS and BigQuery resources"
  type        = string
  default     = "us-east1"
}

variable "gcs_bucket_name" {
  description = "The name of the GCS bucket to store rideshare Parquet files"
  type        = string
}

variable "bq_dataset_id" {
  description = "The BigQuery Dataset ID"
  type        = string
  default     = "rideshare_dw"
}

variable "credentials_path" {
  description = "The local path to the GCP service account JSON key file"
  type        = string
  default     = "../credentials/service-account.json"
}
