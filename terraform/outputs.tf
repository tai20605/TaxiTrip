output "gcs_bucket_name" {
  description = "The name of the created GCS bucket"
  value       = google_storage_bucket.rideshare_bucket.name
}

output "gcs_bucket_url" {
  description = "The URL of the created GCS bucket"
  value       = google_storage_bucket.rideshare_bucket.url
}

output "bq_dataset_id" {
  description = "The ID of the created BigQuery dataset"
  value       = google_bigquery_dataset.rideshare_dataset.dataset_id
}
