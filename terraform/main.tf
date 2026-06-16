# ── GCS Bucket ───────────────────────────────────────────────────
resource "google_storage_bucket" "rideshare_bucket" {
  name          = var.gcs_bucket_name
  location      = "US"
  storage_class = "STANDARD"

  # Force destroy to allow clean deletion of bucket and files on terraform destroy
  force_destroy = true

  # Enable uniform bucket-level access control
  uniform_bucket_level_access = true

  # Auto-cleanup rule: delete files older than 30 days to optimize cost
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }
}

# ── BigQuery Dataset ──────────────────────────────────────────────
resource "google_bigquery_dataset" "rideshare_dataset" {
  dataset_id                  = var.bq_dataset_id
  friendly_name               = "Boston Rideshare Data Warehouse"
  description                 = "Data warehouse storage for Boston Rideshare Analytics Platform"
  location                    = var.region
  
  # Allow dataset deletion even if it contains tables
  delete_contents_on_destroy  = true
}

# ── BigQuery Dataset: rideshare_dw_dq_failures (lưu failing rows từ dbt tests) ─
resource "google_bigquery_dataset" "dq_failures_dataset" {
  dataset_id                 = "rideshare_dw_dq_failures"
  friendly_name              = "Data Quality Failures"
  description                = "Stores failing rows from dbt tests for audit and monitoring"
  location                   = var.region
  delete_contents_on_destroy = true
}

# ── BigQuery External Table pointing to GCS Bronze ───────────────
resource "google_bigquery_table" "external_rideshare_events" {
  dataset_id = google_bigquery_dataset.rideshare_dataset.dataset_id
  table_id   = "external_rideshare_events"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    # Match all Parquet files in the GCS bronze folder recursively
    source_uris   = ["gs://${google_storage_bucket.rideshare_bucket.name}/bronze/rideshare_events/*.parquet"]
  }
}

