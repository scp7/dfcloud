output "bucket_name" {
  description = "GCS bucket name for configs and outputs"
  value       = google_storage_bucket.dfcloud.name
}

output "bucket_url" {
  description = "GCS bucket URL"
  value       = "gs://${google_storage_bucket.dfcloud.name}"
}

output "spin_service_url" {
  description = "Spin service internal URL"
  value       = google_cloud_run_v2_service.spin.uri
}

output "deepfabric_job_name" {
  description = "DeepFabric Cloud Run Job name"
  value       = google_cloud_run_v2_job.deepfabric.name
}

output "artifact_registry" {
  description = "Artifact Registry URL for container images"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.dfcloud.repository_id}"
}

output "deepfabric_service_account" {
  description = "Service account email for DeepFabric job"
  value       = google_service_account.deepfabric_job.email
}

output "spin_service_account" {
  description = "Service account email for Spin service"
  value       = google_service_account.spin_service.email
}
