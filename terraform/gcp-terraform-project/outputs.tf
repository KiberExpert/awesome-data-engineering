output "bucket_name" {
  value = google_storage_bucket.bucket.name
}

output "artifact_repo" {
  value = "${google_artifact_registry_repository.repo.location}/${google_artifact_registry_repository.repo.repository_id}"
}

output "cloud_run_url" {
  value = google_cloud_run_service.default.status[0].url
}

output "service_account_email" {
  value = google_service_account.run_sa.email
}
