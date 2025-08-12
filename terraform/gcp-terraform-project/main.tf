resource "google_project_service" "enable_apis" {
  for_each = toset([
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudscheduler.googleapis.com",
    "cloudbuild.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com"
  ])

  project = var.project
  service = each.key

  disable_on_destroy = false
}

resource "google_storage_bucket" "bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
}

resource "google_artifact_registry_repository" "repo" {
  project       = var.project
  location      = var.region
  repository_id = var.repo_id
  description   = "Docker repository for images"
  format        = "DOCKER"
}

resource "google_service_account" "run_sa" {
  account_id   = var.service_account_id
  display_name = "Cloud Run / Scheduler service account"
}

resource "google_project_iam_member" "sa_storage_access" {
  project = var.project
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.run_sa.email}"
}

resource "google_project_iam_member" "sa_artifact_registry_reader" {
  project = var.project
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.run_sa.email}"
}

resource "google_cloud_run_service" "default" {
  name     = var.cloud_run_service_name
  location = var.region

  template {
    spec {
      service_account_name = google_service_account.run_sa.email

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project}/${var.repo_id}/${var.image_name}:${var.image_tag}"
        ports {
          container_port = 8080
        }
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [
    google_project_service.enable_apis
  ]
}

resource "google_cloud_run_service_iam_member" "invoker" {
  location = google_cloud_run_service.default.location
  project  = var.project
  service  = google_cloud_run_service.default.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_cloud_scheduler_job" "call_run" {
  name        = var.scheduler_name
  description = "Call Cloud Run service periodically"
  schedule    = var.cron
  time_zone   = "UTC"

  http_target {
    http_method = "GET"
    uri         = google_cloud_run_service.default.status[0].url

    oidc_token {
      service_account_email = google_service_account.run_sa.email
      audience              = google_cloud_run_service.default.status[0].url
    }
  }

  depends_on = [
    google_cloud_run_service.default,
    google_project_service.enable_apis
  ]
}
