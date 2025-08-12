variable "project" {
  description = "GCP project id"
  type        = string
}

variable "region" {
  description = "GCP region (e.g. us-central1)"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Storage bucket name (globally unique)"
  type        = string
}

variable "repo_id" {
  description = "Artifact Registry repository id"
  type        = string
  default     = "my-repo"
}

variable "image_name" {
  description = "Container image name (without tag)"
  type        = string
  default     = "sample-app"
}

variable "image_tag" {
  description = "Image tag"
  type        = string
  default     = "v1"
}

variable "cloud_run_service_name" {
  description = "Cloud Run service name"
  type        = string
  default     = "my-cloud-run-service"
}

variable "service_account_id" {
  description = "Service account id (no domain)"
  type        = string
  default     = "cloud-run-sa"
}

variable "scheduler_name" {
  description = "Cloud Scheduler job name"
  type        = string
  default     = "call-cloud-run-job"
}

variable "cron" {
  description = "Cron schedule for Cloud Scheduler"
  type        = string
  default     = "*/5 * * * *"
}

variable "project_users" {
  description = "List of user emails with viewer access"
  type        = list(string)
  default     = ["maqsudjonhamidov@gamil.com", "webpantesting@gmail.com"]
}
