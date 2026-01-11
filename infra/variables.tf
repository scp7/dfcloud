variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "spin_image" {
  description = "Docker image for Spin service"
  type        = string
  # Cloud Run only supports gcr.io, docker.pkg.dev, or docker.io
  # The ghcr.io image must be mirrored to Artifact Registry
}

variable "deepfabric_image" {
  description = "Docker image for DeepFabric job"
  type        = string
}

variable "slack_webhook_url" {
  description = "Slack webhook URL for notifications"
  type        = string
  sensitive   = true
}

variable "bucket_name" {
  description = "GCS bucket name for configs and outputs"
  type        = string
  default     = ""  # Will default to {project_id}-dfcloud
}

variable "spin_min_instances" {
  description = "Minimum instances for Spin service (0 for scale to zero)"
  type        = number
  default     = 1
}

variable "deepfabric_job_timeout" {
  description = "Timeout for DeepFabric job in seconds (max 86400 = 24h)"
  type        = number
  default     = 3600  # 1 hour default
}

variable "deepfabric_job_memory" {
  description = "Memory for DeepFabric job"
  type        = string
  default     = "4Gi"
}

variable "deepfabric_job_cpu" {
  description = "CPU for DeepFabric job"
  type        = string
  default     = "2"
}

variable "google_api_key" {
  description = "Google API key for Gemini"
  type        = string
  sensitive   = true
}
