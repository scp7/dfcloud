terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.project_id}-dfcloud"
}

# Enable required APIs
resource "google_project_service" "apis" {
  for_each = toset([
    "run.googleapis.com",
    "storage.googleapis.com",
    "secretmanager.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

# GCS bucket for configs and outputs
resource "google_storage_bucket" "dfcloud" {
  name                        = local.bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    condition {
      age = 90  # Clean up old outputs after 90 days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  depends_on = [google_project_service.apis]
}

# Create folders in bucket
resource "google_storage_bucket_object" "configs_folder" {
  name    = "configs/"
  bucket  = google_storage_bucket.dfcloud.name
  content = " "
}

resource "google_storage_bucket_object" "outputs_folder" {
  name    = "outputs/"
  bucket  = google_storage_bucket.dfcloud.name
  content = " "
}

# Secret for Slack webhook
resource "google_secret_manager_secret" "slack_webhook" {
  secret_id = "dfcloud-slack-webhook"

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "slack_webhook" {
  secret      = google_secret_manager_secret.slack_webhook.id
  secret_data = var.slack_webhook_url
}

# Secret for Google API key (Gemini)
resource "google_secret_manager_secret" "google_api_key" {
  secret_id = "dfcloud-google-api-key"

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "google_api_key" {
  secret      = google_secret_manager_secret.google_api_key.id
  secret_data = var.google_api_key
}

# Service account for DeepFabric job
resource "google_service_account" "deepfabric_job" {
  account_id   = "deepfabric-job"
  display_name = "DeepFabric Cloud Run Job"
}

# Service account for Spin service
resource "google_service_account" "spin_service" {
  account_id   = "spin-service"
  display_name = "Spin Cloud Run Service"
}

# IAM: DeepFabric job can read/write to GCS
resource "google_storage_bucket_iam_member" "deepfabric_storage" {
  bucket = google_storage_bucket.dfcloud.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.deepfabric_job.email}"
}

# IAM: DeepFabric job can access Slack webhook secret
resource "google_secret_manager_secret_iam_member" "deepfabric_slack" {
  secret_id = google_secret_manager_secret.slack_webhook.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.deepfabric_job.email}"
}

# IAM: DeepFabric job can access Google API key secret
resource "google_secret_manager_secret_iam_member" "deepfabric_google_api_key" {
  secret_id = google_secret_manager_secret.google_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.deepfabric_job.email}"
}

# IAM: DeepFabric job can invoke Spin service
resource "google_cloud_run_service_iam_member" "deepfabric_invoke_spin" {
  location = var.region
  service  = google_cloud_run_v2_service.spin.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.deepfabric_job.email}"
}


# Spin Cloud Run Service
resource "google_cloud_run_v2_service" "spin" {
  name     = "spin-service"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"  # Allow external access for dfcloud CLI

  template {
    service_account = google_service_account.spin_service.email

    scaling {
      min_instance_count = var.spin_min_instances
      max_instance_count = 10
    }

    containers {
      image = var.spin_image

      ports {
        container_port = 3000
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
      }

      startup_probe {
        http_get {
          path = "/vfs/health"
          port = 3000
        }
        initial_delay_seconds = 5
        period_seconds        = 10
        timeout_seconds       = 5
        failure_threshold     = 10
      }
    }
  }

  depends_on = [google_project_service.apis]
}

# DeepFabric Cloud Run Job
resource "google_cloud_run_v2_job" "deepfabric" {
  name     = "deepfabric-job"
  location = var.region

  template {
    template {
      service_account = google_service_account.deepfabric_job.email
      timeout         = "${var.deepfabric_job_timeout}s"

      containers {
        image = var.deepfabric_image

        resources {
          limits = {
            cpu    = var.deepfabric_job_cpu
            memory = var.deepfabric_job_memory
          }
        }

        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.dfcloud.name
        }

        env {
          name  = "SPIN_ENDPOINT"
          value = google_cloud_run_v2_service.spin.uri
        }

        env {
          name = "SLACK_WEBHOOK_URL"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.slack_webhook.id
              version = "latest"
            }
          }
        }

        env {
          name = "GOOGLE_API_KEY"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.google_api_key.id
              version = "latest"
            }
          }
        }

        # These will be overridden at job execution time
        env {
          name  = "CONFIG_PATH"
          value = ""
        }

        env {
          name  = "JOB_NAME"
          value = ""
        }
      }
    }
  }

  depends_on = [
    google_project_service.apis,
    google_cloud_run_v2_service.spin,
  ]
}

# Artifact Registry for container images
resource "google_artifact_registry_repository" "dfcloud" {
  repository_id = "dfcloud"
  location      = var.region
  format        = "DOCKER"
  description   = "Container images for DeepFabric Cloud"

  depends_on = [google_project_service.apis]
}
