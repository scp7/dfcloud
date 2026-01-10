# DeepFabric Cloud (dfcloud)

Run DeepFabric jobs on Google Cloud with Slack notifications.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Google Cloud                            │
│                                                              │
│  ┌─────────────────┐         ┌─────────────────────────┐    │
│  │  Cloud Run      │  HTTP   │  Cloud Run Service      │    │
│  │  Job            │────────▶│  (Spin + mock data)     │    │
│  │  (deepfabric)   │         │  always-on              │    │
│  └────────┬────────┘         └─────────────────────────┘    │
│           │                                                  │
│           │ read config / write output                       │
│           ▼                                                  │
│  ┌─────────────────┐                                        │
│  │  Cloud Storage  │                                        │
│  │  (GCS bucket)   │                                        │
│  └─────────────────┘                                        │
└───────────┬─────────────────────────────────────────────────┘
            │
            │ on completion
            ▼
   ┌─────────────────┐
   │  Slack Webhook  │
   └─────────────────┘
```

## Prerequisites

- Google Cloud account with billing enabled
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) installed and authenticated
- [Terraform](https://developer.hashicorp.com/terraform/downloads) >= 1.0
- Docker for building container images
- Python >= 3.10 for the CLI

## Quick Start

### 1. Set up Slack Webhook

1. Go to [api.slack.com/apps](https://api.slack.com/apps)
2. Create a new app → "From scratch"
3. Add feature → "Incoming Webhooks" → Enable
4. Click "Add New Webhook to Workspace" → Select your channel
5. Copy the webhook URL

### 2. Configure Terraform

```bash
cd infra

# Copy and edit the example config
cp terraform.tfvars.example terraform.tfvars

# Edit terraform.tfvars with your values:
# - project_id
# - spin_image (we'll create this next)
# - deepfabric_image (we'll create this next)
# - slack_webhook_url
```

### 3. Build and Push Container Images

First, deploy infrastructure to create the Artifact Registry:

```bash
cd infra

# Initialize and apply just the registry first
terraform init
terraform apply -target=google_artifact_registry_repository.dfcloud -target=google_project_service.apis
```

Get the registry URL:

```bash
terraform output artifact_registry
# Output: us-central1-docker.pkg.dev/your-project/dfcloud
```

Build and push the DeepFabric job image:

```bash
cd ../deepfabric-job

# Configure Docker for Artifact Registry
gcloud auth configure-docker us-central1-docker.pkg.dev

# Build and push
REGISTRY=$(cd ../infra && terraform output -raw artifact_registry)
docker build -t $REGISTRY/deepfabric-job:latest .
docker push $REGISTRY/deepfabric-job:latest
```

Build and push your Spin service image (assuming you have it):

```bash
# From your spin tools-sdk directory
docker build -t $REGISTRY/spin:latest .
docker push $REGISTRY/spin:latest
```

### 4. Deploy Infrastructure

```bash
cd infra

# Update terraform.tfvars with the image paths
# Then apply all resources
terraform apply
```

### 5. Install the CLI

```bash
cd cli
pip install -e .

# Initialize configuration
dfcloud config init
# Enter: project_id, region, bucket name
```

### 6. Load Mock Data into Spin (if needed)

If your Spin service needs mock data loaded:

```bash
# Get the Spin service URL
cd infra
terraform output spin_service_url

# Load mock data (adjust for your setup)
curl -X POST "$(terraform output -raw spin_service_url)/mock/load" \
  -H "Content-Type: application/json" \
  -d @path/to/mock-data.json
```

## Usage

### Submit a Job

```bash
# Submit a job from a config file
dfcloud submit spin-dataforseo-5x5.yaml --name seo-dataset-v1

# Submit and wait for completion
dfcloud submit spin-dataforseo.yaml --name my-job --wait

# With custom timeout (in seconds)
dfcloud submit config.yaml --timeout 7200
```

### Check Job Status

```bash
# Check latest execution
dfcloud status

# Check specific execution
dfcloud status abc123-def456
```

### View Logs

```bash
# View logs for latest execution
dfcloud logs

# View logs for specific execution
dfcloud logs abc123-def456

# Follow logs in real-time
dfcloud logs -f
```

### List Executions

```bash
# List recent executions
dfcloud list

# Show more executions
dfcloud list --limit 20
```

### Download Outputs

```bash
# List available outputs
dfcloud outputs

# Download outputs for a job
dfcloud download seo-dataset-v1

# Download to specific directory
dfcloud download seo-dataset-v1 --output ./my-outputs
```

### Configuration

```bash
# Initialize config interactively
dfcloud config init

# Set individual values
dfcloud config set project_id my-project
dfcloud config set region us-central1
dfcloud config set bucket my-bucket-dfcloud

# List current config
dfcloud config list

# Get a specific value
dfcloud config get project_id
```

## Configuration File Format

Your DeepFabric YAML configs work as-is. The job runner automatically:

1. Downloads the config from GCS
2. Updates `spin_endpoint` to point to the Cloud Run Spin service
3. Updates `tools_endpoint` if it references localhost
4. Runs `deepfabric generate`
5. Uploads outputs to GCS
6. Sends Slack notification

Example config structure:

```yaml
topics:
  prompt: "Tasks for an SEO assistant..."
  mode: graph
  depth: 5
  degree: 5
  save_as: "topic-graph.jsonl"  # Will be uploaded to GCS
  llm:
    provider: "gemini"
    model: "gemini-2.5-flash"

generation:
  tools:
    spin_endpoint: "http://localhost:3000"  # Auto-updated to Cloud Run URL
    tools_endpoint: "http://localhost:3000/mock/list-tools"
  # ... rest of config

output:
  num_samples: 750
  save_as: "dataset.jsonl"  # Will be uploaded to GCS
```

## Outputs

Job outputs are stored in GCS at:

```
gs://{bucket}/outputs/{job-name}/{timestamp}/
├── topic-graph.jsonl
└── dataset.jsonl
```

## Slack Notifications

You'll receive notifications for:

- **Job completed**: Shows duration and output file locations
- **Job failed**: Shows error message and duration

Example notification:

```
✅ Job Completed: seo-dataset-v1

Status: Succeeded
Duration: 45.2 minutes

Outputs:
• gs://my-bucket/outputs/seo-dataset-v1/20240110-143022/topic-graph.jsonl
• gs://my-bucket/outputs/seo-dataset-v1/20240110-143022/dataset.jsonl
```

## Cost Optimization

- **Spin service**: Set `spin_min_instances = 0` for scale-to-zero (adds cold start latency)
- **Job resources**: Adjust `deepfabric_job_memory` and `deepfabric_job_cpu` based on your workloads
- **Storage**: Outputs older than 90 days are automatically moved to Nearline storage

## Troubleshooting

### Job fails immediately

Check that:
1. The Spin service is running: `gcloud run services describe spin-service --region us-central1`
2. Config file exists in GCS: `gsutil ls gs://your-bucket/configs/`
3. Service account has correct permissions

### Can't connect to Spin

The DeepFabric job needs the `run.invoker` role on the Spin service. Verify:

```bash
gcloud run services get-iam-policy spin-service --region us-central1
```

### Logs show authentication errors

Ensure your local gcloud is authenticated:

```bash
gcloud auth login
gcloud auth application-default login
```

## Cleanup

To destroy all resources:

```bash
cd infra
terraform destroy
```

Note: This will delete the GCS bucket and all outputs. Back up any important data first.
