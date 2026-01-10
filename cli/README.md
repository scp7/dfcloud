# dfcloud CLI

Command-line interface for running DeepFabric jobs on Google Cloud.

## Installation

```bash
pip install -e .
```

## Quick Start

```bash
# Initialize configuration
dfcloud config init

# Submit a job
dfcloud submit path/to/config.yaml --name my-job

# Check status
dfcloud status

# View logs
dfcloud logs

# Download outputs
dfcloud download my-job
```

## Commands

| Command | Description |
|---------|-------------|
| `dfcloud submit` | Submit a new job |
| `dfcloud status` | Check job execution status |
| `dfcloud logs` | View job logs |
| `dfcloud list` | List recent executions |
| `dfcloud download` | Download job outputs |
| `dfcloud outputs` | List available outputs |
| `dfcloud config` | Manage CLI configuration |

## Configuration

Configuration is stored in `~/.dfcloud/config.yaml`.

Required settings:
- `project_id`: Your GCP project ID
- `region`: GCP region (e.g., us-central1)
- `bucket`: GCS bucket name for configs/outputs
- `job_name`: Cloud Run Job name (default: deepfabric-job)

You can also set these via environment variables:
- `DFCLOUD_PROJECT_ID`
- `DFCLOUD_REGION`
- `DFCLOUD_BUCKET`
- `DFCLOUD_JOB_NAME`
