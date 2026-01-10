#!/usr/bin/env python3
"""
DeepFabric Cloud Run Job Entrypoint

Downloads config from GCS, runs deepfabric, uploads outputs, and notifies via Slack.
"""

import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import requests
import yaml
from google.cloud import storage


def get_env(name: str, required: bool = True) -> str | None:
    """Get environment variable."""
    value = os.environ.get(name)
    if required and not value:
        raise ValueError(f"Required environment variable {name} is not set")
    return value


def download_from_gcs(bucket_name: str, blob_path: str, local_path: Path) -> None:
    """Download a file from GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.download_to_filename(str(local_path))
    print(f"Downloaded gs://{bucket_name}/{blob_path} to {local_path}")


def upload_to_gcs(local_path: Path, bucket_name: str, blob_path: str) -> str:
    """Upload a file to GCS. Returns the gs:// URL."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))
    url = f"gs://{bucket_name}/{blob_path}"
    print(f"Uploaded {local_path} to {url}")
    return url


def update_config_spin_endpoint(config_path: Path, spin_endpoint: str) -> None:
    """Update the config file to use the Cloud Run Spin endpoint."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Update spin_endpoint in generation.tools if present
    if "generation" in config and "tools" in config["generation"]:
        tools_config = config["generation"]["tools"]
        if "spin_endpoint" in tools_config:
            original = tools_config["spin_endpoint"]
            tools_config["spin_endpoint"] = spin_endpoint
            print(f"Updated spin_endpoint: {original} -> {spin_endpoint}")

        # Also update tools_endpoint if it references localhost
        if "tools_endpoint" in tools_config:
            original = tools_config["tools_endpoint"]
            if "localhost" in original:
                # Replace localhost:port with spin_endpoint
                new_endpoint = original.replace("http://localhost:3000", spin_endpoint)
                tools_config["tools_endpoint"] = new_endpoint
                print(f"Updated tools_endpoint: {original} -> {new_endpoint}")

    with open(config_path, "w") as f:
        yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)


def get_output_files_from_config(config_path: Path) -> list[str]:
    """Extract output file paths from config."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    outputs = []

    # Topic graph output
    if "topics" in config and "save_as" in config["topics"]:
        outputs.append(config["topics"]["save_as"])

    # Dataset output
    if "output" in config and "save_as" in config["output"]:
        outputs.append(config["output"]["save_as"])

    return outputs


def send_slack_notification(
    webhook_url: str,
    job_name: str,
    status: str,
    duration_seconds: float,
    output_urls: list[str],
    error_message: str | None = None,
) -> None:
    """Send a notification to Slack."""
    duration_min = duration_seconds / 60

    if status == "success":
        emoji = ":white_check_mark:"
        color = "#36a64f"
        title = f"{emoji} Job Completed: {job_name}"
    else:
        emoji = ":x:"
        color = "#ff0000"
        title = f"{emoji} Job Failed: {job_name}"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": title, "emoji": True},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Status:*\n{status.title()}"},
                {"type": "mrkdwn", "text": f"*Duration:*\n{duration_min:.1f} minutes"},
            ],
        },
    ]

    if output_urls:
        outputs_text = "\n".join([f"• `{url}`" for url in output_urls])
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Outputs:*\n{outputs_text}"},
            }
        )

    if error_message:
        # Truncate error message if too long
        if len(error_message) > 500:
            error_message = error_message[:500] + "..."
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Error:*\n```{error_message}```"},
            }
        )

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"DeepFabric Cloud • {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
                }
            ],
        }
    )

    payload = {"blocks": blocks, "attachments": [{"color": color, "blocks": []}]}

    try:
        response = requests.post(webhook_url, json=payload, timeout=30)
        response.raise_for_status()
        print("Slack notification sent successfully")
    except Exception as e:
        print(f"Warning: Failed to send Slack notification: {e}")


def run_deepfabric(config_path: Path, work_dir: Path) -> tuple[bool, str]:
    """Run deepfabric generate command. Returns (success, output/error)."""
    cmd = ["deepfabric", "generate", str(config_path)]

    print(f"Running: {' '.join(cmd)}")
    print(f"Working directory: {work_dir}")

    try:
        result = subprocess.run(
            cmd,
            cwd=str(work_dir),
            capture_output=True,
            text=True,
            timeout=int(get_env("DEEPFABRIC_TIMEOUT", required=False) or 86400),
        )

        # Print output for logging
        if result.stdout:
            print("STDOUT:", result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        if result.returncode == 0:
            return True, result.stdout
        else:
            return False, result.stderr or result.stdout or f"Exit code: {result.returncode}"

    except subprocess.TimeoutExpired as e:
        return False, f"Job timed out after {e.timeout} seconds"
    except Exception as e:
        return False, str(e)


def main():
    start_time = time.time()

    # Get configuration from environment
    gcs_bucket = get_env("GCS_BUCKET")
    config_path = get_env("CONFIG_PATH")  # e.g., "configs/my-job.yaml"
    job_name = get_env("JOB_NAME")
    spin_endpoint = get_env("SPIN_ENDPOINT")
    slack_webhook_url = get_env("SLACK_WEBHOOK_URL")

    print(f"Starting job: {job_name}")
    print(f"Config: gs://{gcs_bucket}/{config_path}")
    print(f"Spin endpoint: {spin_endpoint}")

    # Create working directory
    with tempfile.TemporaryDirectory() as work_dir:
        work_path = Path(work_dir)
        local_config = work_path / "config.yaml"

        try:
            # Download config from GCS
            download_from_gcs(gcs_bucket, config_path, local_config)

            # Update config to use Cloud Run Spin endpoint
            update_config_spin_endpoint(local_config, spin_endpoint)

            # Get expected output files
            expected_outputs = get_output_files_from_config(local_config)
            print(f"Expected outputs: {expected_outputs}")

            # Run deepfabric
            success, output = run_deepfabric(local_config, work_path)

            # Upload outputs to GCS
            output_urls = []
            timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            output_prefix = f"outputs/{job_name}/{timestamp}"

            for output_file in expected_outputs:
                local_output = work_path / output_file
                if local_output.exists():
                    gcs_path = f"{output_prefix}/{output_file}"
                    url = upload_to_gcs(local_output, gcs_bucket, gcs_path)
                    output_urls.append(url)
                else:
                    print(f"Warning: Expected output {output_file} not found")

            # Calculate duration
            duration = time.time() - start_time

            # Send notification
            send_slack_notification(
                webhook_url=slack_webhook_url,
                job_name=job_name,
                status="success" if success else "failed",
                duration_seconds=duration,
                output_urls=output_urls,
                error_message=None if success else output,
            )

            if not success:
                print(f"Job failed: {output}")
                sys.exit(1)

            print(f"Job completed successfully in {duration/60:.1f} minutes")

        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            print(f"Job failed with exception: {error_msg}")

            send_slack_notification(
                webhook_url=slack_webhook_url,
                job_name=job_name,
                status="failed",
                duration_seconds=duration,
                output_urls=[],
                error_message=error_msg,
            )

            sys.exit(1)


if __name__ == "__main__":
    main()
