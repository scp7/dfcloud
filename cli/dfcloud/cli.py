#!/usr/bin/env python3
"""
dfcloud CLI - Run DeepFabric jobs on Google Cloud

Commands:
    submit   - Submit a new job
    status   - Check job status
    logs     - View job logs
    list     - List recent jobs
    download - Download job outputs
    config   - Manage CLI configuration
    init     - Initialize Spin service with tool schema and mock data
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import click
import requests
import yaml
from google.auth.transport.requests import Request
from google.cloud import run_v2, storage
from google.oauth2 import id_token
from rich.console import Console
from rich.progress import Progress
from rich.table import Table

console = Console()

# Default config location
CONFIG_DIR = Path.home() / ".dfcloud"
CONFIG_FILE = CONFIG_DIR / "config.yaml"


def load_config() -> dict:
    """Load CLI configuration."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            return yaml.safe_load(f) or {}
    return {}


def save_config(config: dict) -> None:
    """Save CLI configuration."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        yaml.safe_dump(config, f)


def get_config_value(key: str, required: bool = True) -> str | None:
    """Get a configuration value."""
    config = load_config()
    value = config.get(key) or os.environ.get(f"DFCLOUD_{key.upper()}")
    if required and not value:
        console.print(f"[red]Error:[/red] {key} not configured.")
        console.print(f"Run: dfcloud config set {key} <value>")
        sys.exit(1)
    return value


@click.group()
@click.version_option()
def cli():
    """DeepFabric Cloud CLI - Run DeepFabric jobs on Google Cloud."""
    pass


@cli.group()
def config():
    """Manage CLI configuration."""
    pass


@config.command("set")
@click.argument("key")
@click.argument("value")
def config_set(key: str, value: str):
    """Set a configuration value."""
    cfg = load_config()
    cfg[key] = value
    save_config(cfg)
    console.print(f"[green]Set {key}[/green]")


@config.command("get")
@click.argument("key")
def config_get(key: str):
    """Get a configuration value."""
    cfg = load_config()
    value = cfg.get(key)
    if value:
        console.print(value)
    else:
        console.print(f"[yellow]{key} not set[/yellow]")


@config.command("list")
def config_list():
    """List all configuration values."""
    cfg = load_config()
    if not cfg:
        console.print("[yellow]No configuration set[/yellow]")
        return

    table = Table(title="Configuration")
    table.add_column("Key", style="cyan")
    table.add_column("Value", style="green")

    for key, value in cfg.items():
        # Mask sensitive values
        if "secret" in key.lower() or "webhook" in key.lower():
            value = value[:20] + "..." if len(value) > 20 else value
        table.add_row(key, str(value))

    console.print(table)


@config.command("init")
@click.option("--project", prompt="GCP Project ID", help="Your GCP project ID")
@click.option("--region", default="us-central1", prompt="GCP Region", help="GCP region")
@click.option("--bucket", prompt="GCS Bucket Name", help="GCS bucket for configs/outputs")
@click.option("--progress-interval", default=900, prompt="Slack progress interval (seconds)", help="Seconds between Slack progress updates")
def config_init(project: str, region: str, bucket: str, progress_interval: int):
    """Initialize configuration interactively."""
    cfg = load_config()
    cfg["project_id"] = project
    cfg["region"] = region
    cfg["bucket"] = bucket
    cfg["job_name"] = "deepfabric-job"
    cfg["progress_interval"] = progress_interval
    save_config(cfg)
    console.print("[green]Configuration saved![/green]")
    console.print(f"Config file: {CONFIG_FILE}")


@cli.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.option("--name", "-n", help="Job name (defaults to config filename)")
@click.option("--wait/--no-wait", default=False, help="Wait for job completion")
@click.option("--timeout", default=3600, help="Job timeout in seconds")
@click.option("--topic-only", is_flag=True, help="Only generate topic graph (skip dataset generation)")
@click.option("--topics-load", type=str, help="GCS path to existing topic graph (e.g., outputs/job/timestamp/graph.jsonl)")
def submit(config_file: str, name: str | None, wait: bool, timeout: int, topic_only: bool, topics_load: str | None):
    """Submit a DeepFabric job.

    CONFIG_FILE is the path to your deepfabric YAML configuration file.

    Examples:

        # Run full pipeline (topics + dataset)
        dfcloud submit config.yaml

        # Generate only the topic graph
        dfcloud submit config.yaml --topic-only

        # Generate dataset using existing topic graph
        dfcloud submit config.yaml --topics-load outputs/my-job/20240115-120000/topics.jsonl
    """
    project_id = get_config_value("project_id")
    region = get_config_value("region")
    bucket = get_config_value("bucket")
    job_name = get_config_value("job_name")

    config_path = Path(config_file)
    run_name = name or config_path.stem

    # Generate unique config path in GCS
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    gcs_config_path = f"configs/{run_name}/{timestamp}/config.yaml"

    console.print(f"[bold]Submitting job:[/bold] {run_name}")

    # Upload config to GCS
    with console.status("Uploading config to GCS..."):
        client = storage.Client(project=project_id)
        bucket_obj = client.bucket(bucket)
        blob = bucket_obj.blob(gcs_config_path)
        blob.upload_from_filename(str(config_path))

    console.print(f"  Config: gs://{bucket}/{gcs_config_path}")

    if topic_only:
        console.print(f"  Mode: [cyan]Topic graph generation only[/cyan]")
    elif topics_load:
        console.print(f"  Mode: [cyan]Dataset generation[/cyan]")
        console.print(f"  Topics: gs://{bucket}/{topics_load}")

    # Execute Cloud Run Job
    with console.status("Starting Cloud Run Job..."):
        jobs_client = run_v2.JobsClient()
        job_path = f"projects/{project_id}/locations/{region}/jobs/{job_name}"

        # Build environment variables
        env_vars = [
            run_v2.EnvVar(name="CONFIG_PATH", value=gcs_config_path),
            run_v2.EnvVar(name="JOB_NAME", value=run_name),
        ]

        # Add progress interval from dfcloud config
        cfg = load_config()
        progress_interval = cfg.get("progress_interval", 900)
        env_vars.append(run_v2.EnvVar(name="PROGRESS_INTERVAL", value=str(progress_interval)))

        if topic_only:
            env_vars.append(run_v2.EnvVar(name="TOPIC_ONLY", value="true"))
        elif topics_load:
            env_vars.append(run_v2.EnvVar(name="TOPICS_LOAD", value=topics_load))

        # Create execution with overrides
        overrides = run_v2.RunJobRequest.Overrides(
            container_overrides=[
                run_v2.RunJobRequest.Overrides.ContainerOverride(env=env_vars)
            ],
            timeout=f"{timeout}s",
        )

        request = run_v2.RunJobRequest(name=job_path, overrides=overrides)

        operation = jobs_client.run_job(request=request)

    # Get execution name from operation metadata
    execution_name = None
    try:
        # The operation returns an Execution
        if hasattr(operation, "metadata") and operation.metadata:
            execution_name = operation.metadata.name
    except Exception:
        pass

    console.print(f"  [green]Job submitted![/green]")

    if execution_name:
        execution_id = execution_name.split("/")[-1]
        console.print(f"  Execution ID: {execution_id}")
        console.print(f"\nTo check status: dfcloud status {execution_id}")
        console.print(f"To view logs:    dfcloud logs {execution_id}")
    else:
        console.print("\nTo list executions: dfcloud list")

    if wait:
        console.print("\n[yellow]Waiting for job completion...[/yellow]")
        try:
            result = operation.result()
            if result.succeeded_count > 0:
                console.print("[green]Job completed successfully![/green]")
            else:
                console.print("[red]Job failed.[/red]")
                sys.exit(1)
        except Exception as e:
            console.print(f"[red]Job failed: {e}[/red]")
            sys.exit(1)


@cli.command("import-tools")
@click.option("--mcp-command", default="npx dataforseo-mcp-server@latest",
              help="Command to run the MCP server")
@click.option("--wait/--no-wait", default=True, help="Wait for completion")
def import_tools(mcp_command: str, wait: bool):
    """Import tool schemas from MCP server into Spin service.

    This runs a short-lived job that connects to the MCP server,
    fetches tool definitions, and loads them into the Spin service.

    Run this once after deploying, or when the MCP server tools change.
    """
    project_id = get_config_value("project_id")
    region = get_config_value("region")
    job_name = get_config_value("job_name")

    console.print("[bold]Importing tools into Spin service...[/bold]")
    console.print(f"  MCP command: {mcp_command}")

    # Execute Cloud Run Job with import-tools mode
    with console.status("Starting import-tools job..."):
        jobs_client = run_v2.JobsClient()
        job_path = f"projects/{project_id}/locations/{region}/jobs/{job_name}"

        # Create execution with JOB_MODE=import-tools
        overrides = run_v2.RunJobRequest.Overrides(
            container_overrides=[
                run_v2.RunJobRequest.Overrides.ContainerOverride(
                    env=[
                        run_v2.EnvVar(name="JOB_MODE", value="import-tools"),
                        run_v2.EnvVar(name="MCP_COMMAND", value=mcp_command),
                    ]
                )
            ],
            timeout="300s",  # 5 minute timeout for import-tools
        )

        request = run_v2.RunJobRequest(name=job_path, overrides=overrides)
        operation = jobs_client.run_job(request=request)

    # Get execution name
    execution_name = None
    try:
        if hasattr(operation, "metadata") and operation.metadata:
            execution_name = operation.metadata.name
    except Exception:
        pass

    console.print(f"  [green]Import job submitted![/green]")

    if execution_name:
        execution_id = execution_name.split("/")[-1]
        console.print(f"  Execution ID: {execution_id}")

    if wait:
        console.print("\n[yellow]Waiting for import to complete...[/yellow]")
        try:
            result = operation.result()
            if result.succeeded_count > 0:
                console.print("[green]Tools imported successfully![/green]")
            else:
                console.print("[red]Import failed.[/red]")
                if execution_name:
                    console.print(f"Check logs: dfcloud logs {execution_id}")
                sys.exit(1)
        except Exception as e:
            console.print(f"[red]Import failed: {e}[/red]")
            sys.exit(1)
    else:
        if execution_name:
            console.print(f"\nTo check status: dfcloud status {execution_id}")
            console.print(f"To view logs:    dfcloud logs {execution_id}")


@cli.command()
@click.argument("execution_id", required=False)
def status(execution_id: str | None):
    """Check job execution status.

    If EXECUTION_ID is not provided, shows status of the latest execution.
    """
    project_id = get_config_value("project_id")
    region = get_config_value("region")
    job_name = get_config_value("job_name")

    executions_client = run_v2.ExecutionsClient()

    if execution_id:
        # Get specific execution
        execution_path = f"projects/{project_id}/locations/{region}/jobs/{job_name}/executions/{execution_id}"
        try:
            execution = executions_client.get_execution(name=execution_path)
            _print_execution_status(execution)
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            sys.exit(1)
    else:
        # Get latest execution
        parent = f"projects/{project_id}/locations/{region}/jobs/{job_name}"
        try:
            executions = list(executions_client.list_executions(parent=parent))
            if not executions:
                console.print("[yellow]No executions found[/yellow]")
                return

            # Sort by creation time, newest first
            executions.sort(key=lambda x: x.create_time, reverse=True)
            _print_execution_status(executions[0])
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            sys.exit(1)


def _print_execution_status(execution):
    """Print execution status details."""
    execution_id = execution.name.split("/")[-1]

    # Determine status
    if execution.succeeded_count > 0:
        status = "[green]Succeeded[/green]"
    elif execution.failed_count > 0:
        status = "[red]Failed[/red]"
    elif execution.running_count > 0:
        status = "[yellow]Running[/yellow]"
    else:
        status = "[blue]Pending[/blue]"

    console.print(f"\n[bold]Execution:[/bold] {execution_id}")
    console.print(f"  Status: {status}")
    console.print(f"  Created: {execution.create_time}")

    if execution.completion_time:
        duration = execution.completion_time - execution.create_time
        console.print(f"  Completed: {execution.completion_time}")
        console.print(f"  Duration: {duration}")


@cli.command()
@click.argument("execution_id", required=False)
@click.option("--follow", "-f", is_flag=True, help="Follow logs in real-time")
def logs(execution_id: str | None, follow: bool):
    """View job logs.

    If EXECUTION_ID is not provided, shows logs for the latest execution.
    """
    project_id = get_config_value("project_id")
    region = get_config_value("region")
    job_name = get_config_value("job_name")

    # Build log query
    if execution_id:
        filter_str = f'resource.type="cloud_run_job" resource.labels.job_name="{job_name}" labels."run.googleapis.com/execution_name"="{execution_id}"'
    else:
        filter_str = f'resource.type="cloud_run_job" resource.labels.job_name="{job_name}"'

    # Use gcloud for log viewing (simpler than the logging API)
    import subprocess

    cmd = [
        "gcloud",
        "logging",
        "read",
        filter_str,
        f"--project={project_id}",
        "--limit=100",
        "--format=value(textPayload)",
    ]

    if follow:
        console.print("[yellow]Following logs (Ctrl+C to stop)...[/yellow]\n")
        # For follow mode, we need to use a different approach
        cmd = [
            "gcloud",
            "beta",
            "run",
            "jobs",
            "logs",
            "read",
            job_name,
            f"--project={project_id}",
            f"--region={region}",
            "--tail=100",
        ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.stdout:
            console.print(result.stdout)
        if result.stderr and "ERROR" in result.stderr:
            console.print(f"[red]{result.stderr}[/red]")
    except FileNotFoundError:
        console.print("[red]Error:[/red] gcloud CLI not found. Please install the Google Cloud SDK.")
        sys.exit(1)


@cli.command("list")
@click.option("--limit", "-l", default=10, help="Number of executions to show")
def list_executions(limit: int):
    """List recent job executions."""
    project_id = get_config_value("project_id")
    region = get_config_value("region")
    job_name = get_config_value("job_name")

    executions_client = run_v2.ExecutionsClient()
    parent = f"projects/{project_id}/locations/{region}/jobs/{job_name}"

    try:
        executions = list(executions_client.list_executions(parent=parent))
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)

    if not executions:
        console.print("[yellow]No executions found[/yellow]")
        return

    # Sort by creation time, newest first
    executions.sort(key=lambda x: x.create_time, reverse=True)
    executions = executions[:limit]

    table = Table(title=f"Recent Executions ({job_name})")
    table.add_column("Execution ID", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Created", style="dim")
    table.add_column("Duration", style="dim")

    for execution in executions:
        execution_id = execution.name.split("/")[-1]

        if execution.succeeded_count > 0:
            status = "[green]Succeeded[/green]"
        elif execution.failed_count > 0:
            status = "[red]Failed[/red]"
        elif execution.running_count > 0:
            status = "[yellow]Running[/yellow]"
        else:
            status = "[blue]Pending[/blue]"

        created = execution.create_time.strftime("%Y-%m-%d %H:%M") if execution.create_time else "-"

        if execution.completion_time and execution.create_time:
            duration = execution.completion_time - execution.create_time
            duration_str = str(duration).split(".")[0]  # Remove microseconds
        else:
            duration_str = "-"

        table.add_row(execution_id, status, created, duration_str)

    console.print(table)


@cli.command()
@click.argument("job_run_name")
@click.option("--output", "-o", type=click.Path(), help="Output directory (defaults to current dir)")
def download(job_run_name: str, output: str | None):
    """Download job outputs.

    JOB_RUN_NAME is the name you used when submitting the job.
    """
    project_id = get_config_value("project_id")
    bucket = get_config_value("bucket")

    output_dir = Path(output) if output else Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)

    client = storage.Client(project=project_id)
    bucket_obj = client.bucket(bucket)

    # List outputs for this job
    prefix = f"outputs/{job_run_name}/"
    blobs = list(bucket_obj.list_blobs(prefix=prefix))

    if not blobs:
        console.print(f"[yellow]No outputs found for job: {job_run_name}[/yellow]")
        console.print(f"Looked in: gs://{bucket}/{prefix}")
        return

    console.print(f"[bold]Found {len(blobs)} files[/bold]\n")

    for blob in blobs:
        if blob.name.endswith("/"):
            continue  # Skip folders

        # Create local path preserving structure
        relative_path = blob.name.replace(prefix, "")
        local_path = output_dir / relative_path
        local_path.parent.mkdir(parents=True, exist_ok=True)

        with console.status(f"Downloading {relative_path}..."):
            blob.download_to_filename(str(local_path))

        size_mb = blob.size / (1024 * 1024)
        console.print(f"  [green]✓[/green] {relative_path} ({size_mb:.1f} MB)")

    console.print(f"\n[green]Downloaded to: {output_dir}[/green]")


@cli.command()
@click.argument("job_name", required=False)
@click.option("--files", "-f", is_flag=True, help="Show all files with sizes and timestamps")
def outputs(job_name: str | None, files: bool):
    """List available outputs in GCS.

    If JOB_NAME is provided, lists files for that job.
    Use --files to show detailed file listing.

    Examples:

        # List all jobs with outputs
        dfcloud outputs

        # List files for a specific job
        dfcloud outputs my-job

        # Show all files across all jobs
        dfcloud outputs --files
    """
    project_id = get_config_value("project_id")
    bucket = get_config_value("bucket")

    client = storage.Client(project=project_id)
    bucket_obj = client.bucket(bucket)

    # If job_name provided, list files for that job
    if job_name:
        prefix = f"outputs/{job_name}/"
        blobs = list(bucket_obj.list_blobs(prefix=prefix))

        if not blobs:
            console.print(f"[yellow]No outputs found for job: {job_name}[/yellow]")
            return

        table = Table(title=f"Outputs for {job_name}")
        table.add_column("File", style="cyan")
        table.add_column("Size", style="green", justify="right")
        table.add_column("Created", style="dim")
        table.add_column("GCS Path", style="dim")

        for blob in sorted(blobs, key=lambda b: b.name):
            if blob.name.endswith("/"):
                continue

            relative_path = blob.name.replace(f"outputs/{job_name}/", "")
            size_kb = blob.size / 1024
            size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
            created = blob.time_created.strftime("%Y-%m-%d %H:%M") if blob.time_created else "-"
            # Show path that can be used with --topics-load
            gcs_path = blob.name

            table.add_row(relative_path, size_str, created, gcs_path)

        console.print(table)
        console.print(f"\nTo download: dfcloud download {job_name}")
        console.print(f"To use as topics: dfcloud submit config.yaml --topics-load <GCS Path>")
        return

    # List all job folders
    if files:
        # Show all files across all jobs
        prefix = "outputs/"
        blobs = list(bucket_obj.list_blobs(prefix=prefix))

        if not blobs:
            console.print("[yellow]No outputs found[/yellow]")
            return

        table = Table(title="All Output Files")
        table.add_column("Job", style="cyan")
        table.add_column("Timestamp", style="blue")
        table.add_column("File", style="green")
        table.add_column("Size", justify="right")
        table.add_column("GCS Path", style="dim")

        for blob in sorted(blobs, key=lambda b: b.name, reverse=True):
            if blob.name.endswith("/"):
                continue

            parts = blob.name.replace("outputs/", "").split("/")
            if len(parts) >= 3:
                job = parts[0]
                timestamp = parts[1]
                filename = "/".join(parts[2:])
            else:
                continue

            size_kb = blob.size / 1024
            size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"

            table.add_row(job, timestamp, filename, size_str, blob.name)

        console.print(table)
    else:
        # Just list job names
        prefix = "outputs/"
        iterator = bucket_obj.list_blobs(prefix=prefix, delimiter="/")

        job_folders = set()
        for blob in iterator:
            parts = blob.name.replace(prefix, "").split("/")
            if parts[0]:
                job_folders.add(parts[0])

        for prefix_name in iterator.prefixes:
            jn = prefix_name.replace("outputs/", "").rstrip("/")
            if jn:
                job_folders.add(jn)

        if not job_folders:
            console.print("[yellow]No outputs found[/yellow]")
            return

        table = Table(title="Available Job Outputs")
        table.add_column("Job Name", style="cyan")

        for jn in sorted(job_folders):
            table.add_row(jn)

        console.print(table)
        console.print(f"\nTo see files: dfcloud outputs <job-name>")
        console.print(f"To download:  dfcloud download <job-name>")


def get_identity_token(audience: str) -> str:
    """Get an identity token for authenticating with Cloud Run."""
    import subprocess

    # First try the Python library (works on GCP)
    try:
        token = id_token.fetch_id_token(Request(), audience)
        return token
    except Exception:
        pass

    # Fall back to gcloud CLI (works locally with user credentials)
    try:
        result = subprocess.run(
            ["gcloud", "auth", "print-identity-token"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Error getting identity token:[/red] {e.stderr}")
        console.print("Run: gcloud auth login")
        sys.exit(1)
    except FileNotFoundError:
        console.print("[red]Error:[/red] gcloud CLI not found")
        console.print("Install: https://cloud.google.com/sdk/docs/install")
        sys.exit(1)


def get_spin_service_url() -> str:
    """Get the Spin service URL from config or by querying Cloud Run."""
    # First check if it's in config
    cfg = load_config()
    if cfg.get("spin_service_url"):
        return cfg["spin_service_url"]

    # Otherwise, query Cloud Run
    project_id = get_config_value("project_id")
    region = get_config_value("region")

    try:
        from google.cloud import run_v2

        services_client = run_v2.ServicesClient()
        service_name = f"projects/{project_id}/locations/{region}/services/spin-service"
        service = services_client.get_service(name=service_name)
        return service.uri
    except Exception as e:
        console.print(f"[red]Error getting Spin service URL:[/red] {e}")
        console.print("You can set it manually: dfcloud config set spin_service_url <url>")
        sys.exit(1)


def check_tools_available(spin_url: str, headers: dict) -> list[str]:
    """Check what tools are available in the Spin service."""
    try:
        response = requests.get(
            f"{spin_url}/mock/list-tools",
            headers=headers,
            timeout=30,
        )
        if response.status_code == 200:
            data = response.json()
            # Handle different response formats
            if isinstance(data, list):
                return [t.get("name", t) if isinstance(t, dict) else t for t in data]
            elif isinstance(data, dict) and "tools" in data:
                return [t.get("name", t) if isinstance(t, dict) else t for t in data["tools"]]
        return []
    except Exception:
        return []


def load_mock_responses(spin_url: str, headers: dict, mock_data: dict) -> int:
    """Load default mock responses into Spin service."""
    responses = mock_data.get("mockResponses", {})
    loaded = 0

    for tool_name, data in responses.items():
        default_response = data.get("defaultResponse")
        if not default_response:
            continue

        try:
            response = requests.post(
                f"{spin_url}/mock/update-response",
                headers=headers,
                json={"name": tool_name, "mockResponse": default_response},
                timeout=30,
            )
            if response.status_code == 200:
                loaded += 1
            else:
                console.print(f"  [yellow]Warning:[/yellow] Failed to load response for {tool_name}")
        except Exception as e:
            console.print(f"  [yellow]Warning:[/yellow] Failed to load response for {tool_name}: {e}")

    return loaded


def load_fixtures(spin_url: str, headers: dict, mock_data: dict) -> int:
    """Load fixtures into Spin service."""
    fixtures = mock_data.get("fixtures", {})
    loaded = 0

    for tool_name, tool_fixtures in fixtures.items():
        for fixture in tool_fixtures:
            match = fixture.get("match")
            response = fixture.get("response")

            if not match or not response:
                continue

            try:
                resp = requests.post(
                    f"{spin_url}/mock/add-fixture",
                    headers=headers,
                    json={"name": tool_name, "match": match, "response": response},
                    timeout=30,
                )
                if resp.status_code == 200:
                    loaded += 1
            except Exception:
                pass  # Fixtures are optional, don't warn for each

    return loaded


def run_import_tools(spin_url: str, mcp_command: str, auth_token: str | None = None) -> bool:
    """Run deepfabric import-tools to register tools with Spin service."""
    import subprocess

    cmd = [
        "deepfabric", "import-tools",
        "--transport", "stdio",
        "--command", mcp_command,
        "--spin", spin_url,
    ]

    if auth_token:
        cmd.extend(["--header", f"Authorization=Bearer {auth_token}"])

    console.print(f"  Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )

        if result.returncode == 0:
            return True
        else:
            if result.stderr:
                console.print(f"  [red]Error:[/red] {result.stderr[:500]}")
            if result.stdout:
                console.print(f"  Output: {result.stdout[:500]}")
            return False

    except FileNotFoundError:
        console.print("  [red]Error:[/red] deepfabric not found. Install with: pip install deepfabric")
        return False
    except subprocess.TimeoutExpired:
        console.print("  [red]Error:[/red] import-tools timed out after 120s")
        return False
    except Exception as e:
        console.print(f"  [red]Error:[/red] {e}")
        return False


@cli.command()
@click.option(
    "--mock-data",
    type=click.Path(exists=True),
    help="Path to mock data JSON file (or downloads from GCS if not specified)",
)
@click.option(
    "--mcp-command",
    default="npx dataforseo-mcp-server@latest",
    help="Command to run the MCP server (default: npx dataforseo-mcp-server@latest)",
)
@click.option(
    "--skip-import-tools",
    is_flag=True,
    help="Skip running deepfabric import-tools (use if tools already registered)",
)
@click.option(
    "--upload-first",
    is_flag=True,
    help="Upload local mock-data file to GCS before initializing",
)
def init(mock_data: str | None, mcp_command: str, skip_import_tools: bool, upload_first: bool):
    """Initialize Spin service with tools and mock data.

    This command:
    1. Runs deepfabric import-tools to register tools from the MCP server
    2. Loads mock responses into the Spin service

    Run this after deploying infrastructure, from Cloud Shell or a GCP environment.

    Prerequisites:
    - deepfabric installed (pip install deepfabric)
    - npx/node installed (for running MCP server)

    Examples:

        # Full initialization (import tools + load mock data)
        dfcloud init

        # Skip import-tools if tools already registered
        dfcloud init --skip-import-tools

        # Use a different MCP server
        dfcloud init --mcp-command "npx my-mcp-server"

        # Upload mock data to GCS first
        dfcloud init --upload-first --mock-data mock.json
    """
    project_id = get_config_value("project_id")
    bucket = get_config_value("bucket")

    console.print("[bold]Initializing Spin service...[/bold]\n")

    # Get Spin service URL
    spin_url = get_spin_service_url()
    console.print(f"Spin service: {spin_url}")

    # Get identity token for authentication
    console.print("Getting authentication token...")
    token = get_identity_token(spin_url)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Handle file upload to GCS if requested
    storage_client = storage.Client(project=project_id)
    bucket_obj = storage_client.bucket(bucket)

    if upload_first and mock_data:
        console.print(f"Uploading mock data to GCS...")
        blob = bucket_obj.blob("init/mock-data.json")
        blob.upload_from_filename(mock_data)
        console.print(f"  Uploaded to gs://{bucket}/init/mock-data.json")

    # Load mock data
    mock_data_content = None
    if mock_data:
        console.print(f"Loading mock data from: {mock_data}")
        with open(mock_data, "r") as f:
            mock_data_content = json.load(f)
    else:
        # Try to download from GCS
        console.print(f"Downloading mock data from GCS...")
        try:
            blob = bucket_obj.blob("init/mock-data.json")
            content = blob.download_as_text()
            mock_data_content = json.loads(content)
            console.print(f"  Downloaded from gs://{bucket}/init/mock-data.json")
        except Exception as e:
            console.print(f"[red]Error:[/red] Could not load mock data from GCS: {e}")
            console.print("Upload mock data first: gsutil cp mock-data.json gs://{bucket}/init/")
            sys.exit(1)

    # Import tools from MCP server
    if not skip_import_tools:
        console.print("\nImporting tools from MCP server...")
        if run_import_tools(spin_url, mcp_command, auth_token=token):
            console.print("  [green]Tools imported successfully[/green]")
        else:
            console.print("  [yellow]Warning:[/yellow] Failed to import tools. You may need to run manually:")
            console.print(f"    deepfabric import-tools --transport stdio --command \"{mcp_command}\" --spin {spin_url} --header \"Authorization=Bearer $TOKEN\"")

    # Check available tools in Spin
    console.print("\nChecking available tools in Spin service...")
    available_tools = check_tools_available(spin_url, headers)
    if available_tools:
        console.print(f"  Found {len(available_tools)} tools available")
    else:
        console.print("  [yellow]Warning:[/yellow] No tools found. Import may have failed.")

    # Load mock responses
    console.print("Loading mock responses...")
    responses_count = len(mock_data_content.get("mockResponses", {}))
    responses_loaded = load_mock_responses(spin_url, headers, mock_data_content)
    console.print(f"  Loaded {responses_loaded}/{responses_count} mock responses")

    # Load fixtures
    console.print("Loading fixtures...")
    fixtures_loaded = load_fixtures(spin_url, headers, mock_data_content)
    console.print(f"  Loaded {fixtures_loaded} fixtures")

    # Verify with a health check
    console.print("\nVerifying Spin service...")
    try:
        response = requests.get(f"{spin_url}/vfs/health", headers=headers, timeout=10)
        if response.status_code == 200:
            console.print("  [green]Health check passed[/green]")
        else:
            console.print(f"  [yellow]Health check returned: {response.status_code}[/yellow]")
    except Exception as e:
        console.print(f"  [yellow]Could not verify health: {e}[/yellow]")

    # Test a tool call
    console.print("\nTesting tool execution...")
    try:
        test_payload = {
            "name": "ai_optimization_keyword_data_locations_and_languages",
            "arguments": {},
        }
        response = requests.post(
            f"{spin_url}/mock/execute",
            headers=headers,
            json=test_payload,
            timeout=30,
        )
        if response.status_code == 200:
            console.print("  [green]Tool execution test passed[/green]")
        else:
            console.print(f"  [yellow]Tool execution returned: {response.status_code}[/yellow]")
    except Exception as e:
        console.print(f"  [yellow]Could not test tool execution: {e}[/yellow]")

    console.print("\n[green]Spin service initialized successfully![/green]")
    console.print("\nYou can now submit jobs:")
    console.print("  dfcloud submit <config.yaml> --name <job-name>")


if __name__ == "__main__":
    cli()
