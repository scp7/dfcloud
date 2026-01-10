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
def config_init(project: str, region: str, bucket: str):
    """Initialize configuration interactively."""
    cfg = load_config()
    cfg["project_id"] = project
    cfg["region"] = region
    cfg["bucket"] = bucket
    cfg["job_name"] = "deepfabric-job"
    save_config(cfg)
    console.print("[green]Configuration saved![/green]")
    console.print(f"Config file: {CONFIG_FILE}")


@cli.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.option("--name", "-n", help="Job name (defaults to config filename)")
@click.option("--wait/--no-wait", default=False, help="Wait for job completion")
@click.option("--timeout", default=3600, help="Job timeout in seconds")
def submit(config_file: str, name: str | None, wait: bool, timeout: int):
    """Submit a DeepFabric job.

    CONFIG_FILE is the path to your deepfabric YAML configuration file.
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

    # Execute Cloud Run Job
    with console.status("Starting Cloud Run Job..."):
        jobs_client = run_v2.JobsClient()
        job_path = f"projects/{project_id}/locations/{region}/jobs/{job_name}"

        # Create execution with overrides
        overrides = run_v2.RunJobRequest.Overrides(
            container_overrides=[
                run_v2.RunJobRequest.Overrides.ContainerOverride(
                    env=[
                        run_v2.EnvVar(name="CONFIG_PATH", value=gcs_config_path),
                        run_v2.EnvVar(name="JOB_NAME", value=run_name),
                    ]
                )
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
def outputs():
    """List available outputs in GCS."""
    project_id = get_config_value("project_id")
    bucket = get_config_value("bucket")

    client = storage.Client(project=project_id)
    bucket_obj = client.bucket(bucket)

    # List all job output folders
    prefix = "outputs/"
    iterator = bucket_obj.list_blobs(prefix=prefix, delimiter="/")

    # Get prefixes (folders)
    job_folders = set()
    for blob in iterator:
        parts = blob.name.replace(prefix, "").split("/")
        if parts[0]:
            job_folders.add(parts[0])

    # Also check prefixes
    for prefix_name in iterator.prefixes:
        job_name = prefix_name.replace("outputs/", "").rstrip("/")
        if job_name:
            job_folders.add(job_name)

    if not job_folders:
        console.print("[yellow]No outputs found[/yellow]")
        return

    table = Table(title="Available Job Outputs")
    table.add_column("Job Name", style="cyan")

    for job_name in sorted(job_folders):
        table.add_row(job_name)

    console.print(table)
    console.print(f"\nTo download: dfcloud download <job-name>")


def get_identity_token(audience: str) -> str:
    """Get an identity token for authenticating with Cloud Run."""
    try:
        token = id_token.fetch_id_token(Request(), audience)
        return token
    except Exception as e:
        console.print(f"[red]Error getting identity token:[/red] {e}")
        console.print("Make sure you're running from Cloud Shell or have application default credentials set up.")
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


def load_tools_into_spin(spin_url: str, headers: dict, schema: dict) -> int:
    """Load tool definitions into Spin service."""
    tools = schema.get("tools", [])
    loaded = 0

    for tool in tools:
        try:
            response = requests.post(
                f"{spin_url}/mock/register-tool",
                headers=headers,
                json=tool,
                timeout=30,
            )
            if response.status_code == 200:
                loaded += 1
            else:
                console.print(f"  [yellow]Warning:[/yellow] Failed to register {tool['name']}: {response.text}")
        except Exception as e:
            console.print(f"  [yellow]Warning:[/yellow] Failed to register {tool['name']}: {e}")

    return loaded


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


@cli.command()
@click.option(
    "--schema",
    type=click.Path(exists=True),
    help="Path to schema YAML file (or downloads from GCS if not specified)",
)
@click.option(
    "--mock-data",
    type=click.Path(exists=True),
    help="Path to mock data JSON file (or downloads from GCS if not specified)",
)
@click.option(
    "--upload-first",
    is_flag=True,
    help="Upload local schema and mock-data files to GCS before initializing",
)
def init(schema: str | None, mock_data: str | None, upload_first: bool):
    """Initialize Spin service with tool schema and mock data.

    This command loads tool definitions and mock responses into the Spin service.
    Run this after deploying infrastructure, from Cloud Shell or a GCP environment.

    Files can be provided locally or will be downloaded from GCS (gs://bucket/init/).

    Examples:

        # Using files from GCS (upload them first with --upload-first)
        dfcloud init --upload-first --schema schema.yaml --mock-data mock.json

        # Using files already in GCS
        dfcloud init

        # Using local files (must run from environment with Cloud Run access)
        dfcloud init --schema schema.yaml --mock-data mock.json
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

    if upload_first:
        if schema:
            console.print(f"Uploading schema to GCS...")
            blob = bucket_obj.blob("init/schema.yaml")
            blob.upload_from_filename(schema)
            console.print(f"  Uploaded to gs://{bucket}/init/schema.yaml")

        if mock_data:
            console.print(f"Uploading mock data to GCS...")
            blob = bucket_obj.blob("init/mock-data.json")
            blob.upload_from_filename(mock_data)
            console.print(f"  Uploaded to gs://{bucket}/init/mock-data.json")

    # Load schema
    schema_data = None
    if schema:
        console.print(f"Loading schema from: {schema}")
        with open(schema, "r") as f:
            schema_data = yaml.safe_load(f)
    else:
        # Try to download from GCS
        console.print(f"Downloading schema from GCS...")
        try:
            blob = bucket_obj.blob("init/schema.yaml")
            schema_content = blob.download_as_text()
            schema_data = yaml.safe_load(schema_content)
            console.print(f"  Downloaded from gs://{bucket}/init/schema.yaml")
        except Exception as e:
            console.print(f"[red]Error:[/red] Could not load schema from GCS: {e}")
            console.print("Upload schema first: gsutil cp schema.yaml gs://{bucket}/init/")
            sys.exit(1)

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

    # Load tools into Spin
    console.print("\nLoading tool definitions...")
    tools_count = len(schema_data.get("tools", []))
    tools_loaded = load_tools_into_spin(spin_url, headers, schema_data)
    console.print(f"  Loaded {tools_loaded}/{tools_count} tools")

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
