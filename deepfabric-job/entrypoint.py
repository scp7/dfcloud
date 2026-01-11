#!/usr/bin/env python3
"""
DeepFabric Cloud Run Job Entrypoint

Supports two modes:
- import-tools: Import tool schemas from MCP server into Spin service
- generate: Download config from GCS, run deepfabric generate, upload outputs

Set JOB_MODE environment variable to control which mode to run.
"""

import http.server
import json
import os
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path

import requests
import yaml
from google.cloud import storage


# Global for auth proxy
_spin_endpoint = None
_auth_token = None
_token_obtained_at = 0
_token_lock = threading.Lock()

# Refresh token every 45 minutes (tokens expire after ~1 hour)
TOKEN_REFRESH_INTERVAL = 45 * 60


def transform_tools_response(data: dict) -> dict:
    """Transform Spin's tool format to MCP-compatible format."""
    if "tools" not in data:
        return data

    transformed_tools = []
    for tool in data["tools"]:
        # If inputSchema is a list, convert to JSON Schema object
        input_schema = tool.get("inputSchema", {})
        if isinstance(input_schema, list):
            # Convert list of params to JSON Schema
            properties = {}
            required = []
            for param in input_schema:
                param_name = param.get("name", "")
                if not param_name:
                    continue
                prop = {"type": param.get("type", "string")}
                if param.get("description"):
                    prop["description"] = param["description"]
                if param.get("default") is not None and param.get("default") != "":
                    prop["default"] = param["default"]
                properties[param_name] = prop
                if param.get("required", False):
                    required.append(param_name)

            input_schema = {
                "type": "object",
                "properties": properties,
            }
            if required:
                input_schema["required"] = required

        transformed_tools.append({
            "name": tool.get("name", ""),
            "description": tool.get("description", ""),
            "inputSchema": input_schema,
        })

    return {"tools": transformed_tools}


class AuthProxyHandler(http.server.BaseHTTPRequestHandler):
    """HTTP proxy that adds auth headers to requests to Spin service."""

    def do_GET(self):
        self._proxy_request("GET")

    def do_POST(self):
        self._proxy_request("POST")

    def _proxy_request(self, method):
        global _spin_endpoint, _auth_token, _token_obtained_at, _token_lock

        # Refresh token if needed
        with _token_lock:
            if time.time() - _token_obtained_at > TOKEN_REFRESH_INTERVAL:
                print("Refreshing auth token...")
                new_token = get_identity_token(_spin_endpoint)
                if new_token:
                    _auth_token = new_token
                    _token_obtained_at = time.time()
                    print("Auth token refreshed successfully")
                else:
                    print("Warning: Failed to refresh auth token")

        # Build target URL
        target_url = f"{_spin_endpoint.rstrip('/')}{self.path}"

        # Read request body if present
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else None

        # Build headers with auth
        headers = {"Authorization": f"Bearer {_auth_token}"}
        if self.headers.get("Content-Type"):
            headers["Content-Type"] = self.headers["Content-Type"]

        try:
            req = urllib.request.Request(target_url, data=body, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=60) as response:
                response_body = response.read()

                # Transform tools list response to MCP format
                if "/list-tools" in self.path:
                    try:
                        data = json.loads(response_body)
                        data = transform_tools_response(data)
                        response_body = json.dumps(data).encode()
                    except Exception:
                        pass  # Keep original if transform fails

                self.send_response(response.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", len(response_body))
                self.end_headers()
                self.wfile.write(response_body)
        except urllib.error.HTTPError as e:
            self.send_response(e.code)
            self.end_headers()
            self.wfile.write(e.read())
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(e).encode())

    def log_message(self, format, *args):
        # Suppress default logging
        pass


def start_auth_proxy(spin_endpoint: str, port: int = 3000) -> threading.Thread:
    """Start a local proxy that adds auth to requests to Spin service."""
    global _spin_endpoint, _auth_token, _token_obtained_at

    _spin_endpoint = spin_endpoint
    _auth_token = get_identity_token(spin_endpoint)
    _token_obtained_at = time.time()

    if not _auth_token:
        print("Warning: Could not get identity token for auth proxy")
        return None

    server = socketserver.TCPServer(("", port), AuthProxyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"Auth proxy started on localhost:{port} -> {spin_endpoint}")
    print(f"Token will refresh every {TOKEN_REFRESH_INTERVAL // 60} minutes")
    return thread


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


def update_config_for_proxy(config_path: Path) -> None:
    """Ensure config uses localhost:3000 which will be proxied to Spin service."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Ensure tools config points to localhost (proxy)
    if "generation" in config and "tools" in config["generation"]:
        tools_config = config["generation"]["tools"]

        # Ensure spin_endpoint points to local proxy
        if "spin_endpoint" in tools_config:
            original = tools_config["spin_endpoint"]
            if "localhost:3000" not in original:
                tools_config["spin_endpoint"] = "http://localhost:3000"
                print(f"Updated spin_endpoint: {original} -> http://localhost:3000 (proxy)")

        # Ensure tools_endpoint points to local proxy
        if "tools_endpoint" in tools_config:
            original = tools_config["tools_endpoint"]
            if "localhost:3000" not in original:
                tools_config["tools_endpoint"] = "http://localhost:3000/mock/list-tools"
                print(f"Updated tools_endpoint: {original} -> http://localhost:3000/mock/list-tools (proxy)")

    with open(config_path, "w") as f:
        yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)


def get_progress_interval() -> int:
    """Get Slack progress update interval from environment. Default 900 seconds (15 min)."""
    return int(os.environ.get("PROGRESS_INTERVAL", "900"))


def get_output_files_from_config(
    config_path: Path,
    topic_only: bool = False,
    topics_load: str | None = None,
) -> list[str]:
    """Extract output file paths from config based on run mode."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    outputs = []

    # Topic graph output - only when generating topics (not loading)
    if topic_only or not topics_load:
        if "topics" in config and "save_as" in config["topics"]:
            outputs.append(config["topics"]["save_as"])

    # Dataset output - only when generating dataset (not topic-only)
    if not topic_only:
        if "output" in config and "save_as" in config["output"]:
            outputs.append(config["output"]["save_as"])

    return outputs


def send_slack_notification(
    webhook_url: str,
    job_name: str,
    status: str,
    duration_seconds: float,
    output_files: list[dict],
    error_message: str | None = None,
) -> None:
    """Send a notification to Slack.

    output_files is a list of dicts with keys: url, filename, size_bytes
    """
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

    if output_files:
        outputs_lines = []
        for f in output_files:
            size_kb = f.get("size_bytes", 0) / 1024
            if size_kb < 1024:
                size_str = f"{size_kb:.1f} KB"
            else:
                size_str = f"{size_kb/1024:.1f} MB"
            outputs_lines.append(f"• `{f['filename']}` ({size_str})\n  `{f['url']}`")
        outputs_text = "\n".join(outputs_lines)
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


def get_identity_token(audience: str) -> str | None:
    """Get identity token from GCP metadata server."""
    metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity"
    try:
        response = requests.get(
            f"{metadata_url}?audience={audience}",
            headers={"Metadata-Flavor": "Google"},
            timeout=5,
        )
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Warning: Could not get identity token: {e}")
        return None


def run_import_tools(spin_endpoint: str, mcp_command: str = "npx dataforseo-mcp-server@latest") -> tuple[bool, str]:
    """Run deepfabric import-tools to fetch tools, then push to Spin with auth."""
    import tempfile

    # Step 1: Fetch tools from MCP server to a temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        tools_file = f.name

    cmd = [
        "deepfabric", "import-tools",
        "--transport", "stdio",
        "--command", mcp_command,
        "--output", tools_file,
    ]

    print(f"Step 1: Fetching tools from MCP server...")
    print(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

        if result.stdout:
            print("import-tools STDOUT:", result.stdout)
        if result.stderr:
            print("import-tools STDERR:", result.stderr)

        if result.returncode != 0:
            return False, result.stderr or result.stdout or f"Exit code: {result.returncode}"

        # Step 2: Load tools from file
        print(f"\nStep 2: Loading tools from {tools_file}...")
        with open(tools_file, 'r') as f:
            tools_data = json.load(f)

        # Convert to MCP format for Spin
        if isinstance(tools_data, dict) and 'tools' in tools_data:
            mcp_tools = tools_data['tools']
        elif isinstance(tools_data, list):
            mcp_tools = tools_data
        else:
            return False, f"Unexpected tools format: {type(tools_data)}"

        print(f"Loaded {len(mcp_tools)} tools")

        # Step 3: Push to Spin with auth
        print(f"\nStep 3: Pushing tools to Spin service...")
        token = get_identity_token(spin_endpoint)
        if not token:
            return False, "Could not get identity token for Spin service"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        # Convert tools to MCP format with inputSchema
        mcp_payload = {"tools": []}
        for tool in mcp_tools:
            mcp_tool = {
                "name": tool.get("name"),
                "description": tool.get("description", ""),
                "inputSchema": tool.get("inputSchema") or tool.get("parameters", {}),
            }
            mcp_payload["tools"].append(mcp_tool)

        response = requests.post(
            f"{spin_endpoint.rstrip('/')}/mock/load-schema",
            json=mcp_payload,
            headers=headers,
            timeout=30,
        )

        if response.status_code == 200:
            result_data = response.json()
            loaded = result_data.get("loaded", len(mcp_tools))
            return True, f"Successfully loaded {loaded} tools into Spin"
        else:
            return False, f"Spin returned {response.status_code}: {response.text}"

    except subprocess.TimeoutExpired:
        return False, "import-tools timed out after 120 seconds"
    except Exception as e:
        return False, str(e)
    finally:
        # Cleanup temp file
        try:
            os.remove(tools_file)
        except Exception:
            pass


def run_deepfabric(
    config_path: Path,
    work_dir: Path,
    topic_only: bool = False,
    topics_load: str | None = None,
    slack_webhook_url: str | None = None,
    job_name: str | None = None,
    progress_interval: int = 900,
) -> tuple[bool, str]:
    """Run deepfabric generate command with streaming output. Returns (success, output/error)."""
    cmd = ["deepfabric", "generate", str(config_path), "--tui", "simple"]

    if topic_only:
        cmd.append("--topic-only")
    elif topics_load:
        cmd.extend(["--topics-load", topics_load])

    print(f"Running: {' '.join(cmd)}")
    print(f"Working directory: {work_dir}")
    sys.stdout.flush()

    timeout_seconds = int(get_env("DEEPFABRIC_TIMEOUT", required=False) or 86400)
    output_lines = []
    last_progress_update = time.time()

    try:
        process = subprocess.Popen(
            cmd,
            cwd=str(work_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # Line buffered
        )

        start_time = time.time()

        # Stream output in real-time
        for line in process.stdout:
            line = line.rstrip()
            print(line)
            sys.stdout.flush()
            output_lines.append(line)

            # Check for timeout
            if time.time() - start_time > timeout_seconds:
                process.kill()
                return False, f"Job timed out after {timeout_seconds} seconds"

            # Send periodic Slack progress updates
            if slack_webhook_url and job_name:
                if time.time() - last_progress_update > progress_interval:
                    # Look for progress info in recent lines
                    progress_info = _extract_progress(output_lines[-20:])
                    if progress_info:
                        _send_progress_update(slack_webhook_url, job_name, progress_info)
                    last_progress_update = time.time()

        process.wait()

        if process.returncode == 0:
            return True, "\n".join(output_lines)
        else:
            return False, "\n".join(output_lines) or f"Exit code: {process.returncode}"

    except Exception as e:
        return False, str(e)


def _extract_progress(lines: list[str]) -> str | None:
    """Extract progress information from recent log lines."""
    import re

    # Look for deepfabric --tui simple output patterns
    for line in reversed(lines):
        # Dataset generation: "Step 244: +4 (total 976/10000)"
        if "total" in line and "/" in line:
            match = re.search(r"total\s+(\d+)/(\d+)", line)
            if match:
                current, total = match.groups()
                pct = int(current) / int(total) * 100
                return f"Samples: {current}/{total} ({pct:.1f}%)"

        # Topic generation patterns
        if "topic" in line.lower() and "/" in line:
            return line

    return None


def _send_progress_update(webhook_url: str, job_name: str, progress: str) -> None:
    """Send a progress update to Slack."""
    payload = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":hourglass_flowing_sand: *{job_name}* in progress\n`{progress}`",
                },
            }
        ]
    }
    try:
        requests.post(webhook_url, json=payload, timeout=10)
    except Exception:
        pass  # Don't fail the job for notification errors


def run_import_tools_mode():
    """Run import-tools mode: import tool schemas into Spin service."""
    start_time = time.time()

    spin_endpoint = get_env("SPIN_ENDPOINT")
    mcp_command = get_env("MCP_COMMAND", required=False) or "npx dataforseo-mcp-server@latest"
    slack_webhook_url = get_env("SLACK_WEBHOOK_URL", required=False)

    print("=== Import Tools Mode ===")
    print(f"Spin endpoint: {spin_endpoint}")
    print(f"MCP command: {mcp_command}")

    success, output = run_import_tools(spin_endpoint, mcp_command)
    duration = time.time() - start_time

    if success:
        print(f"\nTools imported successfully in {duration:.1f} seconds")
        if slack_webhook_url:
            send_slack_notification(
                webhook_url=slack_webhook_url,
                job_name="import-tools",
                status="success",
                duration_seconds=duration,
                output_files=[],
                error_message=None,
            )
    else:
        print(f"\nFailed to import tools: {output}")
        if slack_webhook_url:
            send_slack_notification(
                webhook_url=slack_webhook_url,
                job_name="import-tools",
                status="failed",
                duration_seconds=duration,
                output_files=[],
                error_message=output,
            )
        sys.exit(1)


def run_generate_mode():
    """Run generate mode: download config, run deepfabric, upload outputs."""
    start_time = time.time()

    # Get configuration from environment
    gcs_bucket = get_env("GCS_BUCKET")
    config_path = get_env("CONFIG_PATH")  # e.g., "configs/my-job.yaml"
    job_name = get_env("JOB_NAME")
    spin_endpoint = get_env("SPIN_ENDPOINT")
    slack_webhook_url = get_env("SLACK_WEBHOOK_URL")

    # Topic generation options
    topic_only = get_env("TOPIC_ONLY", required=False) == "true"
    topics_load_gcs = get_env("TOPICS_LOAD", required=False)  # GCS path to existing graph

    print("=== Generate Mode ===")
    print(f"Job: {job_name}")
    print(f"Config: gs://{gcs_bucket}/{config_path}")
    print(f"Spin endpoint: {spin_endpoint}")
    if topic_only:
        print("Mode: Topic graph generation only (--topic-only)")
    elif topics_load_gcs:
        print(f"Mode: Dataset generation with existing graph (--topics-load {topics_load_gcs})")

    # Create working directory
    with tempfile.TemporaryDirectory() as work_dir:
        work_path = Path(work_dir)
        local_config = work_path / "config.yaml"
        local_topics_path = None

        try:
            # Start auth proxy to forward localhost:3000 -> Spin service with auth
            proxy_thread = start_auth_proxy(spin_endpoint, port=3000)
            if not proxy_thread:
                raise RuntimeError("Failed to start auth proxy")

            # Download config from GCS
            download_from_gcs(gcs_bucket, config_path, local_config)

            # Ensure config uses localhost:3000 (proxied to Spin)
            update_config_for_proxy(local_config)

            # Download existing topic graph if specified
            if topics_load_gcs:
                local_topics_path = str(work_path / "topics.jsonl")
                download_from_gcs(gcs_bucket, topics_load_gcs, Path(local_topics_path))

            # Get expected output files
            expected_outputs = get_output_files_from_config(
                local_config,
                topic_only=topic_only,
                topics_load=local_topics_path,
            )
            print(f"Expected outputs: {expected_outputs}")

            # Get progress interval from environment (default 900 seconds / 15 min)
            progress_interval = get_progress_interval()
            print(f"Slack progress interval: {progress_interval} seconds")

            # Run deepfabric
            success, output = run_deepfabric(
                local_config,
                work_path,
                topic_only=topic_only,
                topics_load=local_topics_path,
                slack_webhook_url=slack_webhook_url,
                job_name=job_name,
                progress_interval=progress_interval,
            )

            # Upload outputs to GCS
            output_files = []
            timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            output_prefix = f"outputs/{job_name}/{timestamp}"

            for output_file in expected_outputs:
                local_output = work_path / output_file
                if local_output.exists():
                    gcs_path = f"{output_prefix}/{output_file}"
                    file_size = local_output.stat().st_size
                    url = upload_to_gcs(local_output, gcs_bucket, gcs_path)
                    output_files.append({
                        "url": url,
                        "filename": output_file,
                        "size_bytes": file_size,
                    })
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
                output_files=output_files,
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
                output_files=[],
                error_message=error_msg,
            )

            sys.exit(1)


def main():
    """Main entrypoint - dispatch based on JOB_MODE."""
    job_mode = get_env("JOB_MODE", required=False) or "generate"

    if job_mode == "import-tools":
        run_import_tools_mode()
    elif job_mode == "generate":
        run_generate_mode()
    else:
        print(f"Unknown JOB_MODE: {job_mode}")
        print("Valid modes: import-tools, generate")
        sys.exit(1)


if __name__ == "__main__":
    main()
