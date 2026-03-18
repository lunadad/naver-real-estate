#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SERVICE_ID="${RENDER_SERVICE_ID:-}"
SERVICE_NAME="${RENDER_SERVICE_NAME:-naver-real-estate}"
COMMIT_ID="${1:-$(git rev-parse HEAD)}"

if ! command -v render >/dev/null 2>&1; then
  echo "render CLI not found. Install it first." >&2
  exit 1
fi

if ! render whoami >/dev/null 2>&1; then
  echo "Render CLI is not authenticated. Run: render login" >&2
  exit 1
fi

if [[ -z "$SERVICE_ID" ]]; then
  SERVICE_ID="$(
    render services list --output json | python3 -c '
import json
import sys

service_name = sys.argv[1]
services = json.load(sys.stdin)
for service in services:
    if service.get("service", {}).get("name") == service_name:
        print(service["service"]["id"])
        raise SystemExit(0)
raise SystemExit(1)
' "$SERVICE_NAME" 2>/dev/null || true
  )"
fi

if [[ -z "$SERVICE_ID" ]]; then
  cat >&2 <<EOF
Could not resolve the Render service ID automatically.

Set one of these and re-run:
  export RENDER_SERVICE_NAME=$SERVICE_NAME
  export RENDER_SERVICE_ID=<your-service-id>

To find the service ID manually:
  render services list --output json
EOF
  exit 1
fi

echo "Deploying commit $COMMIT_ID to Render service $SERVICE_ID"
render deploys create "$SERVICE_ID" --commit "$COMMIT_ID" --wait --confirm
