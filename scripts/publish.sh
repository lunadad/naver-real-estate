#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

REMOTE_NAME="${REMOTE_NAME:-origin}"
BRANCH_NAME="${BRANCH_NAME:-$(git rev-parse --abbrev-ref HEAD)}"
COMMIT_ID="$(git rev-parse HEAD)"

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Working tree is not clean. Commit or stash changes first." >&2
  exit 1
fi

if [[ "$BRANCH_NAME" == "HEAD" ]]; then
  echo "Detached HEAD detected. Check out a branch first." >&2
  exit 1
fi

if ! git remote get-url "$REMOTE_NAME" >/dev/null 2>&1; then
  echo "Git remote '$REMOTE_NAME' does not exist." >&2
  exit 1
fi

echo "Pushing $COMMIT_ID to $REMOTE_NAME/$BRANCH_NAME"
git push "$REMOTE_NAME" "$BRANCH_NAME"

echo "Triggering Render deploy for $COMMIT_ID"
"$ROOT_DIR/scripts/deploy_render.sh" "$COMMIT_ID"
