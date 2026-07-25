#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

REMOTE_NAME="${REMOTE_NAME:-origin}"
LOCAL_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
COMMIT_ID="$(git rev-parse HEAD)"

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Working tree is not clean. Commit or stash changes first." >&2
  exit 1
fi

if [[ "$LOCAL_BRANCH" == "HEAD" ]]; then
  echo "Detached HEAD detected. Check out a branch first." >&2
  exit 1
fi

if ! git remote get-url "$REMOTE_NAME" >/dev/null 2>&1; then
  echo "Git remote '$REMOTE_NAME' does not exist." >&2
  exit 1
fi

# Some worktrees run under a local branch name that differs from the remote
# branch it tracks (e.g. local main-kakao -> origin/main), since git forbids
# checking out the same branch in two worktrees at once. Resolve the actual
# upstream branch name instead of assuming it matches $LOCAL_BRANCH, so this
# doesn't silently create a stray branch on the remote.
if [[ -z "${BRANCH_NAME:-}" ]]; then
  UPSTREAM_REF="$(git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' 2>/dev/null || true)"
  if [[ -n "$UPSTREAM_REF" ]]; then
    BRANCH_NAME="${UPSTREAM_REF#*/}"
  else
    BRANCH_NAME="$LOCAL_BRANCH"
  fi
fi

echo "Pushing $COMMIT_ID ($LOCAL_BRANCH) to $REMOTE_NAME/$BRANCH_NAME"
git push "$REMOTE_NAME" "HEAD:refs/heads/$BRANCH_NAME"
