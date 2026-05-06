#!/usr/bin/env bash
# launchd entry point. Resolves a usable Python at runtime so a renamed or
# pruned virtualenv can no longer silently break the daily crawl.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"

WRAPPER_LOG="${LOG_DIR}/run_remote_crawl.wrapper.log"
exec >>"${WRAPPER_LOG}" 2>&1

ts() { date '+%Y-%m-%d %H:%M:%S%z'; }

echo "[$(ts)] launchd wrapper starting (pid=$$)"

candidates=()
if [[ -n "${CRAWL_PYTHON_BIN:-}" ]]; then
  candidates+=("${CRAWL_PYTHON_BIN}")
fi
candidates+=(
  "${ROOT_DIR}/.venv/bin/python3"
  "${ROOT_DIR}/.venv-migrate/bin/python3"
  "/opt/homebrew/bin/python3"
  "/usr/local/bin/python3"
  "/usr/bin/python3"
)

PYTHON=""
for candidate in "${candidates[@]}"; do
  if [[ -x "${candidate}" ]]; then
    PYTHON="${candidate}"
    break
  fi
done

if [[ -z "${PYTHON}" ]]; then
  echo "[$(ts)] ERROR: no usable Python interpreter found among: ${candidates[*]}"
  exit 127
fi

echo "[$(ts)] using Python: ${PYTHON}"
cd "${ROOT_DIR}"
exec "${PYTHON}" scripts/run_remote_crawl.py "$@"
