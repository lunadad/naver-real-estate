#!/usr/bin/env bash
# launchd entry point. Resolves a usable Python at runtime so a renamed or
# pruned virtualenv can no longer silently break the daily crawl.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"

# Use /tmp as a fallback so startup errors are always visible even when logs/
# does not yet exist (launchd cannot write StandardOutPath before mkdir runs).
BOOTSTRAP_ERR="/tmp/naver-real-estate-bootstrap.err"
mkdir -p "${LOG_DIR}" 2>>"${BOOTSTRAP_ERR}" || {
  echo "$(date '+%Y-%m-%d %H:%M:%S%z') ERROR: mkdir -p ${LOG_DIR} failed" >>"${BOOTSTRAP_ERR}"
  exit 1
}

WRAPPER_LOG="${LOG_DIR}/run_remote_crawl.wrapper.log"
exec >>"${WRAPPER_LOG}" 2>&1

ts() { date '+%Y-%m-%d %H:%M:%S%z'; }

echo "[$(ts)] launchd wrapper starting (pid=$$)"

# Wait for network connectivity after wake-from-sleep.
# launchd may fire at exactly 09:00 while the NIC is still initialising.
_wait_for_network() {
  local max=12 i=1
  while [[ $i -le $max ]]; do
    if ping -c 1 -W 2 1.1.1.1 >/dev/null 2>&1; then
      echo "[$(ts)] network ready (attempt ${i}/${max})"
      return 0
    fi
    echo "[$(ts)] waiting for network (attempt ${i}/${max})..."
    sleep 5
    (( i++ ))
  done
  echo "[$(ts)] WARNING: network not confirmed after ${max} attempts, proceeding anyway"
}
_wait_for_network

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
