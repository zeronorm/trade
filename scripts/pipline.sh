#!/usr/bin/env bash

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
ROLE="${1:-${PIPELINE_ROLE:-}}"
MARKET="${2:-${MARKET:-}}"

CONDA_BIN="${CONDA_BIN:-${HOME}/miniconda3/bin}"
CONDA_ENV="${CONDA_ENV:-dev}"
LOG_DIR="${LOG_DIR:-${PROJECT_DIR}/logs/cron}"
LOCK_DIR="${LOCK_DIR:-${LOG_DIR}/locks}"
LOCK_SCOPE="${LOCK_SCOPE:-role}"
COMPASS_LOG="${COMPASS_LOG:-${LOG_DIR}/pipline.log}"
REMOTE_HOST="${REMOTE_HOST:-ecs}"
REMOTE_PROJECT_DIR="${REMOTE_PROJECT_DIR:-/root/dev/trade}"
HIST_RETRIES="${HIST_RETRIES:-2}"
HIST_RETRY_DELAY="${HIST_RETRY_DELAY:-1}"
TRADE_DATE="${TRADE_DATE:-}"
PYTHON_BIN="${PYTHON_BIN:-python}"

mkdir -p "${LOG_DIR}" "${LOCK_DIR}"

if [ -z "${ROLE}" ] || [ -z "${MARKET}" ]; then
  echo "usage: $0 <ecs|mac> <cn|hk|us|all>" >&2
  exit 2
fi

JOB_LOG="${LOG_DIR}/${ROLE}.${MARKET}.log"
exec >>"${JOB_LOG}" 2>&1

timestamp() {
  date "+%Y-%m-%dT%H:%M:%S%z"
}

log_event() {
  local status="$1"
  local step="$2"
  local reason="${3:-}"
  local line
  reason="$(printf "%s" "${reason}" | tr '\n' ' ' | tr -s ' ')"
  line="ts=$(timestamp) host=$(hostname -s 2>/dev/null || hostname) pid=$$ role=${ROLE} market=${MARKET} step=${step} status=${status} reason=${reason} log=${JOB_LOG}"
  echo "${line}"
  echo "${line}" >>"${COMPASS_LOG}"
}

fail() {
  log_event "fail" "${1:-main}" "${2:-error}"
  exit "${3:-1}"
}

validate_market() {
  case "$1" in
    cn|hk|us|all) return 0 ;;
    *) return 1 ;;
  esac
}

validate_role() {
  case "$1" in
    ecs|mac) return 0 ;;
    *) return 1 ;;
  esac
}

activate_python() {
  if [ -f "${CONDA_BIN}/activate" ]; then
    # shellcheck disable=SC1090
    set +u
    source "${CONDA_BIN}/activate" "${CONDA_ENV}"
    set -u
  fi
}

market_list() {
  if [ "$1" = "all" ]; then
    echo "cn hk us"
  else
    echo "$1"
  fi
}

latest_hist_date() {
  local market="$1"
  local hist_dir="${PROJECT_DIR}/data_store/hist"
  [ -d "${hist_dir}" ] || return 0
  find "${hist_dir}" -type f \
    -name "${market}.*.csv" \
    ! -name "${market}.merge.*.csv" \
    ! -name "${market}.*.progress.csv" \
    -print \
    | sed -n "s#^.*/${market}\.\([0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]\)\.csv#\1#p" \
    | sort \
    | tail -n 1
}

run_ecs_market() {
  local market="$1"
  local args
  args=(--market "${market}" --hist-retries "${HIST_RETRIES}" --hist-retry-delay "${HIST_RETRY_DELAY}")
  if [ -n "${TRADE_DATE}" ]; then
    args+=(--trade-date "${TRADE_DATE}")
  fi

  log_event "run" "ecs.day_hist" "start"
  "${PYTHON_BIN}" "${PROJECT_DIR}/scripts/sync_market_data.py" "${args[@]}"
  local status=$?
  if [ "${status}" -eq 0 ]; then
    log_event "ok" "ecs.day_hist" "done"
  else
    log_event "fail" "ecs.day_hist" "exit_${status}"
  fi
  return "${status}"
}

sync_from_ecs() {
  local market="$1"
  mkdir -p "${PROJECT_DIR}/data_store/day" "${PROJECT_DIR}/data_store/hist"

  log_event "run" "mac.sync.day" "remote=${REMOTE_HOST}:${REMOTE_PROJECT_DIR}"
  rsync -az \
    --include="${market}.*.csv" \
    --exclude="*" \
    "${REMOTE_HOST}:${REMOTE_PROJECT_DIR}/data_store/day/" \
    "${PROJECT_DIR}/data_store/day/"
  local day_status=$?
  if [ "${day_status}" -ne 0 ]; then
    log_event "fail" "mac.sync.day" "exit_${day_status}"
    return "${day_status}"
  fi
  log_event "ok" "mac.sync.day" "done"

  log_event "run" "mac.sync.hist" "remote=${REMOTE_HOST}:${REMOTE_PROJECT_DIR}"
  rsync -az \
    --include="${market}.*.csv" \
    --exclude="*" \
    "${REMOTE_HOST}:${REMOTE_PROJECT_DIR}/data_store/hist/" \
    "${PROJECT_DIR}/data_store/hist/"
  local hist_status=$?
  if [ "${hist_status}" -ne 0 ]; then
    log_event "fail" "mac.sync.hist" "exit_${hist_status}"
    return "${hist_status}"
  fi
  log_event "ok" "mac.sync.hist" "done"
}

merge_market() {
  local market="$1"
  local trade_date="${TRADE_DATE}"

  if [ -z "${trade_date}" ]; then
    trade_date="$(latest_hist_date "${market}")"
  fi

  if [ -z "${trade_date}" ]; then
    log_event "skip" "mac.merge" "no_local_hist"
    return 0
  fi

  log_event "run" "mac.merge" "trade_date=${trade_date}"
  "${PYTHON_BIN}" - "${market}" "${trade_date}" <<'PY'
import sys

from src.data import store

market = sys.argv[1]
trade_date = sys.argv[2]
out = store.merge_hist(market, trade_date)
print(f"merge: market={market} trade_date={trade_date} path={out}")
PY
  local status=$?
  if [ "${status}" -eq 0 ]; then
    log_event "ok" "mac.merge" "trade_date=${trade_date}"
  else
    log_event "fail" "mac.merge" "exit_${status}"
  fi
  return "${status}"
}

run_mac_market() {
  local market="$1"
  sync_from_ecs "${market}" || return $?
  merge_market "${market}"
}

validate_role "${ROLE}" || fail "validate" "bad_role_${ROLE}" 2
validate_market "${MARKET}" || fail "validate" "bad_market_${MARKET}" 2

case "${LOCK_SCOPE}" in
  role)
    LOCK_PATH="${LOCK_DIR}/${ROLE}.lock"
    ;;
  market)
    LOCK_PATH="${LOCK_DIR}/${ROLE}.${MARKET}.lock"
    ;;
  *)
    fail "validate" "bad_lock_scope_${LOCK_SCOPE}" 2
    ;;
esac

if ! mkdir "${LOCK_PATH}" 2>/dev/null; then
  lock_info="$(cat "${LOCK_PATH}/info" 2>/dev/null || true)"
  log_event "skip" "lock" "already_running ${lock_info}"
  exit 0
fi

cleanup() {
  rm -rf "${LOCK_PATH}"
}
trap cleanup EXIT INT TERM

{
  echo "pid=$$"
  echo "host=$(hostname -s 2>/dev/null || hostname)"
  echo "started_at=$(timestamp)"
  echo "role=${ROLE}"
  echo "market=${MARKET}"
} >"${LOCK_PATH}/info"

cd "${PROJECT_DIR}" || fail "cd" "project_dir_missing" 1
activate_python

log_event "run" "main" "start"

status=0
for market in $(market_list "${MARKET}"); do
  case "${ROLE}" in
    ecs)
      run_ecs_market "${market}" || status=$?
      ;;
    mac)
      run_mac_market "${market}" || status=$?
      ;;
  esac
done

if [ "${status}" -eq 0 ]; then
  log_event "ok" "main" "done"
else
  log_event "fail" "main" "exit_${status}"
fi

exit "${status}"
