#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

MODE=""
MARKET="all"
CONDA_ENV="dev"
CONDA_BIN=""
AS_OF_DATE=""
BATCH_SIZE=100
CONTINUE_ON_ERROR=0
PAUSE_SECONDS="0.0"
PAUSE_JITTER_SECONDS="0.0"
RETRIES=3
RETRY_PAUSE_SECONDS="0.5"

usage() {
  cat <<'EOF'
Usage:
  scripts/run_kline_sync.sh --mode latest|history-backfill [options]

  Options:
  --mode latest|history-backfill  Required
  --market a|hk|us|all            Default: all
  --conda-env ENV                 Default: dev
  --conda-bin PATH                Optional absolute path to conda executable
  --as-of-date YYYYMMDD           Override effective trade date
  --batch-size N                  History batch size, default: 100
  --continue-on-error             Continue on per-symbol history failures
  --pause-seconds FLOAT           Per-symbol pause for history
  --pause-jitter-seconds FLOAT    Extra random pause for history
  --retries N                     Retry count for history, default: 3
  --retry-pause-seconds FLOAT     Delay between retries, default: 0.5
EOF
}

fail() {
  printf '%s ERROR run_kline_sync %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2
  exit 1
}

log() {
  printf '%s INFO run_kline_sync %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="${2:-}"; shift 2 ;;
    --market) MARKET="${2:-}"; shift 2 ;;
    --conda-env) CONDA_ENV="${2:-}"; shift 2 ;;
    --conda-bin) CONDA_BIN="${2:-}"; shift 2 ;;
    --as-of-date) AS_OF_DATE="${2:-}"; shift 2 ;;
    --batch-size) BATCH_SIZE="${2:-}"; shift 2 ;;
    --continue-on-error) CONTINUE_ON_ERROR=1; shift 1 ;;
    --pause-seconds) PAUSE_SECONDS="${2:-}"; shift 2 ;;
    --pause-jitter-seconds) PAUSE_JITTER_SECONDS="${2:-}"; shift 2 ;;
    --retries) RETRIES="${2:-}"; shift 2 ;;
    --retry-pause-seconds) RETRY_PAUSE_SECONDS="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[[ -n "${MODE}" ]] || fail "--mode is required"

case "${MODE}" in
  latest|history-backfill) ;;
  *) fail "unsupported mode: ${MODE}" ;;
esac

case "${MARKET}" in
  a|hk|us|all) ;;
  *) fail "unsupported market: ${MARKET}" ;;
esac

resolve_conda_bin() {
  local -a candidates

  if [[ -n "${CONDA_BIN}" ]]; then
    [[ -x "${CONDA_BIN}" ]] || fail "conda executable not found: ${CONDA_BIN}"
    printf '%s\n' "${CONDA_BIN}"
    return 0
  fi

  if [[ -n "${CONDA_EXE:-}" && -x "${CONDA_EXE}" ]]; then
    printf '%s\n' "${CONDA_EXE}"
    return 0
  fi

  if command -v conda >/dev/null 2>&1; then
    command -v conda
    return 0
  fi

  candidates=(
    "/home/stock/miniconda3/bin/conda"
    "/home/stock/anaconda3/bin/conda"
    "${HOME}/miniconda3/bin/conda"
    "${HOME}/anaconda3/bin/conda"
    "/root/miniconda3/bin/conda"
    "/root/anaconda3/bin/conda"
    "/opt/miniconda3/bin/conda"
    "/opt/anaconda3/bin/conda"
  )
  for candidate in "${candidates[@]}"; do
    if [[ -x "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  fail "conda executable not found; use --conda-bin PATH or set CONDA_EXE"
}

market_timezone() {
  case "$1" in
    a|hk) printf '%s\n' "Asia/Shanghai" ;;
    us) printf '%s\n' "America/New_York" ;;
    *) fail "unsupported market timezone: $1" ;;
  esac
}

market_ready_cutoff_hhmm() {
  case "$1" in
    a) printf '%s\n' "1530" ;;
    hk) printf '%s\n' "1630" ;;
    us) printf '%s\n' "1700" ;;
    *) fail "unsupported market cutoff: $1" ;;
  esac
}

market_effective_trade_date() {
  local market="$1"
  local tz cutoff now_hhmm

  if [[ -n "${AS_OF_DATE}" ]]; then
    if [[ "${AS_OF_DATE}" =~ ^[0-9]{8}$ ]]; then
      printf '%s-%s-%s\n' "${AS_OF_DATE:0:4}" "${AS_OF_DATE:4:2}" "${AS_OF_DATE:6:2}"
    else
      printf '%s\n' "${AS_OF_DATE}"
    fi
    return 0
  fi

  tz="$(market_timezone "${market}")"
  cutoff="$(market_ready_cutoff_hhmm "${market}")"
  now_hhmm="$(TZ="${tz}" date '+%H%M')"
  if (( 10#${now_hhmm} < 10#${cutoff} )); then
    TZ="${tz}" date -v-1d '+%Y-%m-%d'
  else
    TZ="${tz}" date '+%Y-%m-%d'
  fi
}

run_market() {
  local market="$1"
  local trade_date
  local -a cmd
  local conda_exec

  trade_date="$(market_effective_trade_date "${market}")"
  conda_exec="$(resolve_conda_bin)"
  log "market=${market} timezone=$(market_timezone "${market}") cutoff=$(market_ready_cutoff_hhmm "${market}") mode=${MODE} trade_date=${trade_date}"

  if [[ "${MODE}" == "latest" ]]; then
    cmd=(
      "${conda_exec}" run --no-capture-output -n "${CONDA_ENV}" python "${PROJECT_ROOT}/scripts/sync_kline_latest.py"
      --market "${market}"
      --trade-date "${trade_date}"
    )
  else
    cmd=(
      "${conda_exec}" run --no-capture-output -n "${CONDA_ENV}" python "${PROJECT_ROOT}/scripts/backfill_history_2y.py"
      --market "${market}"
      --trade-date "${trade_date}"
      --batch-size "${BATCH_SIZE}"
      --pause-seconds "${PAUSE_SECONDS}"
      --pause-jitter-seconds "${PAUSE_JITTER_SECONDS}"
      --retries "${RETRIES}"
      --retry-pause-seconds "${RETRY_PAUSE_SECONDS}"
    )
    if (( CONTINUE_ON_ERROR == 1 )); then
      cmd+=(--continue-on-error)
    fi
  fi
  "${cmd[@]}"
}

main() {
  local -a markets
  local -a failed_markets
  cd "${PROJECT_ROOT}"
  if [[ "${MARKET}" == "all" ]]; then
    markets=(a hk us)
  else
    markets=("${MARKET}")
  fi
  log "job started mode=${MODE} markets=${markets[*]} conda_env=${CONDA_ENV} conda_bin=$(resolve_conda_bin)"
  for market in "${markets[@]}"; do
    if ! run_market "${market}"; then
      printf '%s ERROR run_kline_sync market failed: %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "${market}" >&2
      failed_markets+=("${market}")
    fi
  done
  if (( ${#failed_markets[@]} > 0 )); then
    fail "job finished with failures: ${failed_markets[*]}"
  fi
  log "job finished mode=${MODE} markets=${markets[*]}"
}

main "$@"
