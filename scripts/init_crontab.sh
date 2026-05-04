#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
ROLE="auto"
PRINT_ONLY=0
DRY_RUN=0

usage() {
  cat <<EOF
usage: $0 [ecs|mac|auto] [--print] [--dry-run]

Installs a thin crontab block that delegates all run/skip logic to scripts/pipline.sh.

Environment:
  PROJECT_DIR          default: ${PROJECT_DIR}
  CONDA_BIN            default: \$HOME/miniconda3/bin
  CONDA_ENV            default: dev
  LOG_DIR              default: \$PROJECT_DIR/logs/cron
  REMOTE_HOST          mac only, default: ecs
  REMOTE_PROJECT_DIR   mac only, default: /root/dev/trade
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    ecs|mac|auto)
      ROLE="$1"
      ;;
    --print)
      PRINT_ONLY=1
      ;;
    --dry-run)
      DRY_RUN=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

detect_role() {
  case "$(uname -s)" in
    Darwin) echo "mac" ;;
    Linux) echo "ecs" ;;
    *)
      echo "cannot auto-detect role on $(uname -s); pass ecs or mac" >&2
      exit 2
      ;;
  esac
}

if [ "${ROLE}" = "auto" ]; then
  ROLE="$(detect_role)"
fi

CONDA_BIN="${CONDA_BIN:-${HOME}/miniconda3/bin}"
CONDA_ENV="${CONDA_ENV:-dev}"
LOG_DIR="${LOG_DIR:-${PROJECT_DIR}/logs/cron}"
REMOTE_HOST="${REMOTE_HOST:-ecs}"
REMOTE_PROJECT_DIR="${REMOTE_PROJECT_DIR:-/root/dev/trade}"
PIPELINE="${PROJECT_DIR}/scripts/pipline.sh"

BEGIN_MARK="# BEGIN trade pipline cron"
END_MARK="# END trade pipline cron"

cron_header() {
  cat <<EOF
${BEGIN_MARK}
SHELL=/bin/bash
PROJECT_DIR=${PROJECT_DIR}
CONDA_BIN=${CONDA_BIN}
CONDA_ENV=${CONDA_ENV}
LOG_DIR=${LOG_DIR}
REMOTE_HOST=${REMOTE_HOST}
REMOTE_PROJECT_DIR=${REMOTE_PROJECT_DIR}

# Thin cron entries only. scripts/pipline.sh owns locks, run/skip decisions, sync, merge, and logs.
EOF
}

cron_footer() {
  cat <<EOF
${END_MARK}
EOF
}

ecs_schedule() {
  cat <<EOF
# A shares: close 15:00 Asia/Shanghai, retry window 15:30-23:30.
30 15 * * 1-5 ${PIPELINE} ecs cn
*/30 16-23 * * 1-5 ${PIPELINE} ecs cn

# Hong Kong: close 16:00 Asia/Shanghai, retry window 16:30-23:30.
30 16 * * 1-5 ${PIPELINE} ecs hk
*/30 17-23 * * 1-5 ${PIPELINE} ecs hk

# US: close 16:00 ET, retry window 04:30-11:30 Asia/Shanghai on Tue-Sat.
30 4 * * 2-6 ${PIPELINE} ecs us
*/30 5-11 * * 2-6 ${PIPELINE} ecs us
EOF
}

mac_schedule() {
  cat <<EOF
# A shares: pull from ecs and rebuild local merge after ecs has had time to write data.
0 16 * * 1-5 ${PIPELINE} mac cn
*/30 17-23 * * 1-5 ${PIPELINE} mac cn

# Hong Kong: pull from ecs and rebuild local merge.
0 17 * * 1-5 ${PIPELINE} mac hk
*/30 18-23 * * 1-5 ${PIPELINE} mac hk

# US: pull from ecs and rebuild local merge after US close.
0 6 * * 2-6 ${PIPELINE} mac us
*/30 7-12 * * 2-6 ${PIPELINE} mac us
EOF
}

build_block() {
  cron_header
  case "${ROLE}" in
    ecs) ecs_schedule ;;
    mac) mac_schedule ;;
    *)
      echo "bad role: ${ROLE}" >&2
      exit 2
      ;;
  esac
  cron_footer
}

BLOCK="$(build_block)"

if [ "${PRINT_ONLY}" -eq 1 ] || [ "${DRY_RUN}" -eq 1 ]; then
  printf "%s\n" "${BLOCK}"
fi

if [ "${PRINT_ONLY}" -eq 1 ]; then
  exit 0
fi

TMP="$(mktemp)"
trap 'rm -f "${TMP}"' EXIT
EXISTING_CRONTAB="$(crontab -l 2>/dev/null || true)"

{
  printf "%s\n" "${EXISTING_CRONTAB}" | awk -v begin="${BEGIN_MARK}" -v end="${END_MARK}" '
    $0 == begin { skip = 1; next }
    $0 == end { skip = 0; next }
    skip != 1 { print }
  '
  printf "\n%s\n" "${BLOCK}"
} >"${TMP}"

if [ "${DRY_RUN}" -eq 1 ]; then
  echo
  echo "dry-run: crontab was not changed"
  exit 0
fi

crontab "${TMP}"
echo "installed ${ROLE} crontab block"
echo "logs: ${LOG_DIR}/pipline.log"
