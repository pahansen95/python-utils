#!/usr/bin/env bash

set -eEuo pipefail

log() { printf -- "%s\n" "$*" >&2; }

: "${CI_PROJECT_DIR:?Missing CI_PROJECT_DIR}"
command -v jq >/dev/null || { log "jq is required"; exit 1; }
(
  log 'Building Package'
  cd "${CI_PROJECT_DIR}" || exit 1
  source "${CI_PROJECT_DIR}/.venv/bin/activate"
  rm -rf "${CI_PROJECT_DIR}/.cache/build" &>/dev/null || true
  install -dm0755 "${CI_PROJECT_DIR}/.cache/build"
  python3 -m build \
    pkg \
      --build="${CI_PROJECT_DIR}/.cache/build" \
      --config="${CI_PROJECT_DIR}/config/stable/all.yaml" \
  | jq
)
log 'fin'
