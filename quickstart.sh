#!/usr/bin/env bash

set -eEuo pipefail

log() { printf -- "%s\n" "$*" >&2; }

: "${CI_PROJECT_DIR:?Missing CI_PROJECT_DIR}"
command -v jq >/dev/null || { log "jq is required"; exit 1; }

### Build a Package

(
  log 'Building the Package'
  cd "${CI_PROJECT_DIR}" || exit 1
  source "${CI_PROJECT_DIR}/.venv/bin/activate"
  rm -rf "${CI_PROJECT_DIR}/.cache/build" &>/dev/null || true
  install -dm0755 "${CI_PROJECT_DIR}/.cache/build" "${CI_PROJECT_DIR}/.cache/build/src" "${CI_PROJECT_DIR}/.cache/build/artifacts"
  ln -s -T "${CI_PROJECT_DIR}/src/utils" "${CI_PROJECT_DIR}/.cache/build/src/utils"
  python3 -m build \
    gen \
      --build="${CI_PROJECT_DIR}/.cache/build" \
      --config="${CI_PROJECT_DIR}/config/edge/all.yaml" \
  | python3 -m build \
    build \
      --build="${CI_PROJECT_DIR}/.cache/build" \
      --config="${CI_PROJECT_DIR}/config/edge/all.yaml"
)

### Test a Utils Package
(
  log 'Testing the Package'
  cd "${CI_PROJECT_DIR}" || exit 1
  bash "${CI_PROJECT_DIR}/test/entrypoint.sh" \
    "config=${CI_PROJECT_DIR}/config/edge/all.yaml"
)

log 'fin'
