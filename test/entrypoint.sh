#!/usr/bin/env bash

set -eEuo pipefail

print() { printf -- "%s\n" "$*"; }
log() { printf -- "[$(date -u)] %b\n" "$*"; }

: "${CI_PROJECT_DIR:?Missing CI_PROJECT_DIR}"
declare BUILD_DIR="${CI_PROJECT_DIR}/build"
[[ -d "${BUILD_DIR}" ]] || { log "Missing Build Directory: ${BUILD_DIR}"; exit 1; }
declare SRC_DIR="${CI_PROJECT_DIR}/src"
[[ -d "${SRC_DIR}" ]] || { log "Missing Source Directory: ${SRC_DIR}"; exit 1; }
declare CONFIG_DIR="${CI_PROJECT_DIR}/config"
[[ -d "${CONFIG_DIR}" ]] || { log "Missing Config Directory: ${CONFIG_DIR}"; exit 1; }
declare TEST_DIR="${CI_PROJECT_DIR}/test"
[[ -d "${TEST_DIR}" ]] || { log "Missing Test Directory: ${TEST_DIR}"; exit 1; }
declare PYTHON_VERSION; PYTHON_VERSION="$(< "${CI_PROJECT_DIR}/.python-version")" || {
  log "Missing Python Version File: ${CI_PROJECT_DIR}/.python-version"
  exit 1
}
declare CACHE_DIR="${CI_PROJECT_DIR}/.cache"
[[ -d "${CACHE_DIR}" ]] || {
  log "Missing Project Cache Directory: ${CACHE_DIR}"
  exit 1
}
declare project_tmp="${CACHE_DIR}/.tmp"
[[ -d "${project_tmp}" ]] || install -dm0750 "${project_tmp}"

build_utils() {
  log "Building the Utils library..."
  local -A _fn_kwarg=()
  for argv in "$@"; do
    case "${argv}" in
      workdir=* ) _fn_kwarg[workdir]="${argv#*=}" ;;
      cfg=* ) _fn_kwarg[cfg]="${argv#*=}" ;;
      * ) log "Unknown argument: ${argv}" ;;
    esac
  done
  [[ -n "${_fn_kwarg[workdir]:-}" ]] || { log "Missing workdir function kwarg"; return 1; }
  [[ -n "${_fn_kwarg[cfg]:-}" ]] || { log "Missing cfg function kwarg"; return 1; }
  local src_dir="${_fn_kwarg[workdir]}/src"
  [[ -d "${src_dir}" ]] || { log "Missing Source Directory: ${src_dir}"; return 1; }
  local build_dir="${_fn_kwarg[workdir]}/build"
  [[ -d "${build_dir}" ]] || { log "Missing Build Directory: ${build_dir}"; return 1; }
  local outputs_dir="${_fn_kwarg[workdir]}/build-outputs"
  [[ -d "${outputs_dir}" ]] || { log "Missing Outputs Directory: ${outputs_dir}"; return 1; }

  # Add the Build Files
  rsync -av "${BUILD_DIR}/" "${build_dir}/"

  # Create the Python Virtual Environment
  (
    cd "${build_dir}" || {
      log "Failed to change to the build directory: ${build_dir}"
      exit 1
    }
    pyenv local "${PYTHON_VERSION}"
    pyenv exec python3 -m venv --upgrade-deps --clear --prompt '(Build)' .venv
    [[ -f "${build_dir}/requirements.txt" ]] && {
      source "${build_dir}/.venv/bin/activate"
      pip install -r "${build_dir}/requirements.txt"
    }
  )

  # Write the Config File
  [[ -f "${_fn_kwarg[cfg]}" ]] || {
    log "Missing Config File: ${_fn_kwarg[cfg]}"
    return 1
  }
  install -m0640 -T <(yq -o j -P "${_fn_kwarg[cfg]}") "${outputs_dir}/config.json"

  # Build the Utils Library
  (
    cd "${_fn_kwarg[workdir]}" || {
      log "Failed to change to the work directory: ${_fn_kwarg[workdir]}"
      exit 1
    }
    export CI_PROJECT_DIR="${_fn_kwarg[workdir]}"
    export LOG_LEVEL="${LOG_LEVEL:-INFO}"
    source "${build_dir}/.venv/bin/activate"
    python3 -m build \
      pkg \
        --build="${outputs_dir}" \
        --config="${outputs_dir}/config.json" \
    | jq > "${outputs_dir}/results.json"
  )

}

test_utils() {
  log "Testing the Utils library..."
  local -A _fn_kwarg=()
  for argv in "$@"; do
    case "${argv}" in
      workdir=* ) _fn_kwarg[workdir]="${argv#*=}" ;;
      * ) log "Unknown argument: ${argv}" ;;
    esac
  done
  [[ -n "${_fn_kwarg[workdir]:-}" ]] || { log "Missing workdir function kwarg"; return 1; }
  local src_dir="${_fn_kwarg[workdir]}/src"
  [[ -d "${src_dir}" ]] || { log "Missing Source Directory: ${src_dir}"; return 1; }
  local outputs_dir="${_fn_kwarg[workdir]}/build-outputs"
  [[ -d "${outputs_dir}" ]] || { log "Missing Outputs Directory: ${outputs_dir}"; return 1; }
  local test_dir="${_fn_kwarg[workdir]}/test"
  [[ -d "${test_dir}" ]] || { log "Missing Test Directory: ${test_dir}"; return 1; }

  # Add the Test Files
  rsync -av "${TEST_DIR}/" "${test_dir}/"

  # Setup the Venv
  (
    cd "${test_dir}" || {
      log "Failed to change to the test directory: ${test_dir}"
      exit 1
    }
    pyenv local "${PYTHON_VERSION}"
    pyenv exec python3 -m venv --upgrade-deps --clear --prompt '(Test)' .venv
    source "${test_dir}/.venv/bin/activate"
    [[ -f "${test_dir}/requirements.txt" ]] && pip install -r "${test_dir}/requirements.txt"
    [[ -f "${src_dir}/requirements.txt" ]] && pip install -r "${src_dir}/requirements.txt"
  )

  # Run the Tests
  (
    cd "${_fn_kwarg[workdir]}" || {
      log "Failed to change to the test directory: ${test_dir}"
      exit 1
    }
    export CI_PROJECT_DIR="${_fn_kwarg[workdir]}"
    export LOG_LEVEL="${LOG_LEVEL:-INFO}"
    export PYTHONPATH="${src_dir}"
    source "${test_dir}/.venv/bin/activate"
    python3 -m test \
      run
  )

}

### Main Logic ###

declare -A kwargs=(
  [config]="${CONFIG_DIR}/stable/all.yaml"
)
for argv in "$@"; do
  case "${argv}" in
    config=* ) kwargs[config]="${argv#*=}" ;;
    * ) log "Unknown argument: ${argv}" ;;
  esac
done
[[ -f "${kwargs[config]}" ]] || { log "Missing Config File: ${kwargs[config]}"; exit 1; }

declare CLEANUP; CLEANUP="${CLEANUP:-true}"
_on_err() {
  declare -g CLEANUP=false
}; trap '_on_err' ERR
_on_exit() {
  [[ "${CLEANUP}" == true ]] && {
    [[ -n "${workdir:-}" ]] && { rm -rf "${workdir}" || true ; }
  }
}; trap '_on_exit' EXIT

declare workdir; workdir="$(mktemp -d --tmpdir="${project_tmp}" "${EPOCHSECONDS}.XXX")"
log "Created Work Directory: ${workdir}"

### Setup the Source Directory ###
declare src_dir="${workdir}/src"
ln -s -T "${SRC_DIR}" "${src_dir}"

### Setup the Build Directory ###
declare build_dir="${workdir}/build"
install -dm0755 "${build_dir}"

### Setup the Build Output Directory ###
declare outputs_dir="${workdir}/build-outputs"
install -dm0755 "${outputs_dir}"

### Build the Utils Library ###
build_utils workdir="${workdir}" cfg="${kwargs[config]}"

### Replace the Source Directory ###
unlink "${src_dir}"
install -dm0755 "${src_dir}"
declare artifact_kind; artifact_kind="$(jq -cr '.spec.artifact.kind' "${outputs_dir}/config.json")"
[[ $artifact_kind =~ ^tar ]] || {
  log "Unsupported Artifact Kind: ${artifact_kind}"
  exit 1
}
declare artifact_file; artifact_file="$(jq -cr '.artifact' "${outputs_dir}/results.json")"
tar -xf "${artifact_file}" -C "${src_dir}"

### Setup the Test Directory ###
declare test_dir="${workdir}/test"
install -dm0755 "${test_dir}"

### Test the Utils Library ###
test_utils workdir="${workdir}"