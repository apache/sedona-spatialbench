#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e
set -o pipefail

if [ ${VERBOSE:-0} -gt 0 ]; then
  set -x
fi

# Check that required dependencies are installed
check_dependencies() {
  local missing_deps=0

  local required_deps=("curl" "git" "gpg" "cargo" "java" "python")
  for dep in "${required_deps[@]}"; do
    if ! command -v $dep &> /dev/null; then
      echo "Error: $dep is not installed or not in PATH"
      missing_deps=1
    fi
  done

  # Check for either shasum or sha256sum/sha512sum
  local has_sha_tools=0
  if command -v shasum &> /dev/null; then
    has_sha_tools=1
  elif command -v sha256sum &> /dev/null && command -v sha512sum &> /dev/null; then
    has_sha_tools=1
  else
    echo "Error: Neither shasum nor sha256sum/sha512sum are installed or in PATH"
    missing_deps=1
  fi

  if [ $missing_deps -ne 0 ]; then
    echo "Please install missing dependencies and try again"
    echo "Rust toolchain can be installed from https://rustup.rs/"
    exit 1
  fi
}

case $# in
  0) VERSION="HEAD"
     SOURCE_KIND="local"
     ;;
  1) VERSION="TARBALL"
     SOURCE_KIND="local_tarball"
     LOCAL_TARBALL="$(realpath $1)"
     ;;
  2) VERSION="$1"
     RC_NUMBER="$2"
     SOURCE_KIND="tarball"
     ;;
  *) echo "Usage:"
     echo "  Verify release candidate:"
     echo "    $0 X.Y.Z RC_NUMBER"
     echo ""
     echo "  Run the source verification tasks on this spatialbench checkout:"
     echo "    $0"
     exit 1
     ;;
esac

# Check dependencies early
check_dependencies

# Note that these point to the current verify-release-candidate.sh directories
# which is different from the SPATIALBENCH_SOURCE_DIR set in ensure_source_directory()
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
SPATIALBENCH_DIR="$(cd "${SOURCE_DIR}/../.." && pwd)"

show_header() {
  if [ -z "$GITHUB_ACTIONS" ]; then
    echo ""
    printf '=%.0s' $(seq ${#1}); printf '\n'
    echo "${1}"
    printf '=%.0s' $(seq ${#1}); printf '\n'
  else
    echo "::group::${1}"; printf '\n'
  fi
}

show_info() {
  echo "└ ${1}"
}

SPATIALBENCH_DIST_URL='https://dist.apache.org/repos/dist/dev/sedona'

download_dist_file() {
  local url="${SPATIALBENCH_DIST_URL}/$1"
  if ! curl \
    --silent \
    --show-error \
    --fail \
    --location \
    --remote-name "$url"; then
    echo "Error: Failed to download $url"
    echo "This usually means the release candidate has not been uploaded yet."
    echo "To test locally, run: $0"
    echo "Or to test a local tarball: $0 /path/to/tarball.tar.gz"
    exit 1
  fi
}

download_rc_file() {
  download_dist_file sedona-spatialbench-${VERSION}-rc${RC_NUMBER}/$1
}

import_gpg_keys() {
  if [ "${GPGKEYS_ALREADY_IMPORTED:-0}" -gt 0 ]; then
    return 0
  fi
  download_dist_file KEYS

  if [ "${SPATIALBENCH_ACCEPT_IMPORT_GPG_KEYS_ERROR:-0}" -gt 0 ]; then
    gpg --import KEYS || true
  else
    gpg --import KEYS
  fi

  GPGKEYS_ALREADY_IMPORTED=1
}

if type shasum >/dev/null 2>&1; then
  sha512_verify="shasum -a 512 -c"
else
  sha512_verify="sha512sum -c"
fi

fetch_archive() {
  import_gpg_keys

  local dist_name=$1
  download_rc_file ${dist_name}-src.tar.gz
  download_rc_file ${dist_name}-src.tar.gz.asc
  download_rc_file ${dist_name}-src.tar.gz.sha512
  gpg --verify ${dist_name}-src.tar.gz.asc ${dist_name}-src.tar.gz
  ${sha512_verify} ${dist_name}-src.tar.gz.sha512
}

verify_dir_artifact_signatures() {
  import_gpg_keys

  # verify the signature and the checksums of each artifact
  find $1 -name '*.asc' | while read sigfile; do
    artifact=${sigfile/.asc/}
    gpg --verify $sigfile $artifact || exit 1

    # go into the directory because the checksum files contain only the
    # basename of the artifact
    pushd $(dirname $artifact)
    base_artifact=$(basename $artifact)
    if [ -f $base_artifact.sha512 ]; then
      ${sha512_verify} $base_artifact.sha512 || exit 1
    fi
    popd
  done
}

setup_tempdir() {
  cleanup() {
    if [ "${TEST_SUCCESS}" = "yes" ]; then
      rm -fr "${SPATIALBENCH_TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${SPATIALBENCH_TMPDIR} for details."
    fi
  }

  show_header "Creating temporary directory"

  if [ -z "${SPATIALBENCH_TMPDIR}" ]; then
    # clean up automatically if SPATIALBENCH_TMPDIR is not defined
    SPATIALBENCH_TMPDIR=$(mktemp -d -t "spatialbench-${VERSION}.XXXXXX")
    trap cleanup EXIT
  else
    # don't clean up automatically
    mkdir -p "${SPATIALBENCH_TMPDIR}"
  fi

  echo "Working in sandbox ${SPATIALBENCH_TMPDIR}"
}

test_rust() {
  show_header "Build and test Rust libraries"

  pushd "${SPATIALBENCH_SOURCE_DIR}"
  cargo test --workspace
  popd
}

test_rat() {
  show_header "Running Apache RAT license check"

  # Use SOURCE_DIR which was computed at script startup (before any directory changes)
  # Run RAT check using the run-rat.sh script from the repo
  bash "${SOURCE_DIR}/run-rat.sh" "${SPATIALBENCH_SOURCE_DIR}"
}

ensure_source_directory() {
  show_header "Ensuring source directory"

  dist_name="apache-sedona-spatialbench-${VERSION}"

  if [ "${SOURCE_KIND}" = "local" ]; then
    # Local repository
    if [ -z "$SPATIALBENCH_SOURCE_DIR" ]; then
      export SPATIALBENCH_SOURCE_DIR="${SPATIALBENCH_DIR}"
    fi
    echo "Verifying local spatialbench checkout at ${SPATIALBENCH_SOURCE_DIR}"
  elif [ "${SOURCE_KIND}" = "local_tarball" ]; then
    # Local tarball
    pushd $SPATIALBENCH_TMPDIR
    tar xf "$LOCAL_TARBALL"
    dist_name=$(ls)
    export SPATIALBENCH_SOURCE_DIR="${SPATIALBENCH_TMPDIR}/${dist_name}"

    popd

    echo "Verifying local tarball at $LOCAL_TARBALL"
  else
    # Release tarball
    echo "Verifying official SpatialBench release candidate ${VERSION}-rc${RC_NUMBER}"
    export SPATIALBENCH_SOURCE_DIR="${SPATIALBENCH_TMPDIR}/${dist_name}"
    if [ ! -d "${SPATIALBENCH_SOURCE_DIR}" ]; then
      pushd $SPATIALBENCH_TMPDIR
      fetch_archive ${dist_name}
      tar xf ${dist_name}-src.tar.gz

      popd
    fi
  fi
}

test_source_distribution() {
  pushd $SPATIALBENCH_SOURCE_DIR

  if [ ${TEST_RAT} -gt 0 ]; then
    test_rat
  fi

  if [ ${TEST_RUST} -gt 0 ]; then
    test_rust
  fi

  popd
}

# By default test all functionalities.
# To deactivate one test, deactivate the test and all of its dependents
# To explicitly select one test, set TEST_DEFAULT=0 TEST_X=1
: ${TEST_DEFAULT:=1}

: ${TEST_SOURCE:=${TEST_DEFAULT}}
: ${TEST_RAT:=${TEST_SOURCE}}
: ${TEST_RUST:=${TEST_SOURCE}}

TEST_SUCCESS=no

setup_tempdir
ensure_source_directory
test_source_distribution

TEST_SUCCESS=yes

echo "Release candidate ${VERSION}-RC${RC_NUMBER} looks good!"
exit 0
