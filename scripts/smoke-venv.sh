#!/bin/sh
# Clean-venv smoke check from ADR 0008.
# Installs arq-rs and arq-rs-worker of one version, then runs:
#   arq-worker --version
#   python -c "import arq_rs"
# Pass a directory that contains both wheels. This script is the check; it is
# not expected to pass before those wheels are built.
set -eu
wheels=${1:-dist}
venv=${VENV:-/tmp/arq-rs-smoke-venv}
python -m venv "$venv"
# shellcheck disable=SC1091
. "$venv/bin/activate"
pip install --disable-pip-version-check "$wheels"/arq_rs-*.whl "$wheels"/arq_rs_worker-*.whl
arq-worker --version
python -c "import arq_rs"
