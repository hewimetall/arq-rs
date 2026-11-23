#!/bin/sh
# Clean-venv smoke check from ADR 0008.
# Installs arq-rs and arq-rs-worker of one workspace.package.version, then runs:
#   arq-worker --version
#   python -c "import arq_rs"
# Pass a directory that contains both wheels. This script does not publish.
set -eu

wheels=${1:-dist}
venv=${VENV:-/tmp/arq-rs-smoke-venv}
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)

version=$(awk '
  /^\[workspace\.package\]/ { in_pkg = 1; next }
  in_pkg && /^\[/ { exit }
  in_pkg && /^version[[:space:]]*=/ {
    gsub(/"/, "", $3)
    print $3
    exit
  }
' "$root/Cargo.toml")

if [ -z "${version}" ]; then
  echo "workspace.package.version is missing from Cargo.toml" >&2
  exit 1
fi

runtime=$(find "$wheels" -maxdepth 1 -type f -name 'arq_rs-*.whl' ! -name 'arq_rs_worker-*' -print -quit || true)
worker=$(find "$wheels" -maxdepth 1 -type f -name 'arq_rs_worker-*.whl' -print -quit || true)
if [ -z "${runtime}" ] || [ -z "${worker}" ]; then
  echo "smoke needs both wheels in ${wheels}: arq_rs-*.whl and arq_rs_worker-*.whl" >&2
  echo "Build arq-rs with maturin from the root pyproject and arq-rs-worker with scripts/build-worker-wheel.sh." >&2
  exit 1
fi

case $(basename "$runtime") in
  arq_rs-"${version}"-*) ;;
  *)
    echo "arq-rs wheel $(basename "$runtime") is not version ${version}" >&2
    exit 1
    ;;
esac
case $(basename "$worker") in
  arq_rs_worker-"${version}"-*) ;;
  *)
    echo "arq-rs-worker wheel $(basename "$worker") is not version ${version}" >&2
    exit 1
    ;;
esac

python3 -m venv "$venv"
# shellcheck disable=SC1091
. "$venv/bin/activate"
python -m pip install --disable-pip-version-check "$runtime" "$worker"

arq_ver=$(python -c 'import importlib.metadata as m; print(m.version("arq-rs"))')
worker_ver=$(python -c 'import importlib.metadata as m; print(m.version("arq-rs-worker"))')
if [ "$arq_ver" != "$worker_ver" ] || [ "$arq_ver" != "$version" ]; then
  echo "version mismatch: workspace ${version} arq-rs ${arq_ver} arq-rs-worker ${worker_ver}" >&2
  exit 1
fi

printed=$(arq-worker --version)
case "$printed" in
  *" ${version}") ;;
  *)
    echo "arq-worker --version printed '${printed}', want version ${version}" >&2
    exit 1
    ;;
esac

python -c 'import arq_rs'
echo "smoke ok: arq-rs ${arq_ver} and arq-worker ${printed}"
