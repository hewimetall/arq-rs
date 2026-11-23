#!/bin/sh
# Build the arq-rs-worker wheel.
#
# Two packages share workspace.package.version:
#   arq-rs        Python package arq_rs and extension arq_rs._native
#                 (maturin from the root pyproject, feature extension-module)
#   arq-rs-worker binary arq-worker
#
# This script builds only the worker. It does not enable extension-module.
# The binary links libpython and the arq-pybridge rlib. A combined wheel is
# not produced.
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$root"

version=$(awk '
  /^\[workspace\.package\]/ { in_pkg = 1; next }
  in_pkg && /^\[/ { exit }
  in_pkg && /^version[[:space:]]*=/ {
    gsub(/"/, "", $3)
    print $3
    exit
  }
' Cargo.toml)

if [ -z "${version}" ]; then
  echo "workspace.package.version is missing from Cargo.toml" >&2
  exit 1
fi

if ! grep -q "arq-rs==${version}" crates/arq-worker/pyproject.toml; then
  echo "arq-rs-worker must depend on arq-rs==${version} (workspace.package.version)" >&2
  exit 1
fi

# The cargo binary is the source of the wheel. It does not pass
# --features extension-module.
cargo build --release -p arq-worker
mkdir -p dist
cp -f target/release/arq-worker dist/arq-worker

if command -v maturin >/dev/null 2>&1; then
  maturin=$(command -v maturin)
else
  echo "maturin is not installed on PATH." >&2
  echo "Installing maturin into ${root}/.venv-maturin (a local virtualenv, not the system Python)." >&2
  python3 -m venv "${root}/.venv-maturin"
  "${root}/.venv-maturin/bin/python" -m pip install --disable-pip-version-check "maturin>=1.16,<2"
  maturin="${root}/.venv-maturin/bin/maturin"
fi

# bindings = bin. extension-module stays off. auditwheel is skipped so a
# missing patchelf or a network fetch does not hide the cargo binary.
# --manifest-path is the Cargo.toml. Maturin reads the pyproject.toml beside it
# (bindings = bin) and does not pass --features extension-module.
if ! "$maturin" build --release --manifest-path crates/arq-worker/Cargo.toml --out dist --auditwheel skip; then
  echo "arq-rs-worker wheel build failed." >&2
  echo "The release binary is still target/release/arq-worker and dist/arq-worker." >&2
  exit 1
fi

wheel=$(find dist -maxdepth 1 -type f -name "arq_rs_worker-${version}-*.whl" -print -quit || true)
if [ -z "${wheel}" ]; then
  echo "maturin finished but dist has no arq_rs_worker-${version}-*.whl" >&2
  exit 1
fi

echo "arq-rs-worker ${version} wheel is ${wheel}"
echo "arq-rs uses the same version; a combined wheel is not produced"
