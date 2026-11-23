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

# --check validates the version pin and does not compile.
# An optional target (x86_64, aarch64, or a rustc triple) is forwarded to
# cargo and maturin. CI does not cross-compile here: wheels.yml passes
# matrix.target to PyO3/maturin-action, which provides the manylinux sysroot.
# With no target this script builds for the host.
if [ "${1:-}" = "--check" ]; then
  exit 0
fi

target=${1:-}
case "$target" in
  "")
    triple=""
    platform=""
    ;;
  x86_64)
    triple=x86_64-unknown-linux-gnu
    platform=x86_64
    ;;
  aarch64)
    triple=aarch64-unknown-linux-gnu
    platform=aarch64
    ;;
  *-*-*)
    triple=$target
    platform=${target%%-*}
    ;;
  *)
    echo "unknown target: ${target}" >&2
    echo "use x86_64, aarch64, or a rustc triple" >&2
    exit 1
    ;;
esac

# The cargo binary is the source of the wheel. It does not pass
# --features extension-module.
if [ -n "$triple" ]; then
  cargo build --release -p arq-worker --target "$triple"
  built="target/${triple}/release/arq-worker"
else
  cargo build --release -p arq-worker
  built="target/release/arq-worker"
fi
mkdir -p dist
cp -f "$built" dist/arq-worker

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
if [ -n "$triple" ]; then
  maturin_ok=0
  "$maturin" build --release --target "$triple" --manifest-path crates/arq-worker/Cargo.toml --out dist --auditwheel skip && maturin_ok=1
else
  maturin_ok=0
  "$maturin" build --release --manifest-path crates/arq-worker/Cargo.toml --out dist --auditwheel skip && maturin_ok=1
fi
if [ "$maturin_ok" -ne 1 ]; then
  echo "arq-rs-worker wheel build failed." >&2
  echo "The release binary is still ${built} and dist/arq-worker." >&2
  exit 1
fi

if [ -n "$platform" ]; then
  wheel_name="arq_rs_worker-${version}-*${platform}*.whl"
else
  wheel_name="arq_rs_worker-${version}-*.whl"
fi
wheel_list=$(find dist -maxdepth 1 -type f -name "$wheel_name" | sort || true)
if [ -z "${wheel_list}" ]; then
  echo "maturin finished but dist has no ${wheel_name}" >&2
  exit 1
fi
count=$(printf '%s\n' "$wheel_list" | wc -l | tr -d ' ')
if [ "$count" -ne 1 ]; then
  echo "expected one ${wheel_name}, found ${count}:" >&2
  printf '%s\n' "$wheel_list" >&2
  exit 1
fi

echo "arq-rs-worker ${version} wheel is ${wheel_list}"
echo "arq-rs uses the same version; a combined wheel is not produced"
