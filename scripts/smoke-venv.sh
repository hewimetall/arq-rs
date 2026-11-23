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

# The platform tag is the last "-" component of the wheel filename.
# x86_64 itself contains "_", so the arch is matched as a suffix of that tag.
host_platform() {
  case $(uname -m) in
    x86_64|amd64) printf '%s\n' x86_64 ;;
    aarch64|arm64) printf '%s\n' aarch64 ;;
    *) uname -m ;;
  esac
}

platform_of() {
  name=$(basename "$1" .whl)
  tag=${name##*-}
  case "$tag" in
    *x86_64) printf '%s\n' x86_64 ;;
    *aarch64|*arm64) printf '%s\n' aarch64 ;;
    any) printf '%s\n' any ;;
    *) printf '%s\n' "$tag" ;;
  esac
}

# Print exactly one wheel that can be installed on this machine.
# A directory that contains both x86_64 and aarch64 must not fall through to
# whichever file find returns first.
pick_wheel() {
  kind=$1
  include=$2
  exclude=$3
  found=$(mktemp)
  compatible=$(mktemp)
  narrowed=$(mktemp)
  find "$wheels" -maxdepth 1 -type f -name "$include" ! -name "$exclude" | sort > "$found"
  if [ ! -s "$found" ]; then
    echo "smoke needs a ${kind} wheel in ${wheels}" >&2
    echo "Build arq-rs with maturin from the root pyproject and arq-rs-worker with scripts/build-worker-wheel.sh." >&2
    rm -f "$found" "$compatible" "$narrowed"
    exit 1
  fi
  host=${SMOKE_ARCH:-$(host_platform)}
  while IFS= read -r wheel; do
    platform=$(platform_of "$wheel")
    if [ "$platform" = "$host" ] || [ "$platform" = "any" ]; then
      printf '%s\n' "$wheel" >> "$compatible"
    fi
  done < "$found"
  if [ ! -s "$compatible" ]; then
    echo "smoke on ${host} found no ${kind} wheel for this machine in ${wheels}" >&2
    echo "present:" >&2
    sed 's/^/  /' "$found" >&2
    rm -f "$found" "$compatible" "$narrowed"
    exit 1
  fi
  count=$(wc -l < "$compatible" | tr -d ' ')
  chosen=$compatible
  if [ "$count" -gt 1 ]; then
    py=$(python3 -c 'import sys; print("cp%d%d" % sys.version_info[:2])')
    while IFS= read -r wheel; do
      base=$(basename "$wheel")
      case "$base" in
        *-"$py"-*) printf '%s\n' "$wheel" >> "$narrowed" ;;
      esac
    done < "$compatible"
    if [ -s "$narrowed" ]; then
      count=$(wc -l < "$narrowed" | tr -d ' ')
      chosen=$narrowed
    fi
  fi
  if [ "$count" -ne 1 ]; then
    echo "smoke found ${count} ${kind} wheels for ${host} and will not pick one" >&2
    sed 's/^/  /' "$chosen" >&2
    rm -f "$found" "$compatible" "$narrowed"
    exit 1
  fi
  cat "$chosen"
  rm -f "$found" "$compatible" "$narrowed"
}

runtime=$(pick_wheel "arq-rs" "arq_rs-*.whl" "arq_rs_worker-*") || exit 1
worker=$(pick_wheel "arq-rs-worker" "arq_rs_worker-*.whl" "no-such-wheel-*") || exit 1

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

if [ "${SMOKE_SELECT_ONLY:-}" = 1 ]; then
  printf '%s\n' "$runtime"
  printf '%s\n' "$worker"
  exit 0
fi

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
