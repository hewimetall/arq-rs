#!/bin/sh
# Build the arq-rs-worker artifact.
# This is not the maturin extension build: extension-module stays off and the
# binary links libpython. Both wheels use workspace.package.version.
set -eu
cd "$(dirname "$0")/.."
mkdir -p dist
cargo build --release -p arq-worker
cp target/release/arq-worker dist/arq-worker
echo "arq-rs-worker binary is dist/arq-worker"
echo "wheel version matches workspace.package.version; a combined wheel is not produced"
