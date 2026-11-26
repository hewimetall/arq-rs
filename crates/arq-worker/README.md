# arq-rs-worker

`arq-worker` binary for [arq-rs](https://pypi.org/project/arq-rs/). The supervisor is Rust. One task runs in one child process. The parent never initializes CPython.

## Install

```bash
pip install arq-rs-worker
```

The package depends on `arq-rs` of the same version.

Published wheels are CPython 3.12, `manylinux_2_39`, `x86_64` and `aarch64`. The wheel vendors `libpython3.12`. The standard library is the machine's CPython 3.12, and the machine needs glibc 2.39 or newer (Ubuntu 24.04 or newer).

## Run

`REDIS_URL` is required. Settings are `ARQ_*` environment variables.

```bash
REDIS_URL=redis://localhost:6379/0 arq-worker --app module:app --queues default
REDIS_URL=redis://localhost:6379/0 arq-worker --app module:create_app --factory --queues default
```

`--app module:attribute` is the `App` object. With `--factory`, the child calls that attribute and it must return an `App`. `--queues` is a comma-separated list of `[a-z0-9]+` names. `--processes` is the number of supervised children. `--concurrency` is the reserved in-flight tasks per child.

`arq-worker --version` prints the version and does not read `REDIS_URL` or `ARQ_*`.

## Contract

<https://github.com/hewimetall/arq-rs>
