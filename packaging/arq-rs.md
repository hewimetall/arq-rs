# arq-rs

Redis queue for Python async handlers. Rust owns the transport, delivery, and child processes. Python runs the handler, middleware, and lifecycle hooks.

Importing `arq_rs` does not open Redis. `Client` opens the connection.

## Install

```bash
pip install arq-rs
```

Published wheels are CPython 3.12, manylinux2014, `x86_64` and `aarch64`. Python 3.13 and newer have no wheel yet.

The worker process is a separate package: [arq-rs-worker](https://pypi.org/project/arq-rs-worker/).

## Declare a handler

```python
from arq_rs import App, Client, Config, actor


@actor(queue="default", max_retries=2)
async def process_item(item_id: str) -> None:
    ...


def create_app() -> App:
    client = Client("redis://localhost:6379/0", config=Config.from_env())
    return App(client=client, actors=[process_item])
```

`actor()` returns an `Actor`. The default name is the function name. The handler is called as `function(**kwargs)`. A queue name is a non-empty `[a-z0-9]+` string.

Enqueue with `await client.send(process_item, item_id="1")`. That returns the `message_id`.

## What the library does

It enqueues tasks, delays them, deduplicates an active task, renews the lease, redelivers, retries, and keeps a dead-letter queue. Reading the queue and controlling it are separate interfaces.

It does not include a scheduler, a workflow engine, a dependency-injection container, business locks, or coordination of external transactions.

## Run the worker

```bash
pip install arq-rs-worker
REDIS_URL=redis://localhost:6379/0 arq-worker --app module:create_app --factory --queues default
```

## Contract

The normative contract and the API live in the repository: <https://github.com/hewimetall/arq-rs>
