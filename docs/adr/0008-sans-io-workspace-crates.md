# ADR 0008. Sans-IO ядро, 5 крейтов и дистрибуция

## Статус

Принято (2026-11-23).

## Контекст

В `arq-rs` смешаны три вида кода: протокол очереди (`WATCH / MULTI / EXEC`, receipt, постусловия), процессы (supervisor, слоты, сигналы) и Python-граница (PyO3, `dispatch`, ping). Родительский supervisor не должен инициализировать CPython. Монолитный крейт эту границу не защищает.

`QueuePort` из `simple-redis-q` (`483e9fc`) содержит только `enqueue` и `finish`: в нём нет `claim`, `renew`, `requeue`, `control`, `operation_id` и `cycle_id`. Второго хранилища кроме Redis контракт не предусматривает.

Донор уже выпускает два wheel. Расширение собирается maturin'ом с feature `extension-module`. Бинарник `agent-worker` этот feature не включает и линкуется с libpython.

## Решение

### Структура

Workspace плоский: `crates/<имя>`, виртуальный корневой `Cargo.toml`, общие версии в `[workspace.dependencies]` и `[workspace.package]`. Логика состояний и протокола пишется по принципу Sans-IO: чистые типы и стейт-машины без сети, `tokio` и `pyo3`. Адаптеры подают в них события и исполняют их команды.

| Крейт | Содержимое | Зависит от | Публикация |
|---|---|---|---|
| `arq-core` | Идентификаторы, `Config` (`ARQ_*`), разбор `OutcomeProposal`, ошибки, `AttemptLifecycle`, `AdmissionGate`, `SlotLifecycle`, `PingWatch`, план операций и проверка постусловий | только `std` и `serde` | нет |
| `arq-redis` | Конкретный `RedisBackend`: цикл `WATCH / MULTI / EXEC`, receipt, maintenance batch, `QueueAdmin`, `QueueControl`, реестр supervisor | `arq-core`, `redis`, `tokio` | нет |
| `arq-pybridge` | PyO3: `Client`, `QueueAdmin`, `QueueControl`, вызов `App.dispatch` и `note_dead`, loop ping, инициализация child-интерпретатора | `arq-core`, `arq-redis`, `pyo3` | wheel `arq-rs` |
| `arq-supervisor` | Родитель без CPython: слоты, `watchexec-supervisor`, сигналы, память и RSS, backoff, metrics helper | `arq-core`, `arq-redis`, `watchexec-supervisor` | нет |
| `arq-worker` | Бинарник и CLI (`--app`, `--factory`), единственная точка сборки зависимостей | все четыре | wheel `arq-rs-worker` |

### Правила зависимостей

- `arq-core` не зависит от `tokio`, `redis`, `pyo3` и файловой системы. Время и случайность приходят аргументами.
- `arq-supervisor` не зависит от `arq-pybridge` ни прямо, ни транзитивно. Проверка: `cargo tree -p arq-supervisor -i pyo3` возвращает пустой результат.
- `QueuePort` не переносится. `RedisBackend` — конкретный тип без трейта-прослойки. Трейт вводится только при появлении второй реализации и отдельном ADR.
- `AttemptLifecycle`, `AdmissionGate` и `SlotLifecycle` живут в `arq-core`. `supervise_slot` проводит `SlotLifecycle`. Production-цикл `crates/arq-worker/src/delivery.rs` держит локальный `AdmissionGate`: `parent_or_signal_drain` закрывает его при выходе родителя и при `SIGUSR1`/`SIGTERM`/`SIGINT`, перед claim `blocks_claim` проверяет его же. Сценарная сессия `arq-supervisor/src/child.rs` использует ту же функцию. `close_admission` закрывает поле `shared.gate`, другой экземпляр. Закрытие ворот родителя само по себе claim child не останавливает. Production-цикл `crates/arq-worker/src/delivery.rs` проводит `AttemptLifecycle`: один экземпляр на доставку, `BeginFinish` один раз на логическую фиксацию, `FinishUnconfirmed` только для окончательно неподтверждённого исхода. Сценарная сессия `arq-supervisor` машину не проводит. Вызов покрывает `crates/arq-worker/tests/deliver_attempt.rs` (ADR 0001).
- Новые крейты добавляются только по новому ADR. Внутри крейта деление идёт модулями.

### Дистрибуция

Публикуются два дистрибутива одной версии из `workspace.package.version`.

| Дистрибутив | Содержимое | Сборка |
|---|---|---|
| `arq-rs` | Python-пакет `arq_rs` и расширение `arq_rs._native` | `maturin` из корневого `pyproject.toml` |
| `arq-rs-worker` | бинарник `arq-worker` | `scripts/build-worker-wheel.sh` против общей `libpython3.12` |

Корневой `pyproject.toml`:

```toml
[build-system]
requires = ["maturin>=1.16,<2"]
build-backend = "maturin"

[tool.maturin]
manifest-path = "crates/arq-pybridge/Cargo.toml"
features = ["extension-module"]
python-source = "python"
python-packages = ["arq_rs"]
module-name = "arq_rs._native"
```

- `arq-pybridge` собирается как `crate-type = ["cdylib", "rlib"]`. Feature `extension-module = ["pyo3/extension-module"]` включает только maturin.
- `arq-worker` этот feature не включает, линкуется с libpython и зависит от `arq-pybridge` как от rlib.
- `.github/workflows/wheels.yml` собирает wheel `arq-rs` через `PyO3/maturin-action` и `-i python3.12` для `x86_64` и `aarch64`. Wheel `arq-rs-worker` собирает `scripts/build-worker-wheel.sh` на раннере `ubuntu-24.04` (`x86_64`) и `ubuntu-24.04-arm` (`aarch64`) против общей `libpython3.12.so.1.0`. Образ manylinux собирает CPython с `--disable-shared` и этой библиотеки не содержит.
- Публикация запускается только на push аннотированного тега `v*`, после job `release-gate`. `arq-rs` идёт из GitHub Environment `pypi`, `arq-rs-worker` из `pypi-worker`: одна связка owner/repo/workflow/environment на PyPI принадлежит одному проекту. У обоих job права `id-token: write`, `contents: read` и `actions: read`. Каждый job оставляет в `dist` только свои wheel.
- Обязательные ревьюеры сред `pypi` и `pypi-worker` задаются настройкой репозитория GitHub. Workflow эту настройку не создаёт. Создание тегов `v*` остаётся ограниченным мейнтейнерами.
- `workflow_dispatch` собирает wheel и запускает smoke. Публикацию этот запуск не включает.
- Smoke скачивает артефакты `wheels-*-x86_64` и задаёт `SMOKE_ARCH=x86_64`. `scripts/smoke-venv.sh` берёт платформенный тег как последний компонент имени по `-`. Совпадение архитектуры — суффикс этого тега, поэтому `x86_64` читается целиком в конце тега. Если для архитектуры и тега CPython остаётся больше одного wheel, скрипт завершается с ошибкой.
- Smoke ставит оба пакета одной версии и запускает `arq-worker --version` и `python -c 'import arq_rs'`.

## Последствия

- Переходы, разбор исходов и проверка постусловий тестируются без Redis и Python.
- Граница «supervisor без CPython» проверяется компилятором и `cargo tree`, а не соглашением.
- Протокол Redis тестируется отдельно от Python, против Redis 8.6.2 и Cluster.
- Подмена хранилища потребует рефакторинга `arq-redis` и нового ADR. Это принятая цена отказа от трейтов с одной реализацией.
- Пользователь ставит два пакета. Версии обязаны совпадать, это проверяет smoke-тест. Отказ от единого wheel принят, потому что совмещение расширения и бинарника в одном wheel у maturin нетривиально.
- Раздел крейтов и отказ от строки `Queue port` уже стоят в `docs/architecture.md`. Разделение `SlotLifecycle`, `AdmissionGate` и `AttemptLifecycle` уже стоит в `docs/migration.md`.
- Ряды `arq_*` и multiprocess-экспорт Python не сведены. `python/arq_rs/metrics.py` записывает `dramatiq_*`. Крейты Rust ряды `arq_*` не выпускают. Сведение задаёт отдельный ADR. Имена и labels в `docs/observability.md` не меняются.
