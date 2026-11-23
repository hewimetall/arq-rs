# ADR 0008. Sans-IO ядро, 5 крейтов и дистрибуция

## Статус

Предложено (2026-11-23).

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
- `AttemptLifecycle`, `AdmissionGate` и `SlotLifecycle` живут в `arq-core`. Строка manifest закрывается, только когда их вызывает цикл supervisor или child и это покрыто интеграционным тестом (ADR 0001).
- Новые крейты добавляются только по новому ADR. Внутри крейта деление идёт модулями.

### Дистрибуция

Публикуются два дистрибутива одной версии из `workspace.package.version`.

| Дистрибутив | Содержимое | Сборка |
|---|---|---|
| `arq-rs` | Python-пакет `arq_rs` и расширение `arq_rs._native` | `maturin` из корневого `pyproject.toml` |
| `arq-rs-worker` | бинарник `arq-worker` | отдельный скрипт сборки wheel |

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
- CI собирает manylinux-wheel под `x86_64` и `aarch64` через `PyO3/maturin-action`. Версии Python задаются списком интерпретаторов.
- Публикация идёт двумя вызовами `gh-action-pypi-publish`, файлы делятся по префиксу имени.
- Smoke-проверка в чистом venv ставит оба пакета одной версии и запускает `arq-worker --version` и `python -c 'import arq_rs'`.

## Последствия

- Переходы, разбор исходов и проверка постусловий тестируются без Redis и Python.
- Граница «supervisor без CPython» проверяется компилятором и `cargo tree`, а не соглашением.
- Протокол Redis тестируется отдельно от Python, против Redis 8.6.2 и Cluster.
- Подмена хранилища потребует рефакторинга `arq-redis` и нового ADR. Это принятая цена отказа от трейтов с одной реализацией.
- Пользователь ставит два пакета. Версии обязаны совпадать, это проверяет smoke-тест. Отказ от единого wheel принят, потому что совмещение расширения и бинарника в одном wheel у maturin нетривиально.
- Нужно обновить `docs/architecture.md` (убрать строку `Queue port`, добавить раздел о крейтах) и `docs/migration.md` (оставить `SlotLifecycle`, `AdmissionGate`, `AttemptLifecycle`).
- Открытый вопрос: как Rust-метрики `arq_*` попадают в тот же multiprocess-экспорт, что и Python-метрики. Он решается отдельным ADR.
