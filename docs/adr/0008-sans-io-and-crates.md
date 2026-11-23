# ADR 0008. Sans-IO ядро и распил на 5 крейтов

## Статус

Принято (2026-11-23).

## Контекст

В `arq-rs` смешаны три разных вида кода: протокол очереди (`WATCH / MULTI / EXEC`, receipt, постусловия), процессы (supervisor, слоты, сигналы) и Python-граница (PyO3, `dispatch`, ping). Родительский supervisor не должен инициализировать CPython (см. `docs/architecture.md`). Монолитный крейт эту границу не защищает.

`QueuePort` из `simple-redis-q` (`483e9fc`) содержит только `enqueue` и `finish`. В нём нет `claim`, `renew`, `requeue`, `control`, `operation_id` и `cycle_id`. Второго бэкенда кроме Redis контракт не предусматривает.

## Решение

Логика состояний и протокола пишется по принципу Sans-IO: чистые типы и стейт-машины без сети, `tokio` и `pyo3`. Адаптеры подают в них события и выполняют их команды. Workspace плоский: `crates/<имя>`, виртуальный корневой `Cargo.toml`, общие версии в `[workspace.dependencies]`.

| Крейт | Содержимое | Зависит от |
|---|---|---|
| `arq-core` | Идентификаторы, `Config` (`ARQ_*`), разбор `OutcomeProposal`, ошибки, `AttemptLifecycle`, `AdmissionGate`, `SlotLifecycle`, `PingWatch`, план операций и проверка постусловий | ничего из IO |
| `arq-redis` | Конкретный `RedisBackend`: цикл `WATCH / MULTI / EXEC`, receipt, maintenance batch, `QueueAdmin`, `QueueControl`, реестр supervisor | `arq-core`, `redis`, `tokio` |
| `arq-pybridge` | PyO3: инициализация child-интерпретатора, вызов `App.dispatch` и `note_dead`, loop ping, Python-классы `Client`, `QueueAdmin`, `QueueControl` | `arq-core`, `arq-redis`, `pyo3` |
| `arq-supervisor` | Родитель без CPython: слоты, `watchexec-supervisor`, сигналы, память и RSS, backoff перезапуска, metrics helper | `arq-core`, `arq-redis`, `watchexec-supervisor` |
| `arq-worker` | Бинарник и CLI (`--app`, `--factory`), единственная точка сборки зависимостей | все четыре |

Правила:

- `arq-core` не зависит от `tokio`, `redis`, `pyo3` и файловой системы. Время и случайность приходят аргументами.
- `arq-supervisor` не зависит от `arq-pybridge` ни прямо, ни транзитивно.
- `QueuePort` не переносится. `RedisBackend` — конкретный тип без трейта-прослойки. Трейт вводится только при появлении второй реализации и новом ADR.
- `AttemptLifecycle`, `AdmissionGate` и `SlotLifecycle` живут в `arq-core`. Они считаются перенесёнными, только когда их вызывает цикл `arq-supervisor` или child и это покрыто интеграционным тестом (ADR 0001).
- Новые крейты добавляются только по новому ADR. Внутри крейта деление идёт модулями.

## Последствия

- Переходы, разбор исходов и проверка постусловий тестируются без Redis и Python.
- Граница «supervisor без CPython» проверяется компилятором и `cargo tree`, а не соглашением.
- Протокол Redis тестируется отдельно от Python, против Redis 8.6.2 и Cluster.
- Подмена хранилища потребует рефакторинга `arq-redis` и нового ADR. Это принятая цена отказа от трейтов с одной реализацией.
- `docs/architecture.md` не содержит строки `Queue port` и описывает эти пять крейтов. `docs/migration.md` в секции незакрытого runtime оставляет `SlotLifecycle`, `AdmissionGate` и `AttemptLifecycle`.
