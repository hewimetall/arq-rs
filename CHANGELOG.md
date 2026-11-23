# Changelog

## Unreleased

Выпуск содержит нормативный контракт Redis-очереди от 2026-11-23 и реализацию в `crates/` и `python/arq_rs`. Расхождение с документом оформляется ADR.

Контракт определяет:

- Python-поверхность `App`, `Client`, `Middleware.install` и порядок hooks закреплённой версии simple-q;
- момент вызова `on_dead` до фиксации DLQ в Redis;
- разделение предложения исхода Python и транспортного commit Rust;
- протокол `WATCH / MULTI / EXEC` с receipt и остановкой при частичном `EXEC`;
- cancel/kill, видимые claim в той же транзакции, и терминальные причины;
- `message_id` как значение `task_id` первой версии;
- manifest переноса runtime с SHA `483e9fc063103708bdf37bfe80ddbc91af2a2fd5`.

Уточнения контракта:

- `WATCH` покрывает receipt, hash задачи и uniq. Общие ZSET индексов под `WATCH` не ставятся.
- `ARQ_MAX_READY` — мягкий предел `ZCARD` всего ready, включая отложенные задачи.
- `QueueControl.settle` переводит заблокированную inflight-задачу в DLQ после истечения lease, если процесс нельзя связать со slot.
- `arq_queue_depth` делит один ZSET ready на непересекающиеся ряды `ready` и `scheduled`. Их сумма сравнивается с `ARQ_MAX_READY`.

Учтены отказы соседних очередей и ограничения донора, при пустых списках issue у `hewimetall/arq-rs` и `hewimetall/simple-redis-q`:

- выдача идёт окном из 16 кандидатов, без отдельного pop до `MULTI` и без Redis Streams;
- null `EXEC` в RESP3, `EXECABORT`, закрытие соединения и отказ от чтения с реплики заданы явно;
- выселение ключей без TTL, потерянный AOF и наложение исполнений описаны как границы гарантии;
- пропущенный heartbeat не снимает uniq, а `cancel` и `kill` не зависят от pubsub;
- handler не стартует по потерянному ответу claim и не стартует, если lease уже истёк к повторному `TIME`.

Реализация:

- Workspace состоит из `arq-core`, `arq-redis`, `arq-pybridge`, `arq-supervisor` и `arq-worker`. Ядро не зависит от сети, `tokio` и `pyo3`. `arq-supervisor` не зависит от `arq-pybridge`.
- `QueuePort` не переносится. Транспорт — конкретный `RedisBackend` с операциями `enqueue`, `claim`, `renew`, `finish`, `requeue`, `cancel`, `kill` и `settle`.
- Имена ключей очереди собирает `QueueKeys` в `arq-core`. `RedisBackend` пишет `ready`, `inflight`, `done`, `dead`, `dead:tag`, `job`, `uniq` и `receipt`. Реестр `arq:supervisors`, hash supervisor и hash child пишет `arq-supervisor`.
- `supervise_slot` в `arq-supervisor` проводит `SlotLifecycle` событиями `ProcessEvent` Ready, Drain, Stop, Reaped, Crash и StartFailed.
- `arq-supervisor` вызывает `AdmissionGate`: `child.rs` проверяет `blocks_claim` перед claim, `supervisor.rs` закрывает ворота на drain.
- Пакет `python/arq_rs` содержит поверхность очереди. Имена `dramatiq_*` записывает `python/arq_rs/metrics.py`.
- `Json::parse` отвергает вложенность глубже `MAX_JSON_DEPTH` = 64 и возвращает `nesting too deep`. Глубина от `ARQ_MAX_MESSAGE_BYTES` не зависит.
- `.github/workflows/wheels.yml` собирает wheel `arq-rs` и `arq-rs-worker` через `PyO3/maturin-action` для `x86_64` и `aarch64`. Публикация идёт после job `release-gate` только на push аннотированного тега `v*`, в GitHub Environment `pypi`, с правами `id-token: write`, `contents: read` и `actions: read`. `workflow_dispatch` публикацию не включает. Обязательные ревьюеры среды `pypi` задаёт репозиторий. Создание тегов `v*` остаётся ограниченным мейнтейнерами. Smoke берёт `wheels-*-x86_64`, задаёт `SMOKE_ARCH=x86_64` и завершается при неоднозначном наборе wheel.

ADR 0008 принят 2026-11-23. Раздел крейтов и отказ от `QueuePort` уже стоят в `docs/architecture.md` и `docs/migration.md`.

Открыто:

- `AttemptLifecycle` реализован и покрыт unit-тестами в `arq-core`. Цикл child и `arq-supervisor` событий ему не посылают. Строка manifest остаётся открытой до этого вызова и интеграционного теста (ADR 0001, ADR 0008).
- Ряды `arq_*` из `docs/observability.md` крейты Rust не выпускают. Общий multiprocess-экспорт с метриками Python задаёт отдельный ADR. Имена и labels контракта не меняются.
