# ADR 0001. Граница переноса runtime

## Статус

Принято (2026-11-23).

## Контекст

PR №42 `hewimetall/simple-redis-q` на SHA `483e9fc063103708bdf37bfe80ddbc91af2a2fd5` содержит и работающий parent/child path, и библиотеки, которые supervisor loop не вызывает. Описание PR прямо отделяет slot state machine, `QueuePort` и drain gate от production adapter. Metrics helper запускается отдельным `tokio::process`.

## Решение

Перенос копирует только компоненты с указанным вызывающим путём. Для каждого компонента manifest в `docs/architecture.md` называет исходный символ, вызывающий путь донора, точку подключения arq-rs и тесты.

`supervise_slot` в `arq-supervisor` проводит `SlotLifecycle` событиями `ProcessEvent` Ready, Drain, Stop, Reaped, Crash и StartFailed. `arq-supervisor` вызывает `AdmissionGate`: `supervisor.rs` закрывает ворота на drain, `child.rs` проверяет `blocks_claim` перед claim. Цикл child и supervisor машину `AttemptLifecycle` не проводят. Строка manifest для неё остаётся открытой, пока один из этих циклов не пошлёт событие и интеграционный тест не покроет вызов.

`QueuePort` не переносится. Транспорт реализуется напрямую через `RedisBackend`, без второй реализации и без порта. `RedisBackend` выполняет `enqueue`, `claim`, `renew`, `finish`, `requeue` и `control` из `docs/operations.md`. `control` включает `cancel`, `kill` и `settle`. Ему же принадлежат maintenance batch и чтение для `QueueAdmin`. Старый `redis_api` донора тоже не переносится.

## Последствия

Реализация, которая копирует crate без вызова из supervisor или child, не закрывает соответствующий пункт manifest. Интеграционный тест из колонки manifest обязателен. Размещение крейтов, дистрибуция и отказ от `QueuePort` задаёт [ADR 0008](0008-sans-io-workspace-crates.md).
