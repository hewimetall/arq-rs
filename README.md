# arq-rs

Redis-очередь для Python async-обработчиков. Rust выполняет транспорт, владеет доставкой и управляет child-процессами. Python выполняет handler, middleware и lifecycle.

**Статус: нормативный контракт.** Код очереди в этом репозитории ещё не написан. Наличие правила в документации не означает, что оно уже реализовано. Предложенное поведение ниже и есть контракт реализации; расхождение с ним оформляется ADR вместе с кодом и тестами.

Дата контракта: 2026-11-23.

Исходный execution runtime: [`hewimetall/simple-redis-q`](https://github.com/hewimetall/simple-redis-q) `483e9fc063103708bdf37bfe80ddbc91af2a2fd5` (head PR №42). Перенос покомпонентно описан в [docs/architecture.md](docs/architecture.md). Модуль, не вызванный production-путём донора, не считается уже интегрированным runtime.

## Границы

Библиотека ставит задачи, откладывает их, дедуплицирует активную задачу, продлевает lease, повторяет выдачу, выполняет прикладной retry, ведёт DLQ и даёт отдельные интерфейсы чтения и управления.

Библиотека не содержит scheduler, workflow engine, DI-контейнер, бизнес-блокировки и координацию внешних транзакций.

## Документы

| Документ | Содержание |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Процессы, границы Rust и Python, manifest переноса |
| [docs/api.md](docs/api.md) | `App`, `Client`, middleware, admin, control, конфигурация |
| [docs/redis-keys.md](docs/redis-keys.md) | Ключи, поля задачи, receipt, retention |
| [docs/operations.md](docs/operations.md) | Транзакции, исходы, retry, DLQ, cancel/kill, shutdown |
| [docs/observability.md](docs/observability.md) | Метрики, логи, трассировка |
| [docs/testing.md](docs/testing.md) | Обязательные интеграционные тесты |
| [docs/migration.md](docs/migration.md) | Отличия от закреплённой версии simple-q |
| [docs/adr/](docs/adr/) | Принятые развилки контракта |

Примеры в документации синтетические: обработчик `process_item`, очередь `default`, аргумент `item_id`.
