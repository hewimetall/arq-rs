# arq-rs

Redis-очередь для Python async-обработчиков. Rust выполняет транспорт, владеет доставкой и управляет child-процессами. Python выполняет handler, middleware и lifecycle.

**Статус: нормативный контракт.** Код очереди лежит в крейтах `arq-core`, `arq-redis`, `arq-pybridge`, `arq-supervisor`, `arq-worker` и в пакете `python/arq_rs`. Транспорт выполняет `RedisBackend` в `arq-redis`. Текст документов задаёт контракт. Расхождение с ним оформляется ADR.

Дата контракта: 2026-11-23.

Исходный execution runtime: [`hewimetall/simple-redis-q`](https://github.com/hewimetall/simple-redis-q) `483e9fc063103708bdf37bfe80ddbc91af2a2fd5` (head PR №42). Перенос покомпонентно описан в [docs/architecture.md](docs/architecture.md). Модуль, не вызванный production-путём донора, не считается уже интегрированным runtime.

## Не закрыто

Ряды `arq_*` из [docs/observability.md](docs/observability.md) крейты Rust не выпускают. `python/arq_rs/metrics.py` записывает имена `dramatiq_*`. Сведение `arq_*` в тот же multiprocess-экспорт — отдельный ADR. Имена и labels в `docs/observability.md` до этого ADR не меняются.

`Json::parse` отвергает документ глубже 64 значений и возвращает сообщение `nesting too deep`. Предел — `MAX_JSON_DEPTH` в `crates/arq-core`. Значение верхнего уровня считается за одно. Тот же предел записан рядом с `ARQ_MAX_MESSAGE_BYTES` в [docs/api.md](docs/api.md). Глубина от байтового потолка не зависит.

Публикация wheel запускается только push-событием аннотированного тега `v*`. `arq-rs` публикуется из GitHub Environment `pypi`, `arq-rs-worker` из `pypi-worker`: PyPI отдаёт одну связку репозиторий/workflow/среда одному проекту. `workflow_dispatch` собирает wheel и запускает smoke. Этот запуск публикацию не включает. Обязательные ревьюеры сред `pypi` и `pypi-worker` задаются в настройках репозитория: workflow их не создаёт. Создание тегов `v*` остаётся ограниченным мейнтейнерами. Smoke ставит wheel архитектуры раннера: job скачивает `wheels-*-x86_64` и задаёт `SMOKE_ARCH=x86_64`. `scripts/smoke-venv.sh` берёт платформенный тег как последний компонент имени по `-`. Совпадение архитектуры — суффикс этого тега, поэтому `x86_64` читается целиком в конце тега. Скрипт завершается, если для этой архитектуры и тега CPython остаётся больше одного wheel.

## Границы

Библиотека ставит задачи, откладывает их, дедуплицирует активную задачу, продлевает lease, повторяет выдачу, выполняет прикладной retry, ведёт DLQ и даёт отдельные интерфейсы чтения и управления.

Библиотека не содержит scheduler, workflow engine, DI-контейнер, бизнес-блокировки и координацию внешних транзакций.

## Документы

| Документ | Содержание |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Процессы, пять крейтов, границы Rust и Python, manifest переноса |
| [docs/api.md](docs/api.md) | `App`, `Client`, middleware, admin, control, конфигурация |
| [docs/redis-keys.md](docs/redis-keys.md) | Ключи, поля задачи, receipt, retention |
| [docs/operations.md](docs/operations.md) | Транзакции, исходы, retry, DLQ, cancel/kill, shutdown |
| [docs/observability.md](docs/observability.md) | Метрики, логи, трассировка |
| [docs/testing.md](docs/testing.md) | Обязательные интеграционные тесты |
| [docs/migration.md](docs/migration.md) | Отличия от закреплённой версии simple-q |
| [docs/adr/](docs/adr/) | Принятые развилки контракта |

Примеры в документации синтетические: обработчик `process_item`, очередь `default`, аргумент `item_id`.
