# ADR 0007. Идентификаторы и сокращение API simple-q

## Статус

Принято (2026-11-23).

## Контекст

Публичный API simple-q уже использует `Actor`, `actor`, `App`, `Client`, `send` и `send_with_options`. Admin донора показывает и `task_id`, и `message_id`. Новый транспорт добавляет `operation_id`, receipt и цикл requeue. Конфигурация донора содержит TTL uniq, sweeper и ожидание освобождения uniq, а actor — business lock. Новый контракт это исключает.

## Решение

В первой версии `task_id` равен `message_id`. Это одно значение, записанное в оба поля DTO. `operation_id` идентифицирует одну изменяющую операцию. `cycle_id` меняется при enqueue и при успешном requeue. `delivery_token` идентифицирует одну доставку.

`send(actor, **kwargs)` принимает только аргументы handler. Транспортные параметры передаются в `send_with_options`. Занятый uniq: `send` завершается ошибкой `DuplicateTask`, `send_with_options` возвращает `None`. Ошибка Redis в обоих методах остаётся ошибкой.

`actor` не принимает `lock_key`, `lock_ttl` и `skip_on_lock`. `Client` не предоставляет `acquire_lock`, `release_lock` и `worker_alive`. `MemoryClient` не входит в публичный API. Переменные `SIMPLE_Q_*` при старте отклоняются. Настройки нового backend читаются из `ARQ_*`.

Декорированные методы контракт не описывает: для них нет тестов донора, которые переносятся как гарантия.

## Последствия

Таблица соответствия и список удалённых настроек находятся в `docs/migration.md`. Изменение равенства `task_id` и `message_id` требует нового ADR.
