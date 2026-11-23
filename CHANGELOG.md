# Changelog

## Unreleased

Зафиксирован нормативный контракт Redis-очереди и execution runtime. Реализация очереди в этот выпуск не входит.

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
