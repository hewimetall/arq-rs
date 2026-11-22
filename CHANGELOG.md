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
