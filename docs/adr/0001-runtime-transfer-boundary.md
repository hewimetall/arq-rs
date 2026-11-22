# ADR 0001. Граница переноса runtime

## Статус

Принято (2026-11-23).

## Контекст

PR №42 `hewimetall/simple-redis-q` на SHA `483e9fc063103708bdf37bfe80ddbc91af2a2fd5` содержит и работающий parent/child path, и библиотеки, которые supervisor loop не вызывает. Описание PR прямо отделяет slot state machine, `QueuePort` и drain gate от production adapter. Metrics helper запускается отдельным `tokio::process`.

## Решение

Перенос копирует только компоненты с указанным вызывающим путём. Для каждого компонента manifest в `docs/architecture.md` называет исходный символ, вызывающий путь донора, точку подключения arq-rs и тесты.

`SlotLifecycle`, `AdmissionGate` и `QueuePort` подключаются в arq-rs отдельной интеграцией. До неё они не описываются как готовый runtime. Старый Redis-адаптер донора заменяется протоколом этого контракта и не переносится как транспорт.

## Последствия

Реализация, которая копирует crate без вызова из supervisor или child, не закрывает соответствующий пункт manifest. Интеграционный тест из колонки manifest обязателен.
