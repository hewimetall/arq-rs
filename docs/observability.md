# Наблюдаемость

Статус: нормативный контракт от 2026-11-23. Имена `dramatiq_*` записывает `python/arq_rs/metrics.py`, каталог multiprocess задаёт `python/arq_rs/multiproc.py`. `arq_storage_inconsistent` и `arq_quarantine_total` записывает тот же модуль, когда child видит halt или карантин. Остальные имена `arq_*` остаются контрактом и крейтами Rust не выпускаются. Расхождение с документом оформляется ADR.

Профиль simple-q и Dramatiq сохраняется: те же имена, те же labels `queue_name` и `actor_name`, тот же смысл счётчиков. `queue_name` канонический, без суффиксов `.DQ` и `.XQ`. Отдельный тест сверяет этот список и корзины histogram.

| Имя | Тип | Когда меняется |
|---|---|---|
| `dramatiq_messages_total` | counter | завершённая попытка handler path |
| `dramatiq_message_errors_total` | counter | попытка завершилась исключением handler или timeout |
| `dramatiq_message_retries_total` | counter | успешно зафиксированный прикладной retry |
| `dramatiq_message_rejects_total` | counter | терминальный переход в DLQ |
| `dramatiq_messages_inprogress` | gauge, `livesum` | плюс один на входе в попытку, минус один на выходе |
| `dramatiq_delayed_messages_inprogress` | gauge, `livesum` | остаётся 0: отложенная задача ждёт в Redis |
| `dramatiq_message_duration_milliseconds` | histogram | время попытки |

Корзины histogram, последняя конечная граница `900000`, далее `+Inf`:

```text
5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000,
7500, 10000, 30000, 60000, 600000, 900000, +Inf
```

Те же корзины используют гистограммы задержек ниже.

После успешного `finish` child вызывает `record_outcome`. Retry увеличивает `dramatiq_message_retries_total`. Dead и `invalid_outcome` увеличивают `dramatiq_message_rejects_total`. Ok и skipped эти счётчики не меняют. Claim, который сам записал `max_deliveries` или `invalid_message`, делает то же и вызывает `note_dead`. Ошибка записи метрики commit не откатывает. `finish_attempt` в Python эти счётчики не увеличивает: повторный вызов до commit посчитал бы незафиксированный retry.

Дополнительные метрики. Labels — только перечисленные, без `task_id`, `message_id`, `operation_id`, `delivery_token` и tag.

| Имя | Labels | Смысл |
|---|---|---|
| `arq_queue_depth` | `queue_name`, `state` | `ready`, `scheduled`, `inflight`, `done`, `dead`; `ready` и `scheduled` не пересекаются |
| `arq_enqueue_duplicates_total` | `queue_name` | ответ `duplicate` |
| `arq_redis_errors_total` | `queue_name`, `op` | ошибка команды или соединения |
| `arq_redis_latency_milliseconds` | `queue_name`, `op` | длительность цикла операции |
| `arq_renew_failures_total` | `queue_name` | renew не подтверждён |
| `arq_ownership_loss_total` | `queue_name` | child остановлен из-за потери владения |
| `arq_ping_latency_milliseconds` | `queue_name` | длительность успешного и неуспешного ping |
| `arq_ping_budget_exceeded_total` | `queue_name` | код выхода 2 |
| `arq_child_rss_bytes` | `queue_name`, `slot` | последний RSS живого child |
| `arq_child_recycle_total` | `queue_name` | exit 0 |
| `arq_child_restart_total` | `queue_name` | перезапуск после ненулевого кода, кроме конфигурации |
| `arq_child_kill_total` | `queue_name` | `SIGKILL`, который послал этот supervisor |
| `arq_control_commands_total` | `queue_name`, `op`, `status` | `cancel`, `kill`, `requeue`, `settle` и статус ответа |
| `arq_operations_indeterminate_total` | `queue_name`, `op` | `IndeterminateOperation` |
| `arq_storage_inconsistent` | `queue_name` | gauge 1 после того, как этот child увидел halted очередь |
| `arq_quarantine_total` | `queue_name`, `reason` | counter переносов в `q:{queue}:quarantine`; `reason` — `missing_hash`, `hash_type`, `task_id` |

`arq_queue_depth{state="ready"}` считает члены `q:{queue}:ready` со score не выше Redis `TIME` на момент снятия. `state="scheduled"` считает остальные члены того же ZSET. Ряды не пересекаются и в сумме дают `ZCARD`. `ARQ_MAX_READY` сравнивается с этой суммой, а не с одним рядом `ready`. Задача переходит из `scheduled` в `ready` с ходом времени, без записи в Redis. `inflight`, `done` и `dead` читаются из своих ключей и в эту сумму не входят.

`slot` в RSS — номер слота supervisor, не идентификатор задачи. После смерти child gauge RSS этого слота снимается, а `dramatiq_messages_inprogress` уменьшается снятием multiprocess-файла слота. Завышенный gauge после смерти child тестом запрещён.

`arq_storage_inconsistent` и `arq_quarantine_total` пишутся в процессный реестр `python/arq_rs/metrics.py` из child. Rust по-прежнему не держит экспортёр Prometheus. Общий multiprocess-экспорт остальных рядов `arq_*` не собран. Имена и labels выше не меняются. `task_id` в label карантина не входит.

Ожидание в очереди считается от `available_at_ms` до успешного claim и попадает в histogram `arq_queue_wait_milliseconds` с label `queue_name`.

## Логи

Обычные логи содержат `message_id`, `actor_name`, `queue` и имя операции. `delivery_token` в них не пишется. Текст ошибки задачи пишется на уровне предупреждения уже усечённым до сохранённого предела.

`StorageInconsistency` и `IndeterminateOperation` логируются с `operation_id` и `message_id`.

## Трассировка

OpenTelemetry выключен, пока `App` создан без `Tracing`. Включение явное.

Входящие headers `traceparent` и `tracestate` продолжают контекст. Header `tracelink`, если это W3C traceparent, становится Span Link. Неразборчивый `traceparent` родителем не становится. Сохранённое сообщение оставляет headers отправителя и на retry. `on_send` — место, где отправитель записывает контекст до Redis.

Имена spans постоянны: `enqueue`, `execute`, `finish`, `supervisor.start`, `supervisor.stop`, `child.start`, `child.exit`, `child.restart`, `drain`. Имя обработчика, `message_id`, `task_id` и `cycle_id` лежат в attributes. `delivery_token` в attributes не кладётся.

Span supervisor не связывается с span задачи: supervisor handler не выполняет. Ресурс содержит `service.name`, `service.version`, `host.name` и `process.pid`.

Очередь экспорта ограничена так же, как у донора: 2048 spans, batch 512. Переполнение при удержанном GIL span отбрасывает. Код выхода 2 буфер не сбрасывает. Штатный shutdown сбрасывает экспорт до `on_stop`.
