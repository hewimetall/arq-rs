# Отличия от simple-q

Статус: нормативный контракт от 2026-11-23.

Ориентир — `hewimetall/simple-redis-q` `483e9fc063103708bdf37bfe80ddbc91af2a2fd5`. Ниже только осознанная разница поверхностей. Поведение, которое контракт называет сохранённым, реализации нужно брать из исходных файлов этого SHA, а не из пересказа.

## Что сохраняется

- `Actor`, `actor`, `App`, протоколы `Client` и `WorkerApp`, метод `Middleware.install`.
- Загрузка `--app module:app` и `--app module:create_app --factory`.
- Разделение `send(actor, **kwargs)` и `send_with_options`.
- Порядок hooks и привязка `current_message()` до внешнего `around_dispatch`.
- `on_dead` до возврата из `dispatch`. Ошибка hook логируется и решение dispatch не подменяет.
- `DeadLetter` с прежними полями.
- Формула `compute_backoff` и значения по умолчанию `time_limit`, `min_backoff`, `max_backoff`, `max_retries`, `max_deliveries`.
- Попытка на границе `max_deliveries` ещё выполняется. Следующая доставка не создаётся.
- Имена метрик Dramatiq, labels и корзины histogram, включая последнюю конечную границу 900000 мс.
- Коды выхода child 0, 2, 3, 4 и 5 и backoff перезапуска 1…30 с.
- Один ping path: `call_soon_threadsafe`, затем `PingWatch`.
- Родительский процесс без CPython и `watchexec-supervisor` 6.0.0 для слотов worker. Metrics helper остаётся отдельным `tokio::process`.

## Идентификаторы

| simple-q | arq-rs |
|---|---|
| `message_id` в Python, отдельный `task_id` транспорта | в первой версии это одно значение; admin по-прежнему показывает оба поля |
| нет `operation_id` | UUID одной изменяющей операции, живёт в receipt |
| нет `cycle_id` | новый UUID на enqueue и на каждый успешный requeue |
| lease Apalis в meta hash | `delivery_token` и `lease_deadline_ms` в hash задачи |

Повтор логической отправки использует прежние `message_id` и `operation_id`. Клиент генерирует их до сети, если вызывающий код их не передал.

## Отправка

`send_with_options` дополнительно принимает `tag`, `message_id` и `operation_id`. `delay` остаётся миллисекундами. Занятый uniq по-прежнему даёт `None` из `send_with_options` и ошибку из `send`. Ошибка Redis по-прежнему не маскируется под дубликат. Имя исключения дубликата на `send` — `DuplicateTask`.

Абсолютное время доступности в API не передаётся: есть только неотрицательный `delay`.

## Исходы

Донор в `simple_q_python_bridge` считает отсутствующий `outcome` успехом, а документация донора считает успехом и любое другое значение. Код worker при неизвестной строке пишет dead `protocol`.

arq-rs фиксирует одно правило. `ok`, `skipped`, `retry` и `dead` — единственные принимаемые предложения. Отсутствие ключа, неизвестная строка и неверная форма дают DLQ `invalid_outcome`. Исключение `dispatch` даёт `dispatch_error`. ACK из неизвестного словаря не выполняется.

`skipped` по-прежнему завершает задачу успешно для транспорта и записывает `result=skipped`.

## Что удаляется

| Поверхность simple-q | Поведение arq-rs |
|---|---|
| `lock_key`, `lock_ttl`, `skip_on_lock` | `TypeError` на `actor` |
| `acquire_lock`, `release_lock` | методов нет |
| `worker_alive` | метода нет; свежесть heartbeat видна в snapshot и процесс не доказывает |
| `MemoryClient` | в публичный пакет не входит |
| TTL uniq и `SIMPLE_Q_UNIQUE_TTL_SECS` | uniq без TTL, пока задача активна |
| sweeper и `SIMPLE_Q_SWEEP_*` | maintenance batch того же worker |
| `SIMPLE_Q_IDEMPOTENCY_WAIT_*` | исход доказывается receipt и постусловиями, а не ожиданием ключа |
| ключи Apalis `{queue}:active`, `{queue}:scheduled`, `{queue}:data` | схема из [redis-keys.md](redis-keys.md) |
| причина `poison` | причина `max_deliveries` на claim |

Любая переменная окружения `SIMPLE_Q_*` останавливает старт с ошибкой конфигурации. Читаются только `ARQ_*` из [api.md](api.md).

## Чтение и управление

`QueueAdmin` привязан к одной очереди в конструкторе. `peek` принимает `ready`, `scheduled` и `inflight`. Добавлены `get`, `dead_for_tag` и `command`. `scheduled` — срез ready с будущим score, не отдельное состояние Redis.

`QueueControl` — новый интерфейс. `cancel`, `kill`, `requeue` и `settle` в simple-q в этой форме отсутствуют. Их протокол задан в [operations.md](operations.md), а не выводится из старого admin. `settle` закрывает inflight с принятой командой после истечения lease, когда процесс нельзя отождествить.

Пагинация больше не обрезает `limit` молча до 200: значение вне `1…200` поднимает `ValueError`.

## Время и транзакции

Scores и поля времени — Unix-миллисекунды от Redis `TIME`. Донор в ряде ключей хранил секунды.

Транспорт донора на этом SHA не является `WATCH / MULTI / EXEC` протоколом arq-rs. Перенос runtime не включает `redis_api` и `QueuePort`. Очередь пишет `RedisBackend` по [operations.md](operations.md): `enqueue`, `claim`, `renew`, `finish`, `requeue`, `control`, maintenance batch и чтение для `QueueAdmin`.

Частичный `EXEC` останавливает изменения очереди в процессе. Донор такой остановки не описывает.

## Runtime, который ещё надо подключить

`SlotLifecycle` и `AdmissionGate` на SHA донора supervisor loop не вызывает. `AttemptLifecycle` у донора собирается локально в момент finish. В arq-rs это живое состояние одной доставки child. Наличие этих типов в дереве simple-q не означает, что arq-rs уже получил рабочий slot machine, drain gate или цикл доставки. Строки manifest в [architecture.md](architecture.md) закрываются только новым интеграционным тестом.
