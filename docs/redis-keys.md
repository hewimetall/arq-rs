# Ключи Redis

Статус: нормативный контракт от 2026-11-23. Имена ключей очереди собирает `QueueKeys` в `crates/arq-core/src/keys.rs`. `RedisBackend` пишет `q:{queue}:ready`, `inflight`, `done`, `dead`, `dead:tag`, `job`, `uniq`, `receipt`, `quarantine`, `qreason` и `halted`. Реестр `arq:supervisors`, hash `arq:supervisor:{hostname}.{pid}` и hash `arq:supervisor:{hostname}.{pid}:child:{slot}` пишет `arq-supervisor` через `RedisBackend`. Расхождение с документом оформляется ADR.

## Hash slot

Имя очереди `default` даёт ключи вида `q:{default}:ready`. Первая пара фигурных скобок — hash tag Redis Cluster, внутри неё только имя очереди. Все ключи одной очереди из таблицы ниже используют тот же tag и потому один slot.

`unique_key` и `tag` дописываются после закрывающей скобки tag. В них запрещены управляющие символы, пробелы, `{` и `}`. Ограничение держит ключ однозначным для протокола Redis.

Реестр supervisor в tag очереди не входит. Его обновление не атомарно с задачей.

Посторонний клиент эти ключи не пишет. Нарушение этого условия выводит задачу из гарантии согласованности.

## Ключи очереди

| Ключ | Тип | Содержимое |
|---|---|---|
| `q:{queue}:ready` | ZSET | член — `task_id`, score — время доступности, Unix ms |
| `q:{queue}:inflight` | ZSET | член — `task_id`, score — срок lease, Unix ms |
| `q:{queue}:job:{task_id}` | HASH | сообщение, состояние, текущая доставка, команда, история |
| `q:{queue}:uniq:{unique_key}` | STRING | `task_id` владельца |
| `q:{queue}:done` | ZSET | член — `task_id`, score — `finished_at_ms`; нужен, чтобы retention находил `done` без обхода всех hash |
| `q:{queue}:dead` | ZSET | член — `task_id`, score — время перехода в DLQ, Unix ms |
| `q:{queue}:dead:tag:{tag}` | ZSET | тот же член и score для задач с tag |
| `q:{queue}:receipt:{operation_id}` | STRING | канонический JSON запроса и результата |
| `q:{queue}:quarantine` | ZSET | член, который нельзя выдать: нет hash, hash не того типа или `task_id` не разбирается. Score — Unix ms переноса |
| `q:{queue}:qreason` | HASH | поле — член карантина, значение — `missing_hash`, `hash_type` или `task_id` |
| `q:{queue}:halted` | STRING | причина системного halt. Ключ отсутствует, пока очередь не остановлена |

Отдельной записи delivery нет. Текущая доставка лежит в hash задачи.

Пример для очереди `default`, задачи с id `3f1c0b2a-7e4d-4c1a-9b2e-6d5a4c3b2a10` и ключа `item:1001`:

```text
q:{default}:ready
q:{default}:job:3f1c0b2a-7e4d-4c1a-9b2e-6d5a4c3b2a10
q:{default}:uniq:item:1001
```

## Время

Абсолютные метки и scores — Unix time в миллисекундах. Источник времени проверки — Redis `TIME`, пересчитанный в миллисекунды: `seconds * 1000 + microseconds / 1000`. Это значение фиксируется в начале цикла чтения и им считается срок, который пишет транзакция. Redis сам срок lease внутри `EXEC` не перепроверяет. После подтверждённого claim child читает `TIME` ещё раз и не вызывает handler, если lease уже истёк. Локальные часы машины для lease, delay и глубины `scheduled` не используются.

Локальные сроки безопасности, включая `ARQ_RENEW_CONFIRM_MS` и стадии ping, измеряются монотонными часами процесса.

## Hash задачи

Версия схемы поля `version` равна `1`.

| Поле | Смысл |
|---|---|
| `version` | `1` |
| `task_id`, `message_id` | одинаковый UUID |
| `cycle_id` | UUID текущего цикла |
| `actor_name`, `queue` | адресация handler |
| `kwargs` | JSON-объект |
| `headers` | JSON-объект строковых пар |
| `tag` | строка или отсутствие поля |
| `unique_key` | строка или отсутствие поля |
| `state` | `ready`, `inflight`, `done`, `dead` |
| `created_at_ms`, `available_at_ms` | создание и доступность |
| `started_at_ms` | начало текущей доставки; renew его не меняет |
| `finished_at_ms` | терминальное время текущего цикла |
| `delivery_token`, `worker_id`, `lease_deadline_ms` | текущая доставка; пусты вне inflight |
| `deliveries`, `retries` | счётчики текущего цикла |
| `max_deliveries`, `max_retries` | пределы текущего цикла |
| `time_limit_ms`, `min_backoff_ms`, `max_backoff_ms` | копия политики на момент enqueue или requeue |
| `result` | `ok`, `skipped` или пусто |
| `reason`, `error` | терминальная причина и усечённый текст |
| `stop_evidence` | пусто, `observed_exit` или `lease_expired` |
| `control_op` | пусто, `cancel` или `kill`; пусто после терминального перехода |
| `control_operation_id`, `control_cycle_id`, `control_delivery_token`, `control_accepted_at_ms` | принятая команда |
| `history` | JSON-массив терминальных записей прошлых циклов |

`error` обрезается до 4096 байт UTF-8 по границе кодовой точки. История одного цикла:

```json
{
  "cycle_id": "6b1f1c2a-1111-4222-8333-444455556666",
  "state": "dead",
  "reason": "retries",
  "error": "RuntimeError: failed",
  "retries": 2,
  "deliveries": 3,
  "finished_at_ms": 1763942400000,
  "control_op": "",
  "stop_evidence": ""
}
```

Активная задача, её uniq и незакрытая команда TTL не имеют. Возраст uniq удалением не является.

## Receipt

Значение — JSON без пробелов, с ключами объектов в лексикографическом порядке:

```json
{
  "cycle_id": "6b1f1c2a-1111-4222-8333-444455556666",
  "op": "enqueue",
  "operation_id": "11111111-2222-4333-8444-555555555555",
  "request": {},
  "result": {"status": "enqueued", "task_id": "3f1c0b2a-7e4d-4c1a-9b2e-6d5a4c3b2a10"},
  "task_id": "3f1c0b2a-7e4d-4c1a-9b2e-6d5a4c3b2a10"
}
```

`request` содержит нормализованные параметры операции: для enqueue это канонический JSON kwargs, headers, tag, unique key, delay, actor, queue и message id. Числа `1` и `1.0` различны. Сравнение повтора идёт по этому каноническому тексту и по полю `op`.

Receipt пишется той же транзакцией, что и изменение. TTL ключа — `ARQ_RECEIPT_TTL_SECS` (8 суток). Этот срок не короче retention DLQ.

Token доставки и новый `cycle_id` requeue выбираются до отправки `EXEC` и при повторе того же `operation_id` берутся из уже созданного receipt, а не генерируются заново.

## Реестр процессов

| Ключ | Тип | Содержимое |
|---|---|---|
| `arq:supervisors` | ZSET | член `{hostname}.{pid}`, score — `last_seen_ms` |
| `arq:supervisor:{hostname}.{pid}` | HASH | статические поля процесса и счётчики |
| `arq:supervisor:{hostname}.{pid}:child:{slot}` | HASH | slot, PID, состояние, RSS, причина, код выхода, счётчики задач |

`hostname` — результат того же источника, что `ids::hostname()` донора: 1–253 символа из `[A-Za-z0-9._-]`. Поля hash включают host, pid, started_at, version, queues, processes, concurrency, max_tasks, пороги памяти, cgroup, recycled, drains, kills, restarts, last_seen. Child-hash хранит ppid, slot, state, reason, exit, rss, started_at, ended_at, tasks_done, max_tasks, alive window.

TTL записи supervisor — два окна heartbeat. Активный supervisor продлевает его записью heartbeat. Удаление по TTL реестр чинит только как исчезновение записи из admin; задачи очереди эта запись не удаляет.

Orphan — child-запись, у которой родительский supervisor уже отсутствует в `arq:supervisors`. Admin показывает её в `orphans` и не удаляет.

## Retention

Удаление terminal-записей выполняет maintenance path worker, ограниченный `ARQ_MAINTENANCE_BATCH`, с периодом `ARQ_MAINTENANCE_INTERVAL_MS`. Отдельный процесс для этого не запускается. Цикл идёт и при пустом ready.

Один batch читает `q:{queue}:done` и `q:{queue}:dead` по score старше соответствующего TTL и удаляет вместе:

- член `done` либо член `dead`;
- член `q:{queue}:dead:tag:{tag}`, если tag был;
- hash задачи вместе с `history`;
- uniq этот batch не удаляет повторно: uniq снимается в терминальной транзакции. Если uniq всё ещё указывает на удаляемую задачу, batch останавливается с `StorageInconsistency` и запись не трогает.

`done` живёт 24 часа, `dead` — 7 суток. Активные `ready` и `inflight`, их uniq и незакрытая команда в batch не попадают.

После истечения receipt повтор с тем же `operation_id` доказывается только полным совпадением постусловий. Если состояние задачи этого совпадения не даёт, операция возвращает `IndeterminateOperation` и ничего не пишет.

## Предел ready

`enqueue` новой задачи читает `ZCARD` всего ключа ready, включая члены с будущим score. Ключ при этом не ставится под `WATCH`. При наблюдаемом `ZCARD >= ARQ_MAX_READY` результат — `QueueFull`. Одновременные постановки могут ненадолго превысить предел; граница превышения задана в [operations.md](operations.md). Inflight и DLQ в это число не входят. Повтор операции, у которой receipt уже содержит успех или `duplicate`, лимит заново не применяет.
