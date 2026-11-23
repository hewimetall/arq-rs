# Архитектура

Статус: нормативный контракт от 2026-11-23. Код очереди в arq-rs ещё не написан.

Источник execution runtime: `hewimetall/simple-redis-q` `483e9fc063103708bdf37bfe80ddbc91af2a2fd5`.

## Назначение

arq-rs выполняет Python async-обработчики через Redis. Rust отвечает за транспорт, владение доставкой и процессы. Python отвечает за handler, middleware и lifecycle.

```text
Producer
    ↓
Python Client
    ↓
Rust Redis-backend
    ↓
Python child / asyncio
    ↓
Handler

Rust supervisor
    ├── запуск и замена child по slot
    ├── контроль владения доставкой
    ├── контроль asyncio loop
    └── остановка child

QueueAdmin   → чтение одной очереди
QueueControl → cancel / kill / requeue
```

Библиотека предоставляет постановку, отложенное выполнение, дедупликацию активной задачи, lease и renew, повторную выдачу, прикладной retry, DLQ, чтение состояния и управляющие команды.

Библиотека не предоставляет scheduler, workflow engine, DI-контейнер, бизнес-блокировки и координацию внешних транзакций. Дедупликация постановки не делает внешние эффекты идемпотентными.

## Решения

- Поддерживаемая версия сервера — Redis 8.6.2, включая Redis Cluster. Протокол использует `TIME`, строки, hash, sorted set и транзакции `WATCH / MULTI / EXEC`. `WATCH` покрывает ключи одной задачи. Общие ZSET индексов в него не входят. Lua и Redis Streams в протокол не входят.
- После принятия Redis задача выполняется хотя бы один раз. Ровно один запуск не обещается.
- Linux-supervisor без `pidfd_open` завершается кодом 3 и слоты не запускает. Child следит за pidfd родителя после захвата дескриптора и подъёма runtime, до импорта Python. Выход родителя, включая `SIGKILL`, запускает тот же drain, что и `SIGUSR1`. Зависший, но живой родитель этим сторожем не виден. Drain, который не вернулся, обрывается по `ARQ_SHUTDOWN_TIMEOUT_MS`.
- Все ключи одной очереди стоят в одном hash slot. Межочередные атомарные операции не предоставляются.
- На одном child одновременно выполняется одна задача.
- Supervisor не инициализирует CPython.
- Rust — единственный владелец транспортных переходов. Python возвращает предложение исхода.
- Отдельный recovery-sweeper не запускается. Чтение состояния данные не исправляет.
- Signal handler процесса, который перезапускает child, устанавливается до инициализации Python в этом процессе. Родительский supervisor CPython не инициализирует и вооружает `SIGTERM` и `SIGINT` до запуска слотов. Child инициализирует интерпретатор с `install_signal_handlers = 0`.

Подробности транспорта — в [operations.md](operations.md), ключей — в [redis-keys.md](redis-keys.md).

## Поток одной доставки

1. Producer вызывает `Client.send` или `send_with_options`. `on_send` выполняется в процессе отправителя до Redis.
2. `enqueue` атомарно создаёт задачу, захватывает uniq и добавляет её в ready.
3. Idle child делает `claim`: один новый `delivery_token`, `deliveries = deliveries + 1`, запись в inflight.
4. Child связывает `current_message()`, входит в цепочку `around_dispatch` и вызывает handler.
5. Python возвращает предложение. Rust выполняет `renew` на протяжении работы и `finish` при действующем владении.
6. `done` и `dead` снимают uniq, если значение ключа всё ещё равно `task_id`. `retry` оставляет uniq и возвращает задачу в ready без новой доставки.

Потеря lease останавливает старый child. Новая выдача появляется следующим `claim` после истечения lease либо после подтверждённого освобождения. Сетевая ошибка сама lease не снимает.

## Manifest переноса

SHA зафиксирован: `483e9fc063103708bdf37bfe80ddbc91af2a2fd5`.

Копирование файла без вызова из указанного пути не закрывает строку. Компонент со статусом «вне production-пути» требует отдельной интеграции в arq-rs и нового интеграционного теста. До этого он не считается перенесённым runtime.

| Компонент | Символ донора | Кто вызывает у донора | Куда входит в arq-rs | Тесты донора | Новый тест | Статус у донора |
|---|---|---|---|---|---|---|
| Запуск слота | `agent-worker` `process_port::spawn_worker` | `supervisor::run` | цикл слотов supervisor | `supervisor_tests`, `process_compare` | замена child по slot | production |
| watchexec-supervisor 6.0.0 | `watchexec_supervisor::job::start_job` | `spawn_worker` | тот же spawn | `crates/supervisor/tests/process_compare.rs` | SIGUSR1, группа процессов, exit status | production spawn; crate `simple-q-process` сам по себе job не стартует |
| Метрики-процесс | `supervisor::spawn_metrics` | `supervisor::run` | отдельный `tokio::process`, не слот watchexec | тесты supervisor roster | scrape после смерти child | production, отдельный механизм |
| Родитель без CPython | `main` ветка supervisor, `arm_stop_signals` | `main` до `supervisor::run` | тот же порядок | `supervisor_tests` | сигнал до `Py_Initialize` | production |
| Child interpreter | `runtime::initialize`, `PyConfig.install_signal_handlers = 0` | `child::run_child` | child | тесты child | venv и отсутствие обработчиков CPython | production |
| Загрузка App | `bridge::start` | `child::run_child` | `--app module:app` и `--app module:create_app --factory` | тесты запуска worker | factory выполняется внутри child | production |
| Dispatch и middleware | `simple_q.dispatch.App` | `bridge::execute` → `dispatch` | Python-пакет `arq_rs` | `python/tests/test_middleware_stack.py` | порядок hooks в реальном child | production |
| Контекст сообщения | `message.bind_message` / `reset_message` | `App.dispatch` | тот же `try/finally` | `test_middleware_stack` | очистка после ошибки и soft cancel | production |
| Outcome parser | `simple_q_python_bridge::parse_outcome`, `finish_for` | `worker` после `dispatch` | finish path child | тесты `python-bridge` | неизвестный outcome и исключение dispatch | production; семантику неизвестного outcome задаёт ADR 0003, а не ветка missing-key донора |
| Loop ping | `bridge::ping`, `ping::PingWatch::observe` | `child::lifecycle` | единственный ping path | `ping_tests` | зависший loop и обычный `asyncio.sleep` | production |
| Graceful drain | `request_graceful_drain`, pidfd родителя, `SIGUSR1` | `child::lifecycle` | shutdown policy | `parent_exit_queue` | shutdown с активной доставкой | production |
| Память и recycle | `plan_memory_signals`, `slot_busy`, `max_tasks_per_process` | цикл supervisor и child | те же пороги | `supervisor_tests` | soft signal не бьёт busy child; exit 0 заменяет слот | production |
| Интроспекция | `simple_q_runtime::admin` записи child и supervisor | heartbeat supervisor | `QueueAdmin.snapshot` | `admin` tests | gauges после смерти child | production как источник полей; ключи заменяются схемой arq-rs |
| Attempt lifecycle | `simple_q_orchestration::AttemptLifecycle` | `worker::held_finish` собирает значение в момент finish | живой объект одной доставки в child | unit-тесты `attempt.rs` | потеря владения во время handler | вне живого цикла: донор конструирует его локально при finish |
| Drain gate | `AdmissionGate` | вызовов из `agent-worker` нет | вход claim | unit-тесты `drain.rs` | drain не делает claim | вне production-пути |
| Slot lifecycle | `simple_q_process::SlotLifecycle` | вызовов из `supervisor.rs` нет | владелец переходов слота | unit-тесты `lifecycle.rs` | supervisor проводит Ready/Drain/Reaped | вне production-пути |
| Queue port | `simple_q_backend::QueuePort` | реализации в `agent-worker` нет | Redis backend этого контракта | типы порта | тесты `WATCH`-операций | порт без production-адаптера |
| Старый транспорт | `simple_q_runtime::redis_api`, Apalis-ключи | worker донора | не переносится | `enqueue.rs` | новый протокол вместо этих тестов | production донора, исключён |
| Sweeper донора | `cleanup::sweep_queue` | интервал в `child::run_child` | не переносится; retention делает maintenance batch | `cleanup_tests` | retention terminal-записей | production донора, исключён как recovery |
| Business lock | `lock`, `acquire_lock` | dispatch и ping донора | не переносится | lock-тесты | `actor(lock_key=...)` даёт `TypeError` | production донора, исключён |

Python-модули, которые стоят на пути child и переносятся с адаптацией имён пакета: `dispatch.py`, `interfaces.py`, `message.py`, `backoff.py`, `metrics.py`, `tracing.py`, `tracking.py`, `exposition.py`, `multiproc.py`. `memory.py` остаётся только внутренней тестовой двойной и не экспортируется. `admin.py` переписывается под ключи и методы этого контракта; поля snapshot, которые донор уже показывает, сохраняются.

Связанные тесты переносятся вместе с модулем. Тест, завязанный на ключи Apalis, TTL uniq или business lock, заменяется тестом из [testing.md](testing.md), а не копируется как зелёный.

## Коды выхода child

Контракт сохраняет коды донора, потому что supervisor уже ветвится по ним.

| Код | Имя | Реакция supervisor |
|---|---|---|
| 0 | штатный recycle или завершённый drain | немедленная замена слота, backoff сбрасывается к 1 с |
| 2 | исчерпан ping budget | restart с backoff; inflight остаётся для выдачи по lease |
| 3 | ошибка конфигурации | supervisor останавливается, слот не перезапускается |
| 4 | владение не подтверждено | restart с backoff; finish этого исполнения не повторяется вслепую |
| 5 | Redis-ошибка, после которой child не может продолжать безопасный цикл | restart с backoff |

Ошибка соединения внутри лимита повторов `WATCH` кодом 5 не является. Код 4 применяется, когда renew нельзя подтвердить к локальному дедлайну или владение уже потеряно. Код 2 не вызывает Python `on_stop` и не делает finish.

Backoff перезапуска: первая пауза 1 с, далее удвоение до 30 с. Пауза не прерывает уже принятую остановку supervisor. Кратковременная недоступность Redis крутит этот backoff, а не плотный цикл respawn.

## Asyncio loop ping

Единственный путь:

```text
child::lifecycle
    → bridge::ping
        → call_soon_threadsafe на работающий asyncio loop
        → callback выставляет флаг
    → PingWatch::observe
```

Ping измеряет отзывчивость loop. Обычное async-ожидание handler, включая `asyncio.sleep`, ping проходит. Блокировка потока loop копит бюджет. Бюджет по умолчанию — лимит времени задачи плюс 60 с, если `ARQ_PING_BUDGET_SECS` не задан явно. Один пропущенный ping renew не останавливает. Истечение бюджета останавливает renew, запрашивает отмену текущей доставки и завершает child кодом 2.

Второй watchdog, другой ABI и параллельная политика ping не вводятся. Перенос сохраняет работу с GIL, исключением из callback, жизненным циклом loop и взаимным исключением с shutdown и renew.

## Graceful shutdown

1. Supervisor перестаёт запускать новые claim на этом child и посылает drain.
2. Активная доставка продолжает renew.
3. Runtime ждёт завершения `dispatch` в пределах `ARQ_SHUTDOWN_TIMEOUT_MS`.
4. Предложение фиксируется обычным `finish`, если владение действует.
5. Выполняется `on_stop`.
6. Child останавливается с кодом 0.

Начало drain ACK не отправляет и uniq не освобождает. По истечении shutdown timeout child останавливается принудительно. Незавершённая доставка остаётся предметом lease recovery, если её не остановила команда `QueueControl`.

Порядок относительно экспортера трассировок совпадает с донором: штатный путь сбрасывает экспорт до `on_stop`. Код 2 экспорт не сбрасывает.

## Процессы и реестры

Реестр supervisor и дочерних процессов хранится отдельно от ключей очереди и может занимать другой slot. Его запись не входит в транзакцию claim или finish. Снимок admin поэтому не является глобально атомарным.

Heartbeat реестра нужен для отображения свежести. Он не продлевает lease, не снимает uniq, не создаёт доставку и не доказывает, что процесс жив или завершился. Отсутствие heartbeat остановку child не подтверждает и задачу в ready не возвращает.

`SIGTERM` или `SIGINT` до запуска слотов удаляет hash supervisor и его член в `arq:supervisors`, затем процесс выходит. Если процесс убит раньше этой очистки, запись исчезает по TTL в два окна heartbeat. Задачи очереди этот путь не меняет.
