# Python API

Статус: нормативный контракт от 2026-11-23. Сигнатуры ниже — поверхность, которую должна реализовать библиотека. Сейчас в репозитории этих модулей нет.

Закреплённый ориентир: `simple_q.interfaces`, `simple_q.dispatch` и `docs/api.md` simple-q на SHA `483e9fc063103708bdf37bfe80ddbc91af2a2fd5`. Отличия собраны в [migration.md](migration.md).

Импорт пакета Redis не открывает. Соединение открывает `Client`.

## Декларация обработчика

```python
from arq_rs import App, Client, Config, actor


@actor(queue="default", max_retries=2)
async def process_item(item_id: str) -> None:
    ...


def create_app() -> App:
    client = Client(
        "redis://redis:6379/0",
        config=Config.from_env(),
    )
    return App(client=client, actors=[process_item])
```

```python
def actor(
    fn=None,
    *,
    name: str | None = None,
    queue: str = "default",
    max_retries: int = 0,
    time_limit: int = 1_800_000,
    min_backoff: int = 15_000,
    max_backoff: int = 604_800_000,
    max_deliveries: int = 5,
):
    ...
```

`actor()` возвращает неизменяемую декларацию `Actor`. Декларация не привязана к `App` и к соединению. `name` по умолчанию равен имени функции. Повтор имени в одном `App` поднимает `ValueError`.

Handler вызывается как `function(**kwargs)`. Очередь — непустая строка `[a-z0-9]+`.

Границы параметров:

| Параметр | Ограничение | Единица |
|---|---|---|
| `max_retries` | `>= 0` | число уже записанных retry перед следующим |
| `max_deliveries` | `>= 1` | число созданных доставок цикла |
| `time_limit` | `> 0` | миллисекунды, только вокруг вызова handler |
| `min_backoff`, `max_backoff` | `0 < min_backoff <= max_backoff` | миллисекунды |

Параметры `lock_key`, `lock_ttl` и `skip_on_lock` поднимают `TypeError`. Декорированный метод контракт не поддерживает: такая форма не объявляется и тестами не покрывается.

Политика retry записывается в сообщение при enqueue. Последующее изменение декларации уже поставленную задачу не меняет.

## Загрузка

Supervisor запускает child одним из способов:

```text
--app module:app
--app module:create_app --factory
```

Factory вызывается внутри child и возвращает `App`. Обязательный базовый класс приложения или middleware не вводится. `App` удовлетворяет протоколу `WorkerApp` и не наследует его. `Client` удовлетворяет протоколу клиента и не наследует его.

## Client

```python
class Client:
    def __init__(self, url: str, config: Config | None = None) -> None: ...

    def on_send(self, fn): ...

    async def send(self, actor: Actor, /, **kwargs) -> str: ...

    async def send_with_options(
        self,
        actor: Actor,
        *,
        kwargs: dict | None = None,
        delay: int | None = None,
        headers: dict[str, str] | None = None,
        unique_key: str | None = None,
        tag: str | None = None,
        message_id: str | None = None,
        operation_id: str | None = None,
    ) -> str | None: ...

    async def close(self) -> None: ...
```

`send` передаёт только аргументы handler. Служебные поля из `kwargs` не читаются. Возвращаемое значение — `message_id`. Занятый uniq поднимает `DuplicateTask`.

`send_with_options` разделяет аргументы handler и параметры транспорта. Занятый uniq возвращает `None` и задачу не создаёт. Ошибка Redis в обоих методах остаётся исключением и в `None` не превращается.

`delay` — неотрицательная задержка в миллисекундах. `None` и `0` означают доступность сразу. `kwargs` сериализуются в JSON: объекты, массивы, строки, числа и булевы значения. `headers` имеет тип `dict[str, str]`.

Пустой `unique_key` и пустой `tag` недопустимы. Оба значения — непрозрачные строки длиной от 1 до 256 байт UTF-8 без управляющих символов, пробелов, `{` и `}`. Очередь не разбирает их содержимое. Headers и контекст трассировки в uniq сами не попадают.

`message_id` и `operation_id`, если переданы, являются каноническим UUID в нижнем регистре. Пропущенные значения клиент создаёт до первой сетевой отправки и использует повторно, пока жив этот вызов. Повтор той же логической отправки после перезапуска процесса возможен, когда вызывающий код сохранил оба идентификатора и те же параметры.

При `IndeterminateOperation` исключение содержит `operation_id` и `message_id`. Повтор выполняется с теми же параметрами.

`on_send` синхронный, регистрируется на клиенте и выполняется в задаче вызывающего кода до Redis. Ошибка `on_send` отправку прерывает, receipt не пишется.

## Идентификаторы

| Поле | Роль |
|---|---|
| `message_id` | Идентификатор сообщения в Python API |
| `task_id` | Идентификатор hash задачи. В первой версии равен `message_id` |
| `operation_id` | Одна изменяющая операция enqueue, claim, renew, finish, requeue или control |
| `cycle_id` | Цикл от enqueue или requeue до терминального исхода |
| `delivery_token` | Одна доставка. В обычные логи не пишется |

Admin показывает `task_id` и `message_id` отдельными полями с одинаковым значением. Requeue сохраняет оба и меняет `cycle_id`. Идентификатор операции requeue является собственным `operation_id` и с receipt первоначального enqueue не совпадает: повторное использование того же `operation_id` с другим типом или другими параметрами даёт `OperationConflict`.

## App и WorkerApp

```python
class App:
    def __init__(
        self,
        *,
        client: Client,
        actors,
        on_start=None,
        on_stop=None,
        on_dead=None,
        around_dispatch=None,
        tracing: Tracing | None = None,
    ) -> None: ...

    client: Client
    tracing: Tracing | None

    async def startup(self) -> None: ...
    async def shutdown(self) -> None: ...
    async def dispatch(self, message: dict) -> dict: ...
    async def note_dead(
        self,
        actor_name: str,
        kwargs: dict,
        reason: str,
        message_id: str,
        retries: int,
        deliveries: int = 0,
        *,
        error: str | None = None,
    ) -> None: ...

    def on_start(self, fn): ...
    def on_stop(self, fn): ...
    def on_dead(self, fn): ...
    def around_dispatch(self, fn): ...
```

`dispatch` принимает и возвращает словарь. Это граница Python bridge.

```python
def current_message() -> CurrentMessage | None: ...
def worker_name() -> str | None: ...
```

`CurrentMessage` содержит `message_id`, `task_id`, `cycle_id`, `retries`, `max_retries`, `deliveries`, `max_deliveries`, `queue_name`, `actor_name`, `kwargs`, `headers`, `tag` и `worker_name`. Вне handler `current_message()` возвращает `None`, а `worker_name()` — пустое значение.

## Middleware

```python
class Middleware:
    def install(self, client: Client, app: WorkerApp) -> None:
        ...
```

Наследование от класса библиотеки не требуется. `install` синхронный и только регистрирует hooks.

```python
on_send(actor_name: str, kwargs: dict, headers: dict[str, str]) -> dict[str, str] | None
on_start() -> None | Awaitable[None]
on_stop() -> None | Awaitable[None]
on_dead(letter: DeadLetter) -> None | Awaitable[None]
around_dispatch(message: dict, call_next) -> dict | Awaitable[dict]
```

`on_send` возвращает новый mapping headers либо `None`, чтобы оставить mapping предыдущего hook. `call_next` вызывает остаток цепочки, включая handler и решение retry/dead, и возвращает словарь.

Порядок при регистрации A, затем B:

```text
on_send / on_start / on_stop / on_dead:
    A, затем B

around_dispatch:
    A enter
      B enter
        handler и решение retry/dead
      B exit
    A exit
```

`current_message()` привязывается до входа во внешний `around_dispatch` и снимается в `finally` после выхода из всей цепочки: при успехе, исключении и кооперативной отмене. Внешний hook может вернуть словарь, не вызывая `call_next`. Словарь внутреннего слоя внешний слой получает и может заменить. Внутренняя повторная выдача и транспортный retry `on_send` не вызывают.

Исключение одного `on_start`, `on_stop` или `on_dead` логируется. Следующие hooks того же списка выполняются. Исключение внутри `startup()` после этого списка наружу не выходит, поэтому ошибка `on_start` сама по себе claim не запрещает: так устроен `_run_hooks` донора. Если `startup()` как вызов bridge завершается ошибкой, child claim не начинает.

Ошибка `on_send` отправку прерывает до Redis.

`DeadLetter` содержит `actor_name`, `kwargs`, `reason`, `message_id`, `retries`, `deliveries` и `error`. `kwargs` — копия. Смысл `on_dead` и `note_dead` задан в [operations.md](operations.md).

При hard kill выполнение `finally`, `on_stop` и остальных Python hooks не обещается.

## Предложение исхода

Rust принимает от `dispatch` только следующие словари.

| `outcome` | Обязательные поля | Транспортный смысл |
|---|---|---|
| `ok` | нет | `done` |
| `skipped` | нет | `done` с результатом `skipped` |
| `retry` | `delay_ms` — целое `>= 0` | увеличить `retries`, вернуть в ready |
| `dead` | `reason` — непустая строка; `error` — строка или `null` | DLQ с этой причиной |

Другое содержимое при действующем владении фиксируется как DLQ `invalid_outcome`: нет ключа `outcome`, неизвестная строка, результат не словарь, лишние типы полей, отрицательный `delay_ms`. Исключение из `dispatch`, включая внешний middleware, фиксируется как `dispatch_error` и неявный retry не создаёт.

Причины, которые Python формирует сам на пути handler:

| Причина | Условие | `error` |
|---|---|---|
| `retries` | handler поднял `Exception`, и `retries >= max_retries` | `"{Type}: {message}"` |
| `timeout` | handler не уложился в `time_limit` на последней попытке | текст исключения |
| `unknown_actor` | имени нет в этом `App` | `null` |

Пока `retries < max_retries`, исключение handler и timeout handler возвращают `retry`. Задержка считается функцией донора `compute_backoff`: показатель степени ограничен 32, база равна `min(min_backoff * 2**retries, max_backoff)`, результат равен `int(half + uniform(0, half))`, где `half = base / 2`. `retries` в формуле — уже записанное число, до текущего увеличения.

Timeout ставится вокруг вызова handler внутри самого внутреннего слоя, после входа в `around_dispatch`. Цепочка middleware в этот таймер не входит. По таймеру handler получает отмену. Если кооперативная отмена не завершилась за `cancel_grace_ms`, child уничтожается, а доставка остаётся потерей владения: исход retry или dead для неё не синтезируется. Подавленная `CancelledError`, после которой `dispatch` всё же вернул допустимый словарь до конца grace, фиксируется как этот словарь.

Смерть child до передачи корректного результата — потеря доставки. Уже увеличенный `deliveries` сохраняется.

## QueueAdmin

Очередь задаётся при создании. Методы только читают.

```python
admin = QueueAdmin(client, queue="default")


class QueueAdmin:
    async def snapshot(self, host: str | None = None) -> Snapshot: ...
    async def get(self, task_id: str) -> TaskInfo | None: ...
    async def peek(
        self,
        state: str,
        limit: int = 50,
        cursor: str | None = None,
    ) -> TaskPage: ...
    async def dead(
        self,
        limit: int = 50,
        cursor: str | None = None,
    ) -> DeadPage: ...
    async def dead_for_tag(
        self,
        tag: str,
        limit: int = 50,
        cursor: str | None = None,
    ) -> DeadPage: ...
    async def command(self, operation_id: str) -> CommandReceipt | None: ...
```

`peek` принимает `ready`, `scheduled` и `inflight`. `scheduled` — это ready-задачи с временем доступности в будущем, отдельным состоянием не является.

`Snapshot` содержит `taken_at`, `queues`, `supervisors` и `orphans`. Счётчики очереди включают глубину `ready`, `scheduled`, `inflight`, `done` и `dead`. У supervisor есть `children`, у child есть `tasks`. Снимок сохраняет host, PID, slot, RSS, причину выхода и счётчики supervisor: recycled, drains, kills, restarts. `retries` и `deliveries` видны отдельно. Свежесть heartbeat не доказывает ни жизнь, ни выход процесса. Отсутствие записи в DLQ не доказывает, что исполнение ещё идёт.

Чтение не пишет heartbeat, не продлевает lease и не чинит индексы. Снимок не атомарен относительно всех ключей сразу.

Пагинация:

- `limit` — целое от 1 до 200 включительно. `0` и нецелое значение поднимают `ValueError`. Большее значение поднимает `ValueError`.
- Cursor непрозрачный и привязан к очереди и фильтру. Чужой или повреждённый cursor поднимает `ValueError`.
- Обход идёт по паре `(score, task_id)` и исключает элемент, который cursor называет.
- `ready`, `scheduled` и `inflight` идут по возрастанию score. `dead` и `dead_for_tag` — по убыванию.
- Конкурентное изменение score может пропустить или повторить элемент между страницами.
- Член индекса без hash задачи попадает на страницу как диагностическая запись `missing_job`. Admin его не удаляет и очередь из-за этого чтения не останавливает.

`dead_for_tag` читает индекс тега. Это отдельный метод чтения, а не фильтр предметной области: tag остаётся непрозрачной строкой.

`command` возвращает сохранённый receipt либо `None`, если срок хранения истёк и ключа нет.

## QueueControl

```python
control = QueueControl(client, queue="default")


class QueueControl:
    async def cancel(
        self,
        task_id: str,
        *,
        operation_id: str | None = None,
    ) -> CommandReceipt: ...

    async def kill(
        self,
        task_id: str,
        *,
        operation_id: str | None = None,
    ) -> CommandReceipt: ...

    async def requeue(
        self,
        task_id: str,
        *,
        operation_id: str | None = None,
    ) -> RequeueResult: ...
```

Других публичных методов управления supervisor нет. Поведение команд — в [operations.md](operations.md). Receipt подтверждает принятие команды, а не завершение остановки.

```python
class CommandReceipt:
    operation_id: str
    task_id: str
    status: str  # accepted | already_terminal | not_found
    cycle_id: str | None


class RequeueResult:
    operation_id: str
    task_id: str
    status: str  # requeued | duplicate | not_dead | not_found
    cycle_id: str | None
```

`delivery_token` в эти DTO не кладётся.

## Config

`Config.from_env()` читает только `ARQ_*`. Неразобранное значение, нуль там, где нужно положительное число, и нарушение неравенств поднимают ошибку. Пропущенная переменная получает значение по умолчанию. Любая установленная переменная `SIMPLE_Q_*` поднимает ошибку старта: старый префикс не игнорируется и не читается.

| Переменная | По умолчанию | Смысл |
|---|---:|---|
| `ARQ_POLL_INTERVAL_MS` | 500 | пауза idle-child между поисками работы |
| `ARQ_INFLIGHT_SCAN_INTERVAL_MS` | 500 | верхняя граница интервала проверки просроченного inflight |
| `ARQ_LEASE_MS` | 15000 | длительность нового lease |
| `ARQ_RENEW_INTERVAL_MS` | 5000 | период renew |
| `ARQ_RENEW_CONFIRM_MS` | 2000 | монотонный срок, за который renew должен подтвердиться |
| `ARQ_PING_INTERVAL_MS` | 2000 | период ping loop |
| `ARQ_PING_TIMEOUT_MS` | 3000 | срок одной стадии ping |
| `ARQ_LIFECYCLE_INTERVAL_MS` | 200 | период пробуждения lifecycle |
| `ARQ_PING_BUDGET_SECS` | 1860 | непрерывная тишина loop до кода 2 |
| `ARQ_SHUTDOWN_TIMEOUT_MS` | 30000 | ожидание dispatch при drain |
| `ARQ_CANCEL_GRACE_MS` | 5000 | ожидание кооперативной отмены |
| `ARQ_MAX_DELIVERIES` | 5 | предел доставок, если декларация его не переопределила на сообщении |
| `ARQ_MAX_TASKS_PER_CHILD` | 500 | recycle после стольких завершённых задач |
| `ARQ_MEMORY_SOFT_RATIO` | 0.80 | мягкий порог cgroup, drain |
| `ARQ_MEMORY_HARD_RATIO` | 0.95 | жёсткий порог cgroup, kill |
| `ARQ_CHILD_MEMORY_SOFT` | не задан | байты RSS мягкого порога child |
| `ARQ_CHILD_MEMORY_HARD` | не задан | байты RSS жёсткого порога child |
| `ARQ_HEARTBEAT_SECS` | 15 | период записи реестра supervisor |
| `ARQ_MISSED_HEARTBEATS` | 4 | окно свежести для admin |
| `ARQ_MAINTENANCE_INTERVAL_MS` | 60000 | период удаления terminal-записей |
| `ARQ_MAINTENANCE_BATCH` | 100 | размер одного batch удаления |
| `ARQ_WATCH_RETRIES` | 8 | повторы конфликта `WATCH` на одну попытку операции |
| `ARQ_DONE_TTL_SECS` | 86400 | retention `done` |
| `ARQ_DEAD_TTL_SECS` | 604800 | retention `dead` |
| `ARQ_RECEIPT_TTL_SECS` | 691200 | retention receipt |
| `ARQ_MAX_READY` | 100000 | предел всего ready, включая отложенные |
| `ARQ_MAX_MESSAGE_BYTES` | 1048576 | UTF-8 JSON `kwargs` |
| `ARQ_MAX_HEADERS_BYTES` | 65536 | сумма байт ключей и значений headers |
| `ARQ_MAX_ERROR_BYTES` | 4096 | сохраняемый текст ошибки |

Неравенства: `renew_interval_ms + renew_confirm_ms < lease_ms`, `receipt_ttl >= dead_ttl >= done_ttl`, `0 < memory_soft_ratio < memory_hard_ratio < 1`. Если заданы оба байтовых порога child, мягкий меньше жёсткого. `max_ready >= 1`.

Heartbeat реестра lease не продлевает и задачу не выдаёт. `ARQ_MAX_DELIVERIES` — значение по умолчанию для декларации; в сообщение всё равно копируется число с конкретной декларации.

OpenTelemetry выключен, пока в `App` не передан `Tracing`. Форма `Tracing` совпадает с донором: exporter, resource и необязательные targets. `tracing=None` слой не устанавливает.
