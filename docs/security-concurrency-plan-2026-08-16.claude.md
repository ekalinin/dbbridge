# План устранения находок по безопасности и конкурентности

Дополнение к [`pr-plan.md`](pr-plan.md). Здесь только находки, которых нет в
[`spec-implementation-review.md`](spec-implementation-review.md): дыра в
валидации формата результата, гонки за пределами watcher'ов, отсутствие
периметра безопасности и несколько неучтённых расхождений со
[`spec.md`](spec.md).

Каждый PR создаётся в отдельной ветке от актуальной `main`. Внутри потока PR
идут последовательно, между потоками - параллельно: потоки владеют
непересекающимися наборами файлов.

## Шкала критичности

| Уровень | Значение |
|---|---|
| **P0** | Эксплуатируется анонимно и разрушительно либо молча теряет уже посчитанный результат. Открывать немедленно, вне очереди волн `pr-plan.md` |
| **P1** | Нарушает обязательный инвариант спеки, даёт DoS или утечку внутренних данных |
| **P2** | Корректность и эксплуатация, есть обходной путь |
| **P3** | Контракт и косметика |

## Сводка находок

| ID | Находка | Критичность | PR | Поток |
|---|---|---|---|---|
| S1 | Path traversal через `result_format`: создание, усечение и удаление произвольных файлов | P0 | SEC-1 | A |
| S7 | Контейнер работает от root, нет `securityContext` | P0 | SEC-2 | D |
| C1 | `RedisMetaStore.Heartbeat` перетирает терминальное состояние (read-modify-write) | P0 | CONC-1 | B |
| S4 | Утечка DSN и внутренних ошибок в ответах, все ошибки как 500 | P1 | SEC-3 | C1 |
| S3 | WS отключает проверку Origin (CSWSH) | P1 | SEC-4 | C2 |
| C5 | WS: неограниченные подписки и горутины на соединение | P1 | SEC-4 | C2 |
| S5 | Нет лимита тела запроса и таймаутов `http.Server` | P1 | SEC-5 | D |
| S2 | Нет аутентификации и авторизации ни на одном транспорте | P1 | SEC-6 | E |
| C4 | `syncPools` держит write-lock на время сетевого I/O | P1 | PR 14 (расширить) | A |
| C2 | `reapStaleOwners` без fencing, машина состояний не применяется | P1 | CONC-3 | A |
| S6 | Секреты открытым текстом в ConfigMap, нет подстановки env | P1 | PR 3 (расширить) | D |
| S8 | `/metrics` и `/v1/admin/*` на публичном listener'е | P2 | SEC-7 | D |
| S9 | Нет TLS, gRPC поднят как h2c | P2 | SEC-8 | D |
| S11 | `ClientIPFromXFF` без списка доверенных прокси | P2 | SEC-8 | D |
| S12 | Нет проверки владельца запроса: любой `query_id` даёт чужой результат | P2 | SEC-9 | E |
| S10 | Нет ограничения числа одновременных запросов и rate limit | P2 | SEC-10 | A |
| C3 | GC работает на всех узлах одновременно без блокировки | P2 | CONC-4 | A+B |
| D1 | `CanIBeStopped` не использует `MetaStore.CountInFlight`, три метода мертвы | P2 | PR 24 (расширить) | A |
| D7 | В спеке нет раздела про authn, TLS и лимиты | P2 | SPEC-1 | F |
| D5 | Нет `416`, битый `Content-Range` при `SizeBytes == 0` | P3 | SPEC-2 | C1 |
| D4 | `mode` не валидируется | P3 | PR 7 (расширить) | A |
| D3 | Hot-reload не покрывает `instance.*` | P3 | SPEC-3 | D |

## Потоки параллельной работы

| Поток | Владеет файлами |
|---|---|
| **A. Ядро** | `internal/core/manager/`, `internal/core/domain/`, `internal/storage/backends/` |
| **B. MetaStore** | `internal/state/` |
| **C1. REST и Connect** | `internal/transport/rest/`, `internal/transport/grpcconnect/` |
| **C2. WebSocket** | `internal/transport/ws/` |
| **D. Периметр и деплой** | `cmd/dbbridge/main.go`, `internal/config/`, `deploy/` |
| **E. Аутентификация** | `internal/transport/authn/` (новый) + точки подключения |
| **F. Документы** | `docs/`, `api/openapi/`, `api/proto/` |

Пересечения, о которых нужно знать заранее:

- Поток A - самый загруженный: `manager.go` трогают ещё PR 4, 5, 7, 11, 14, 15,
  17, 23 из `pr-plan.md`. Порядок внутри A задан явно ниже.
- SEC-6 (аутентификация) в конце добавляет по одной строке в
  `rest/server.go` и `main.go`. Это единственное пересечение потока E с C1 и D,
  разрешается ребейзом.
- SEC-1 намеренно **не** трогает транспорты: он вводит типизированную ошибку, а
  маппинг в статус-коды делает SEC-3 в своём потоке.

## Волна 0 - три P0, стартуют одновременно

### SEC-1. Валидация формата результата и запирание storage в корне

- **Критичность:** P0
- **Ветка:** `fix/result-format-validation`
- **Коммит:** `fix(storage): validate result format and confine paths to root`
- **Поток:** A, первым, до всех остальных правок `manager.go`
- **Зависимости:** нет

**Проблема.** `result_format` приходит от клиента, не валидируется и попадает в
имя файла: `fs/storage.go:34-37` делает
`filepath.Join(rootDir, queryID+"."+format)` и `os.Create`. Проверено:
`format="../../../../../../etc/cron.d/dbb"` даёт путь `/etc/cron.d/dbb`.
`store.Writer` (`manager.go:309`) вызывается до того, как `EncodeStream`
отвергнет неизвестный формат, то есть файл уже создан и усечён; затем
`deletePartial` (`manager.go:730-734`) вызывает `os.Remove` по тому же пути.
Итог: анонимное усечение и удаление произвольного файла.

**Файлы:**

- `internal/core/domain/errors.go` - добавить `ValidationError` рядом с
  `DrainingError`
- `internal/core/domain/models.go` - добавить `ValidResultFormats` и
  `(QueryOptions).Validate()`
- `internal/core/manager/manager.go:158-168` - вызвать валидацию после
  подстановки дефолтов и **до** `AcquireIdempotency`
- `internal/storage/backends/fs/storage.go` - `filepath.IsLocal` в `Writer`,
  `os.Root` для `Create`/`Open`/`Remove`/`Stat`
- `internal/storage/backends/s3/storage.go:75` - отклонять ключ, не проходящий
  `filepath.IsLocal`
- Тесты: `internal/core/manager/manager_test.go`,
  `internal/storage/backends/fs/storage_test.go`

**Что делать.**

1. В `domain/errors.go`:

```go
// ValidationError marks a client-side input error. Transports match it via
// errors.AsType and map it to 400 (REST) / InvalidArgument (gRPC).
type ValidationError struct {
	Field  string
	Reason string
}

func (e ValidationError) Error() string {
	return "invalid " + e.Field + ": " + e.Reason
}
```

2. В `domain/models.go` - белый список и валидация. `parquet` отклоняется явно,
   до реализации настоящего encoder'а (это поглощает PR 10 из `pr-plan.md`):

```go
// ValidResultFormats lists the encodings EncodeStream can actually produce.
var ValidResultFormats = []string{"jsonl", "csv"}

func (o QueryOptions) Validate() error {
	if !slices.Contains(ValidResultFormats, o.ResultFormat) {
		return ValidationError{
			Field:  "result_format",
			Reason: "must be one of " + strings.Join(ValidResultFormats, ", "),
		}
	}
	return nil
}
```

3. В `SubmitQuery` после блока дефолтов (`manager.go:163-168`):

```go
if err := opts.Validate(); err != nil {
	return nil, err
}
```

4. В `fs/storage.go` держать `*os.Root`, открытый в `NewFSResultStore` через
   `os.OpenRoot(rootDir)`, и выполнять все файловые операции через него.
   `Writer` дополнительно проверяет `filepath.IsLocal(filename)`. `Reader`,
   `Stat` и `Delete` получают `ref.Locator` из метаданных, то есть из внешнего
   хранилища, - для них считать относительный путь через `filepath.Rel(root, …)`
   и отклонять всё, что начинается с `..`. Формат `Locator` не менять, иначе
   сломаются уже сохранённые записи.

**Тест, который должен падать до правки** (`manager_test.go`, стиль существующих
тестов, хелпер `newManager(t)`):

```go
func TestSubmitQuery_RejectsTraversalFormat(t *testing.T) {
	qm, _ := newManager(t)
	victim := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(victim, []byte("keep me"), 0o600); err != nil {
		t.Fatalf("seed victim: %v", err)
	}

	rel, err := filepath.Rel(resultsDir, victim)
	if err != nil {
		t.Fatalf("rel: %v", err)
	}

	_, err = qm.SubmitQuery(context.Background(), "testdb", "SELECT 1",
		domain.QueryOptions{Mode: "async", ResultFormat: rel})
	if _, ok := errors.AsType[domain.ValidationError](err); !ok {
		t.Fatalf("SubmitQuery error = %v, want domain.ValidationError", err)
	}
	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("victim file was touched: %v", err)
	}
}
```

Плюс тест в `fs/storage_test.go`: `Writer(ctx, "id", "../escape")` возвращает
ошибку и не создаёт файл за пределами корня.

**Проверка:** `go test ./internal/core/manager/... ./internal/storage/...`,
затем ручной smoke против запущенного инстанса:
`POST /v1/queries` с `"result_format": "../../tmp/x"` возвращает 400 (после
SEC-3; до него - 500), файл `/tmp/x` не появляется.

### SEC-2. Контейнер не от root

- **Критичность:** P0
- **Ветка:** `chore/container-nonroot`
- **Коммит:** `chore(deploy): run container as non-root`
- **Поток:** D, Go-код не трогается, полностью параллелен всему
- **Зависимости:** нет

**Проблема.** В `deploy/Dockerfile` нет `USER`, в `deploy/k8s/deployment.yaml`
нет `securityContext`. Процесс работает от root, что превращает SEC-1 из
«записи в каталог результатов» в «запись куда угодно в контейнере».

**Файлы:** `deploy/Dockerfile`, `deploy/k8s/deployment.yaml`,
`deploy/docker-compose.yaml`.

**Что делать.**

- В `Dockerfile` во втором стейдже: `adduser -D -u 10001 dbbridge`,
  `chown dbbridge:dbbridge /app/results`, `USER dbbridge`.
- В `deployment.yaml` добавить pod- и container-level `securityContext`:
  `runAsNonRoot: true`, `runAsUser: 10001`, `allowPrivilegeEscalation: false`,
  `readOnlyRootFilesystem: true`, `capabilities.drop: [ALL]`,
  `seccompProfile.type: RuntimeDefault`.
- Ловушка: `main.go:77` создаёт FS-store безусловно, даже когда
  `default_storage: s3`, то есть `MkdirAll("/data/results")` выполняется всегда,
  а в `deployment.yaml` сейчас смонтирован только том с конфигом. При
  `readOnlyRootFilesystem: true` старт упадёт на `log.Fatalf`, поэтому в том же
  PR добавить `emptyDir` (или PVC) на `/data/results`.

**Проверка:** `docker build -f deploy/Dockerfile .`,
`docker run --rm --entrypoint id <image> -u` возвращает `10001`;
`make up` и smoke из `deploy/README.md` проходят.

### CONC-1. Лиза в отдельном ключе вместо перезаписи записи

- **Критичность:** P0
- **Ветка:** `fix/heartbeat-lease-keys`
- **Коммит:** `fix(state): stop overwriting query records in heartbeat`
- **Поток:** B, целиком внутри `internal/state`
- **Зависимости:** нет

**Проблема.** `redis.go:144-152`: `Heartbeat` делает `GetQuery` →
выставить `LeaseDeadline` → `UpdateQuery`, то есть полную перезапись JSON.
Параллельно `run()` пишет `SUCCEEDED` с `ResultRef` (`manager.go:357`).
Интерливинг «heartbeat прочитал RUNNING → run записал SUCCEEDED → heartbeat
записал обратно RUNNING» оставляет запись навечно в `RUNNING` с `Result == nil`
(нарушение I4), возвращает ID в `dbbridge:instance:<id>:queries` (нарушение I5),
и GC такую запись не подберёт. Окно - каждые `heartbeat_ttl/2`, по умолчанию
2.5 с, на каждый активный запрос. В memory-store проблемы нет: там `Heartbeat`
меняет только поле `LeaseDeadline` под тем же мьютексом (`memory.go:88-92`).

**Файлы:** `internal/state/redis.go`, `internal/state/memory.go`,
`internal/state/state_test.go`, `go.mod` (тестовая зависимость).

**Что делать.**

- Ввести ключ `dbbridge:lease:<queryID>` со значением `ownerInstanceID` и TTL.
  `Heartbeat` обновляет его одним пайплайном `SET key owner EX ttl` на каждый
  owned-запрос, **не читая и не переписывая запись запроса**.
- `GetQuery` подставляет `LeaseDeadline` из оставшегося TTL ключа лизы, чтобы
  поле в API не стало мёртвым.
- `ListStaleQueries` проверяет отсутствие именно ключа лизы запроса (сейчас -
  ключа instance), это точнее и не зависит от того, успел ли владелец записать
  `LeaseDeadline`.
- В `DeleteQuery` и при переходе в терминальное состояние ключ лизы удаляется.

**Тест.** Redis-реализация сейчас не покрыта юнит-тестами вообще
(`state_test.go` тестирует только memory). Добавить в `go.mod` тестовую
зависимость `github.com/alicebob/miniredis/v2` и написать детерминированный
тест, не требующий гонки:

```go
func TestRedisHeartbeatDoesNotRewriteRecord(t *testing.T) {
	mr := miniredis.RunT(t)
	store := state.NewRedisMetaStore(mr.Addr(), "", 0)
	t.Cleanup(func() { _ = store.Close() })

	rec := &domain.QueryRecord{
		ID: "q1", DatabaseID: "db1", State: domain.StateRunning,
		OwnerInstanceID: "inst-1", Options: domain.QueryOptions{ResultTTL: time.Hour},
	}
	if err := store.PutQuery(t.Context(), rec); err != nil {
		t.Fatalf("PutQuery: %v", err)
	}
	before := mr.Get("dbbridge:query:q1")

	if err := store.Heartbeat(t.Context(), "inst-1", []string{"q1"}, 5*time.Second); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if got := mr.Get("dbbridge:query:q1"); got != before {
		t.Fatalf("heartbeat rewrote the query record:\nbefore=%s\nafter =%s", before, got)
	}
	if !mr.Exists("dbbridge:lease:q1") {
		t.Fatal("lease key was not written")
	}
}
```

Второй тест: `ListStaleQueries` возвращает запрос после `mr.FastForward(ttl)` и
не возвращает его сразу после `Heartbeat`.

**Проверка:** `go test -race ./internal/state/...`.

## Волна 1 - P1, четыре потока параллельно

### SEC-3. Статус-коды и санитайзинг ошибок

- **Критичность:** P1
- **Ветка:** `fix/transport-error-responses`
- **Коммит:** `fix(rest): map domain errors to status codes and hide internals`
- **Поток:** C1
- **Зависимости:** SEC-1 (нужен тип `domain.ValidationError`)

**Проблема.** `rest/server.go:133` отдаёт наружу `err.Error()`, а
`manager.go:153` оборачивает ошибку драйвера текстом
`"database %s is unreachable: %w"`; у pgx и mysql такая ошибка содержит хост,
пользователя и параметры подключения. Все ошибки, кроме draining, - 500, включая
несуществующий `db_id` и невалидные опции. `QueryError.Message` тоже хранит
сырой текст ошибки БД и отдаётся любому, кто знает `query_id`.

**Файлы:** `internal/transport/rest/server.go`,
`internal/transport/grpcconnect/handler.go`, `internal/core/domain/errors.go`
(добавить `NotFoundError` при необходимости), `internal/core/manager/manager.go`
(не вкладывать ошибку драйвера в текст, отдавать типизированную).

**Что делать.**

- Единый хелпер в `rest`: `writeError(w, r, err)` - разбирает `err` через
  `errors.AsType` в порядке `ValidationError` → 400, `state.ErrNotFound` → 404,
  `DrainingError` → 503, остальное → 500 с телом
  `{"error":"internal error","request_id":"..."}` и полным текстом в лог.
- Аналогично в `grpcconnect`: `CodeInvalidArgument`, `CodeNotFound`,
  `CodeUnavailable`, `CodeInternal`, наружу - без `err`.
- `QueryError.Message` для кодов уровня БД оставить, но обрезать до разумной
  длины и не включать DSN: в `manager.go:153` не оборачивать ошибку `Ping`,
  а возвращать `ValidationError`/типизированную ошибку без текста драйвера.

**Тест:** `rest/server_test.go` - `POST /v1/queries` с несуществующим
`database_id` даёт 404 или 400, но не 500; тело ответа не содержит подстроки
`dsn`, `password`, `postgres://`.

**Проверка:** `go test ./internal/transport/...`.

### SEC-4. Укрепление WebSocket

- **Критичность:** P1
- **Ветка:** `fix/ws-hardening`
- **Коммит:** `fix(ws): check origin and bound per-connection subscriptions`
- **Поток:** C2
- **Зависимости:** нет. Обязателен **до** PR 18 из `pr-plan.md`

**Проблема.** `ws/server.go:36` - `InsecureSkipVerify: true`, проверка Origin
отключена: любая страница в браузере внутри периметра открывает соединение с
dbbridge и читает события чужих запросов (CSWSH). `ws/server.go:78` - на каждое
сообщение `{"action":"watch"}` запускается новая горутина и регистрируется новый
watcher-канал, снимаются они только при закрытии соединения; ID запроса при этом
не проверяется (`manager.go:454`). Одно соединение в цикле даёт неограниченный
рост горутин и памяти. Объявленный в `ClientMessage` `action: "unwatch"` не
обработан.

**Файлы:** `internal/transport/ws/server.go`, `internal/config/config.go`
(поле `server.ws_allowed_origins`), `internal/transport/ws/server_test.go`.

**Что делать.**

- Заменить `InsecureSkipVerify: true` на
  `OriginPatterns: cfg.Server.WSAllowedOrigins`. Пустой список - запрет
  cross-origin (поведение `coder/websocket` по умолчанию, same-origin
  разрешён). Прокинуть конфиг в `NewHub`.
- Хранить в соединении `map[string]context.CancelFunc` подписок: повторный
  `watch` того же `query_id` не создаёт вторую горутину, `unwatch` отменяет
  контекст конкретной подписки.
- Ввести константу `maxSubscriptionsPerConn = 32`; при превышении отвечать
  сообщением об ошибке и не создавать подписку.
- Все подписки отменяются при выходе из `ServeHTTP`.

**Тест:** `ws/server_test.go` - соединение отправляет 100 сообщений `watch` с
разными ID, после чего число активных подписок не превышает 32; повторный
`watch` того же ID не увеличивает счётчик; `unwatch` уменьшает. Плюс тест, что
`websocket.Dial` с чужим `Origin` получает отказ.

**Проверка:** `go test -race ./internal/transport/ws/...`.

### SEC-5. Лимиты и таймауты HTTP-серверов

- **Критичность:** P1
- **Ветка:** `fix/http-server-limits`
- **Коммит:** `fix(server): add request size and connection timeouts`
- **Поток:** D
- **Зависимости:** согласовать с PR 6 из `pr-plan.md` (он снимает
  `middleware.Timeout` со скачивания и sync-запросов)

**Проблема.** У обоих `http.Server` (`main.go:116-119`, `main.go:127-130`) не
заданы `ReadHeaderTimeout`, `ReadTimeout`, `IdleTimeout`, `MaxHeaderBytes` -
оба слушателя открыты для Slowloris. Тело `POST /v1/queries` читается без
`http.MaxBytesReader` (`rest/server.go:100`), размер SQL ничем не ограничен.

**Файлы:** `cmd/dbbridge/main.go`, `internal/transport/rest/server.go`,
`internal/config/config.go`.

**Что делать.**

- На обоих серверах: `ReadHeaderTimeout: 10s`, `IdleTimeout: 120s`,
  `MaxHeaderBytes: 1 << 20`.
- `WriteTimeout` **не** ставить глобально: он оборвёт скачивание больших
  результатов и WebSocket. Ограничение времени записи делать пороутово вместе
  с PR 6.
- `ReadTimeout` тоже глобально не ставить (WS живёт долго); вместо него
  `MaxBytesReader` на теле `POST /v1/queries` с лимитом из конфига
  (`server.max_request_bytes`, дефолт 1 MiB) и обработка
  `*http.MaxBytesError` как 413.

**Тест:** `rest/server_test.go` - `POST /v1/queries` с телом больше лимита даёт
413 и не создаёт запрос.

**Проверка:** `go test ./internal/transport/rest/...`.

### SEC-6. Аутентификация и авторизация транспортов

- **Критичность:** P1
- **Ветка:** `feat/transport-auth`
- **Коммит:** `feat(authn): require credentials on api endpoints`
- **Поток:** E
- **Зависимости:** нет технических, но **нужно решение владельца по механизму**
  (см. раздел «Требует решения»)

**Проблема.** Middleware аутентификации нет ни в одном транспорте, в OpenAPI нет
ни одной `securityScheme`. Открыты в том числе `POST /v1/admin/reload` и
`GET /v1/queries/{id}/result`. Фактически это неаутентифицированное выполнение
произвольного SQL под сервисными кредами всех настроенных БД.

**Файлы:** новый пакет `internal/transport/authn/`,
`internal/transport/rest/server.go` (одна строка `r.Use`),
`cmd/dbbridge/main.go` (connect-интерцептор), `internal/config/config.go`,
`api/openapi/dbbridge.yaml`, `deploy/k8s/*`.

**Что делать (вариант со статическими токенами, минимальный по объёму).**

- `internal/transport/authn`: тип `Token{Value, Subject, Scopes []string}`,
  загрузка из конфига (`auth.tokens`) с чтением значения из переменной
  окружения, сравнение через `crypto/subtle.ConstantTimeCompare`.
- Скоупы: `read` (status, stats, download, list, watch), `write` (start, stop),
  `admin` (reload, can-stop).
- chi-middleware для `/v1/*` и connect-интерцептор для gRPC. Исключения:
  `/healthz`, `/readyz`, `/metrics` (последний уезжает на отдельный listener в
  SEC-7).
- В `api/openapi/dbbridge.yaml` добавить `securitySchemes.bearerAuth` и
  глобальный `security`.
- Отсутствие настроенных токенов при непустом `auth` - ошибка старта, а не
  тихий пропуск.

**Тест:** `rest/server_test.go` и `grpcconnect/handler_test.go` - запрос без
токена даёт 401 / `Unauthenticated`, с токеном без нужного скоупа - 403 /
`PermissionDenied`, `/healthz` и `/readyz` доступны без токена.

**Проверка:** `go test ./internal/transport/...`, `make ci`.

### CONC-3. Fencing владельца и применение машины состояний

- **Критичность:** P1
- **Ветка:** `fix/owner-fencing`
- **Коммит:** `fix(manager): fence owner takeover and enforce state machine`
- **Поток:** A, после SEC-1
- **Зависимости:** CONC-1, PR 5 из `pr-plan.md`

**Проблема.** `manager.go:668-677`: любой узел, не увидев ключа
`dbbridge:instance:<owner>`, ставит `FAILED/OWNER_LOST`, не проверяя, что
владелец - не он сам, и что запись с тех пор не менялась. Кратковременный сбой
Redis или GC-пауза дольше `heartbeat_ttl` (5 с) - и живому запросу проставляется
FAILED, после чего владелец перетирает его на SUCCEEDED. `CanTransitionTo`
(`models.go:31`) при этом не вызывается нигде: переходы не проверяются вообще.

**Файлы:** `internal/core/manager/manager.go`,
`internal/core/domain/models.go`, `internal/state/` (условная запись),
`internal/core/manager/manager_test.go`.

**Что делать.**

- В `reapStaleOwners` пропускать записи, где `rec.OwnerInstanceID == qm.instanceID`
  (сейчас проверяется только локальный `activeReg`, чего мало после рестарта).
- Добавить в `MetaStore` условную запись
  `UpdateQueryIfState(ctx, rec, expected QueryState) (bool, error)`: Redis -
  Lua-скрипт с проверкой поля `state`, memory - под тем же мьютексом. Reaper
  переводит в FAILED только если состояние всё ещё `PENDING`/`RUNNING`.
- Проверять `rec.State.CanTransitionTo(next)` перед каждой записью состояния в
  `manager.go` и логировать отказ. Это делает `CanTransitionTo` живым кодом и
  закрывает расхождение с §3 спеки.

**Тест:** `manager_test.go` - запись в `SUCCEEDED`, затем `reapStaleOwners()`;
состояние остаётся `SUCCEEDED`, `Result` не потерян. Второй тест: запрос,
принадлежащий текущему instance, reaper не трогает даже без ключа лизы.

**Проверка:** `go test -race ./internal/core/manager/... ./internal/state/...`.

## Волна 2 - P2

### SEC-7. Отдельный listener для метрик и админки

- **Критичность:** P2 | **Ветка:** `feat/admin-listener` |
  **Коммит:** `feat(server): move metrics and admin to a separate listener` |
  **Поток:** D | **Зависимости:** SEC-6

`/metrics` отдаётся с публичного listener'а и раскрывает `db_id` всех
настроенных БД; `/v1/admin/*` там же. Вынести оба на `server.admin_addr`
(дефолт `:8081`), в k8s не публиковать порт в Service, поправить
`prometheus.io/port` в аннотациях и `deploy/prometheus.yml`.

### SEC-8. TLS и доверенные прокси

- **Критичность:** P2 | **Ветка:** `feat/tls` |
  **Коммит:** `feat(server): support tls and trusted proxy headers` |
  **Поток:** D | **Зависимости:** нет

TLS нет нигде, gRPC поднят с `SetUnencryptedHTTP2` (`main.go:133`). Добавить
`server.tls.{cert_file,key_file}` и `ListenAndServeTLS`, оставив h2c-режим
явным опт-ином для локальной разработки. Заодно передать в
`middleware.ClientIPFromXFF` список доверенных CIDR из конфига (или
`ClientIPFromXFFTrustedProxies(n)`, если известно только число хопов): сейчас
он вызывается без аргументов (`rest/server.go:45`), то есть берёт крайний
правый элемент `X-Forwarded-For`, что корректно ровно при одном доверенном
прокси перед сервисом и подделывается заголовком в любой другой топологии.

### SEC-9. Привязка запроса к субъекту

- **Критичность:** P2 | **Ветка:** `feat/query-ownership` |
  **Коммит:** `feat(manager): bind queries to the requesting subject` |
  **Поток:** E | **Зависимости:** SEC-6

Сейчас знание `query_id` даёт доступ к чужому SQL, статусу и результату. Добавить
`QueryRecord.Subject`, заполнять из контекста аутентификации, проверять в
`GetQueryStatus`, `GetQueryStats`, `DownloadResult`, `StopQuery` и `Watch`.
Скоуп `admin` видит всё. Требует миграции: у старых записей поле пустое -
трактовать как «доступно только admin».

### SEC-10. Ограничение параллелизма и rate limit

- **Критичность:** P2 | **Ветка:** `feat/query-concurrency-limit` |
  **Коммит:** `feat(manager): cap concurrent query execution` |
  **Поток:** A, после CONC-3 | **Зависимости:** SEC-6 для per-token лимита

`SubmitQuery` запускает горутину без семафора: любой клиент открывает сколько
угодно запросов, каждый занимает соединение пула и файл на диске. Добавить
`defaults.max_concurrent_queries` (семафор, при переполнении - 429/
`ResourceExhausted`) и per-token rate limit на `golang.org/x/time/rate` (новая
прямая зависимость).

### CONC-4. Единственный исполнитель GC

- **Критичность:** P2 | **Ветка:** `fix/gc-single-flight` |
  **Коммит:** `fix(manager): run garbage collection under a cluster lock` |
  **Поток:** A + B | **Зависимости:** PR 15 из `pr-plan.md`

`collectGarbage` (`manager.go:589`) вызывается независимо на каждом instance,
`ListExpiredQueries` возвращает всем один и тот же набор: узлы параллельно
удаляют один результат, один успевает, остальные логируют ошибку удаления, что
маскирует настоящие сбои очистки. Добавить в `MetaStore`
`TryLock(ctx, name string, ttl time.Duration) (bool, error)` (Redis - `SET NX
PX`, memory - тривиально) и брать `dbbridge:gc:lock` на время прохода. Заодно
`ListExpiredQueries`/`ListStaleQueries` делают `SCAN` + `GET` по всем ключам
раз в минуту с каждого узла - отметить в PR как известное ограничение либо
завести индексный `ZSET` по `finished_at`.

### SPEC-1. Раздел безопасности в спецификации

- **Критичность:** P2 | **Ветка:** `docs/spec-security-section` |
  **Коммит:** `docs(spec): describe authn, tls and request limits` |
  **Поток:** F, полностью параллелен всему | **Зависимости:** нет

В `spec.md` нет ни слова про аутентификацию, авторизацию, TLS, лимиты запроса и
владение результатом - это пробел самой спеки, а не расхождение с ней.
Одновременно актуализировать два документа: в
`spec-implementation-review.md` пометить пункт 1 (гонка watcher'ов) закрытым
коммитом `2af6e1c` и обновить раздел «Выполненные проверки» (линтеры и CI
настроены), в `spec-divergences.md` снять пункт «§14 - no CI».

## Волна 3 - P3

### SPEC-2. Семантика Range

- **Критичность:** P3 | **Ветка:** `fix/rest-range-semantics` |
  **Коммит:** `fix(rest): return 416 for unsatisfiable ranges` |
  **Поток:** C1 | **Зависимости:** PR 8 из `pr-plan.md`

`rest/server.go:263-274`: при `rangeStart >= SizeBytes` возвращается 206 вместо
416, при `SizeBytes == 0` заголовок вырождается в `Content-Range: bytes 0--1/*`.

### SPEC-3. Область действия hot-reload

- **Критичность:** P3 | **Ветка:** `docs/reload-scope` |
  **Коммит:** `docs(config): document non-reloadable settings` |
  **Поток:** D | **Зависимости:** PR 14 из `pr-plan.md`

`qm.instanceID` фиксируется в конструкторе (`manager.go:53`), metastore и
storage создаются один раз в `main.go`. Reload меняет только снапшот. Либо
задокументировать `instance.*`, `server.*` и `storage.*` как требующие
рестарта, либо возвращать их в `ReloadReport` как проигнорированные.

## Изменения к pr-plan.md

| PR из `pr-plan.md` | Что меняется |
|---|---|
| PR 10 `fix/reject-unimplemented-parquet` | **Поглощён SEC-1.** Белый список форматов решает ту же задачу и закрывает path traversal; отдельный PR не нужен |
| PR 3 `fix/k8s-instance-id` | **Расширить (S6):** заодно перенести `redis_password`, `access_key_id`, `secret_access_key` и DSN из ConfigMap в Secret. Подстановка env, которую PR и так вводит ради `POD_NAME`, - тот же механизм |
| PR 14 `fix/database-pool-reload` | **Расширить (C4):** `syncPools` держит `dbPoolsMu.Lock()` во время `db.OpenPool` (`manager.go:76-91`), а тот делает `PingContext`. На недоступной БД reload блокирует все `SubmitQuery` и heartbeat. Открывать пулы вне блокировки, под мьютексом делать только подмену map |
| PR 7 `fix/query-option-defaults` | **Расширить (D4):** заодно валидировать `mode` - сейчас `"SYNC"` молча становится async. Использовать `domain.ValidationError` из SEC-1 |
| PR 24 `feat/stoppable-lifecycle` | **Расширить (D1):** §9 спеки требует, чтобы `CanIBeStopped` зависел от `CountInFlight`, а `service.go:121-124` считает локальный `activeReg`. После рестарта pod'а с тем же instance ID узел рапортует `can_be_stopped=true`, имея owned-запросы в Redis. Заодно задействовать или удалить мёртвые `ListByInstance` и `ListDatabasesSeen` |
| PR 17 `fix/watch-current-state` | Синхронизировать с SEC-4: проверка существования `query_id` в `Watch` и ограничение подписок в WS решают одну задачу с разных сторон |
| PR 18 `fix/ws-request-timeout` | **Только после SEC-4.** Снятие 60-секундного таймаута без ограничения подписок убирает единственное, что сейчас ограничивает время жизни утечки горутин |

## Требует решения

1. **Механизм аутентификации (SEC-6).** Статические bearer-токены из конфига -
   минимум по объёму и достаточно для внутреннего периметра. Альтернативы:
   mTLS (естественно для service-to-service, но требует PKI) или OIDC-токены
   (нужен провайдер). От выбора зависит объём SEC-6 и SEC-9.
2. **Терминируется ли TLS на LB (SEC-8).** Если да, PR сводится к
   `ClientIPFromXFFTrustedProxies` и документации, а сам TLS не нужен.
3. **Запрет DML и DDL.** В `TODO.md` уже есть пункт «проверка: только select,
   иначе 400». Это не заменяет SEC-6, но резко снижает последствия компрометации
   токена. Если решение положительное - это отдельный PR в потоке A, по
   критичности P1, и он должен использовать парсер, а не регулярку.

## Порядок

1. Волна 0 целиком параллельна: SEC-1 (поток A), SEC-2 (D), CONC-1 (B). Три
   разработчика, три непересекающихся набора файлов.
2. Волна 1: SEC-3 (C1), SEC-4 (C2), SEC-5 (D), SEC-6 (E) стартуют одновременно;
   CONC-3 (A) - после SEC-1 и CONC-1.
3. Волна 2 и 3 - по мере освобождения потоков; SPEC-1 (F) можно делать в любой
   момент, он ни с чем не пересекается.
