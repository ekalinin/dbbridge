# План PR

Скорректированный план работ по итогам
[`spec-implementation-review.md`](spec-implementation-review.md). Отличия от
исходного разбиения перечислены в конце документа.

Каждый PR создаётся в отдельной ветке от актуальной `main`. Зависимые PR
открываются и сливаются в указанном порядке, изменения не объединяются в одну
ветку.

## Волна 0 - разблокировка quality gates

Ни от чего не зависит, может идти параллельно с волной 1. Смысл в том, чтобы
`errcheck` заработал до того, как начнётся ручная чистка `_ = err` в волне 1.

| PR | Ветка | Commit | Scope | Зависимости |
|---|---|---|---|---|
| 0 | `chore/lint-config-version` | `chore(ci): fix golangci and buf lint configs` | Добавить `version: "2"` в `.golangci.yml` (сейчас `golangci-lint run` падает с `unsupported version of the configuration: ""`), устранить `buf lint` (package `dbbridge.v1` лежит в `api/proto/dbbridge/v1`, не совпадает с module root) | Нет |
| 1 | `chore/ci-quality-gates` | `chore(ci): add unit race vet lint and proto checks` | CI workflow: `go test`, `go test -race`, `go vet`, `golangci-lint`, `buf lint`, `gofmt` | PR 0 |

## Волна 1 - падение процесса, потеря данных, нерабочий multi-node

| PR | Ветка | Commit | Scope | Зависимости |
|---|---|---|---|---|
| 2 | `fix/query-watcher-race` | `fix(manager): prevent watcher send-close race` | Устранить обе гонки между `notifyWatchers` и закрытием канала: `send on closed channel` и мутацию backing array слайса в `append(list[:i], list[i+1:]...)`. Regression-тест под `-race` с отменой подписки во время уведомления | Нет |
| 3 | `fix/k8s-instance-id` | `fix(deploy): assign unique instance ids` | `dbbridge-$(POD_NAME)` в ConfigMap остаётся литералом (в загрузчике конфигурации нет подстановки переменных окружения), обе реплики считают себя одним owner. Передавать уникальный pod ID в конфигурацию и проверить ownership на двух репликах | Нет |
| 4 | `fix/result-finalization` | `fix(manager): handle result writer close errors` | Проверять ошибку `writer.Close`: для S3 и ClickHouse именно `Close` дожидается upload/commit и возвращает финальную ошибку. Не переводить запрос в `SUCCEEDED` при неуспешной материализации, очищать частичный результат | Нет |
| 5 | `fix/query-state-persistence` | `fix(manager): handle query state persistence failures` | Перестать игнорировать ошибки сохранения `RUNNING` и терминальных состояний, не оставлять metadata в ложном `PENDING`/`RUNNING` после удаления запроса из локального active registry | Нет |
| 6 | `fix/download-request-timeout` | `fix(rest): exempt result download and sync query from request timeout` | Общий `middleware.Timeout(60s)` применяется ко всему роутеру, включая скачивание результата и sync-запросы. Вынести их из-под общего таймаута, сохранив его для обычных HTTP-операций | Нет |

## Волна 2 - публичный контракт

| PR | Ветка | Commit | Scope | Зависимости |
|---|---|---|---|---|
| 7 | `fix/query-option-defaults` | `fix(manager): normalize query option defaults` | Сохранять `mode: async` при отсутствии mode, применять `defaults.query_timeout` (объявлен в конфиге, не используется нигде) | Нет |
| 8 | `fix/rest-openapi-contract` | `fix(rest): align responses with openapi schema` | REST отдаёт доменную модель напрямую: `timeout`, `result_ttl`, `db_exec_duration` в наносекундах, тогда как OpenAPI обещает `timeout_ms`, `result_ttl_seconds`, `db_exec_duration_ms`. API асимметричен сам себе - принимает `timeout_ms`, возвращает наносекунды. Ввести транспортные DTO и contract-тесты | PR 7 |
| 9 | `fix/connect-zero-timestamps` | `fix(grpc): map absent timestamps to zero` | `time.Time{}.UnixNano()/1e6` даёт для `PENDING` записи отрицательные `started_at_ms`, `finished_at_ms`, `lease_deadline_ms`. Возвращать ноль | Нет |
| 10 | `fix/reject-unimplemented-parquet` | `fix(storage): reject unimplemented parquet format` | Сейчас `parquet` сериализуется как JSONL, клиент получает файл `.parquet` с JSONL внутри и `format: "parquet"` в метаданных. До реализации настоящего encoder (PR 20) отклонять `result_format: parquet` явной ошибкой валидации | Нет |

## Волна 3 - отмена, storage lifecycle, идемпотентность

| PR | Ветка | Commit | Scope | Зависимости |
|---|---|---|---|---|
| 11 | `fix/query-cancellation-state` | `fix(manager): preserve canceled and timed out query state` | Любая ошибка `Pool.Exec` превращается в `DB_EXEC_FAILED` без разбора. Распознавать `context.Canceled` (сохранять `CANCELED`) и `context.DeadlineExceeded` (отдельный код ошибки таймаута). Покрыть отмену до получения `RowStream` | Нет |
| 12 | `fix/storage-cancellation` | `fix(storage): propagate query cancellation to writers` | Для writer создаётся отдельный `context.Background()`, S3 дополнительно игнорирует переданный контекст и вызывает `Upload` с `context.Background()`. Передавать execution context в storage writer, прекращать S3/ClickHouse upload после stop или timeout | PR 4, PR 11 |
| 13 | `fix/idempotency-retention` | `fix(state): align idempotency with result retention` | Ключ захватывается до `PutQuery`, поэтому ошибка записи metadata оставляет ключ на несуществующий query ID. TTL ключа считается от submission, а retention результата - от `FinishedAt`. `QueryRecord.IdempotencyKey` не заполняется нигде, хотя cleanup в memory- и redis-store читает именно его | PR 5 |
| 14 | `fix/database-pool-reload` | `fix(config): apply database pool changes on reload` | `syncPools` переиспользует пул строго по ID и не читает результат `DiffDatabases`, поэтому изменение DSN, engine или `max_conns` не применяется, а отчёт при этом гарантированно возвращает ложный `updated`. Ошибки открытия новых pool только логируются, reload возвращает успех. Удалённые pool закрываются сразу, без drain | Нет |
| 15 | `fix/gc-storage-cleanup` | `fix(manager): retain metadata when storage cleanup fails` | Не удалять metadata при ошибке удаления результата из storage, сохранять возможность повторной очистки | PR 5 |

## Волна 4 - domain-контракт и подписки

| PR | Ветка | Commit | Scope | Зависимости |
|---|---|---|---|---|
| 16 | `refactor/query-error-codes` | `refactor(domain): define query error codes` | Заменить свободные строки кодов ошибок на ограниченный domain-тип и обновить transport mapping | Нет |
| 17 | `fix/watch-current-state` | `fix(manager): validate watches and emit current state` | `Watch` не проверяет существование query ID и не отправляет текущее состояние, поэтому подписка после завершения запроса ждёт вечно. Проверять ID, сразу отдавать текущее состояние, завершать подписку после терминального | PR 2 |
| 18 | `fix/ws-request-timeout` | `fix(rest): exempt websocket from request timeout` | WebSocket наследует контекст запроса и умирает примерно через минуту из-за общего `middleware.Timeout(60s)`. Снимать таймаут только после PR 17, иначе зависшая подписка перестанет обрываться вообще | PR 17 |
| 19 | `feat/distributed-query-watch` | `feat(state): publish query events across instances` | Watchers хранятся в локальной map, Redis Pub/Sub используется только для `STOP_QUERY`. Доставлять query events через MetaStore, чтобы Watch работал на любом instance | PR 17 |

## Волна 5 - форматы и storage

| PR | Ветка | Commit | Scope | Зависимости |
|---|---|---|---|---|
| 20 | `feat/parquet-results` | `feat(storage): implement parquet result encoding` | Реализовать настоящий Parquet encoder, снять запрет из PR 10, добавить тест чтения созданного файла | PR 4, PR 10 |
| 21 | `feat/clickhouse-result-storage` | `feat(storage): register clickhouse result backend` | Backend написан, но в `main.go` регистрируются только FS и S3, поэтому `storage_backend: clickhouse` падает с `unknown storage backend` уже после начала выполнения SQL. Инициализировать и регистрировать ResultStore из config, добавить валидацию конфигурации и lifecycle cleanup | PR 4, PR 12 |
| 22 | `feat/result-checksums` | `feat(storage): calculate result checksums` | `ResultRef.Checksum` всегда пустой. Рассчитывать и сохранять при streaming, без повторного чтения результата | PR 4 |
| 23 | `feat/query-progress-stats` | `feat(manager): persist query progress statistics` | Stats и progress записываются только при завершении. Обновлять rows, bytes и streaming progress до терминального состояния, публиковать progress events | PR 5, PR 19 |

## Волна 6 - lifecycle и telemetry

| PR | Ветка | Commit | Scope | Зависимости |
|---|---|---|---|---|
| 24 | `feat/stoppable-lifecycle` | `feat(lifecycle): add stoppable state` | Добавить переход `DRAINING -> STOPPABLE` при нулевом количестве owned queries, согласовать REST/Connect ответы | PR 3, PR 5 |
| 25 | `feat/telemetry-pipeline` | `feat(telemetry): unify metrics and link execution spans` | Доменные метрики полностью реализованы на `prometheus/client_golang` и отдаются через `/metrics`, но поднятый OTel MeterProvider не получает ни одного инструмента. Решить, где источник истины, и убрать раздвоение. Execution span теряет родительский transport span из-за отделения execution context - это следствие I1, поэтому связывать через `trace.Link`, а не восстанавливать родителя | PR 5 |

## Волна 7 - интеграционные тесты и документация

| PR | Ветка | Commit | Scope | Зависимости |
|---|---|---|---|---|
| 26 | `chore/backend-integration-tests` | `chore(test): add backend integration coverage` | Сейчас e2e-тесты используют in-memory metastore, FS и fake driver. Добавить `testcontainers-go` тесты Redis, PostgreSQL, MySQL, ClickHouse и S3/MinIO, включая multi-node idempotency, cancellation и download. Подключить integration job к CI из PR 1 | PR 1, PR 3, PR 12, PR 13, PR 19, PR 21 |
| 27 | `docs/spec-code-alignment` | `docs(spec): reconcile interface and layout differences` | Согласовать interface sketches, layout, config loader и фактические контракты в `spec.md`, OpenAPI, proto и README. Разделы сливать по мере готовности соответствующих волн, не копить в один PR | По разделам |

## Порядок выполнения

1. Волна 0 - первым делом или параллельно с волной 1. Это правки на несколько
   строк, которые включают `errcheck`, а он ловит ровно тот класс ошибок, из
   которого состоят PR 4 и PR 5.
2. Волна 1 - падение процесса, молчаливая потеря результатов, нерабочий
   multi-node и обрезанное скачивание. Совокупно небольшой объём при наибольшем
   эффекте.
3. Волна 2 - публичный контракт. Сегодня любой сгенерированный по OpenAPI клиент
   не читает ответы корректно.
4. Волна 3 - отмена, идемпотентность, reload, GC.
5. Волна 4 - domain-контракт ошибок и работающий multi-node Watch.
6. Волна 5 - форматы и storage backends.
7. Волна 6 - lifecycle и телеметрия.
8. Волна 7 - реальная проверка backends и финальная синхронизация документации.

## Отличия от разбиения в spec-implementation-review.md

- **Правки конфигов линтеров вынесены в PR 0 и подняты в начало.** В исходном
  плане они были частью PR 24 и зависели от интеграционных тестов, то есть
  23 PR предлагалось вручную чинить `_ = err` при выключенном линтере.
- **Таймаут запроса разделён на два PR (6 и 18).** Снятие таймаута с download и
  sync-запросов ни от чего не зависит и относится к первой волне. Снятие с
  WebSocket корректно зависит от PR 17: без него зависшая подписка перестанет
  обрываться вообще.
- **PR по k8s instance ID и общему таймауту подняты в первую волну.** Первый
  чинит нерабочий multi-node правкой в несколько строк, второй отменяет
  минутный лимит на скачивание больших результатов - основной сценарий продукта.
- **Убрана зависимость REST/OpenAPI PR от рефактора кодов ошибок.** Косметический
  рефактор не должен стоять в критическом пути контрактной правки.
- **Добавлен PR 10** - временный явный отказ на `result_format: parquet`, чтобы
  до реализации encoder клиенты не получали JSONL под видом Parquet.
- **Расширен scope отмены (PR 11)** - `context.DeadlineExceeded` идёт по тому же
  пути, что и `context.Canceled`, и требует отдельного кода ошибки.
- **Переформулирован telemetry PR (PR 25).** Доменные метрики существуют, но на
  Prometheus; проблема не в их отсутствии, а в неиспользуемом параллельно
  поднятом OTLP meter provider.
- **Документационный PR перестал зависеть от всех остальных.** В исходном плане
  зависимость «PR 9 - PR 24» гарантировала, что он не сольётся никогда.
