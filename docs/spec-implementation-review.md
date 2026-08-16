# Ревью реализации относительно спецификации

Основной каркас реализован, но считать [`spec.md`](spec.md) полностью
выполненной пока нельзя. Есть несколько ошибок, нарушающих обязательные
инварианты и работу multi-node режима.

## Критические замечания

1. Возможна паника при уведомлении watchers. Канал закрывается при отмене
   контекста в [`manager.go`](../internal/core/manager/manager.go#L441), а
   `notifyWatchers` отпускает mutex до отправки в этот канал в
   [`manager.go`](../internal/core/manager/manager.go#L455). Отключение клиента
   одновременно с завершением запроса может привести к `send on closed channel`
   и падению процесса.

2. Запрос может стать `SUCCEEDED`, хотя результат не сохранён. Ошибка
   `writer.Close()` игнорируется в
   [`manager.go`](../internal/core/manager/manager.go#L308). Для S3 и ClickHouse
   именно `Close` ожидает завершения upload/commit и возвращает финальную ошибку,
   например в [`s3/storage.go`](../internal/storage/backends/s3/storage.go#L65).

3. Ошибки сохранения терминального состояния игнорируются в
   [`manager.go`](../internal/core/manager/manager.go#L343),
   [`manager.go`](../internal/core/manager/manager.go#L364) и
   [`manager.go`](../internal/core/manager/manager.go#L384). При временной
   недоступности Redis результат может быть записан, но запись останется
   `PENDING` или `RUNNING`, после чего запрос удалится из локального active
   registry. Это нарушает I2 и I4.

4. Kubernetes deployment создаёт два экземпляра с одинаковым ID. Значение
   `dbbridge-$(POD_NAME)` находится внутри ConfigMap в
   [`configmap.yaml`](../deploy/k8s/configmap.yaml#L8), где переменные окружения
   не подставляются. Переменная `POD_NAME` из
   [`deployment.yaml`](../deploy/k8s/deployment.yaml#L32) приложением не
   читается. В результате оба pod считают себя одним owner, а потеря одного pod
   не определяется через lease.

## Важные ошибки реализации

5. Hot reload не применяет обновления БД. Любой существующий pool
   переиспользуется только по ID, без сравнения DSN, engine или `max_conns`, в
   [`manager.go`](../internal/core/manager/manager.go#L83). При этом отчёт
   показывает database как `updated`. Ошибки открытия новых pool только
   логируются, поэтому reload всё равно возвращает успех. Удалённые pool
   закрываются сразу, без обещанного drain.

6. Отмена запроса во время `Pool.Exec` приводит к `FAILED`, а не `CANCELED`.
   Любая ошибка `Exec`, включая `context.Canceled`, без проверки превращается в
   `DB_EXEC_FAILED` в
   [`manager.go`](../internal/core/manager/manager.go#L269). Текущий тест отмены
   проверяет только фазу streaming и не покрывает этот путь.

7. Timeout и cancel не распространяются на storage upload. Для writer создаётся
   отдельный `context.Background()` в
   [`manager.go`](../internal/core/manager/manager.go#L294), а S3 дополнительно
   игнорирует переданный контекст в
   [`s3/storage.go`](../internal/storage/backends/s3/storage.go#L74). Зависший
   upload может не завершиться после `StopQuery` или timeout.

8. Идемпотентность не гарантируется на всём retention window:

   - ключ захватывается до `PutQuery`, поэтому ошибка записи метаданных оставляет
     ключ, указывающий на отсутствующий query ID, в
     [`manager.go`](../internal/core/manager/manager.go#L168);
   - TTL ключа начинается при submission, а retention результата считается от
     `FinishedAt`, например в
     [`memory.go`](../internal/state/memory.go#L178);
   - перед проверкой idempotency выполняется `Ping`, поэтому повторный запрос не
     вернёт существующий ID, если БД временно недоступна;
   - верхнее поле `QueryRecord.IdempotencyKey` никогда не заполняется, из-за чего
     cleanup этого ключа не работает по предусмотренному пути.

9. `WatchQuery` работает только внутри owner-процесса. Watchers хранятся в
   локальной map в [`manager.go`](../internal/core/manager/manager.go#L434), а
   Redis Pub/Sub используется только для `STOP_QUERY`. Подписка через другой
   instance не получает события. Кроме того, `Watch` не проверяет существование
   запроса и не отправляет его текущее состояние, поэтому подписка после
   завершения будет ждать бесконечно.

10. Parquet фактически не реализован. Формат `parquet` сериализуется как JSONL в
    [`encoder.go`](../internal/storage/encoder.go#L77). Клиент получит JSONL-файл
    с форматом `parquet`.

11. ClickHouse result storage написан, но не подключён в executable. В
    [`main.go`](../cmd/dbbridge/main.go#L70) регистрируются только FS и S3.
    `storage_backend: clickhouse` завершится ошибкой `unknown storage backend`
    уже после начала выполнения SQL.

12. REST не соответствует собственному OpenAPI:

    - доменные duration-поля сериализуются в наносекундах и имеют имена
      `timeout`, `result_ttl`, `db_exec_duration` в
      [`models.go`](../internal/core/domain/models.go#L50);
    - OpenAPI обещает `timeout_ms`, `result_ttl_seconds`,
      `db_exec_duration_ms` в
      [`dbbridge.yaml`](../api/openapi/dbbridge.yaml#L364).

    Сгенерированный по OpenAPI клиент не сможет корректно интерпретировать
    ответы.

13. В Connect незаполненные timestamps преобразуются через
    `time.Time{}.UnixNano()` в
    [`handler.go`](../internal/transport/grpcconnect/handler.go#L243). Для
    `PENDING` записи клиент получает отрицательные `started_at_ms`,
    `finished_at_ms` и `lease_deadline_ms` вместо нулей.

14. Общий `middleware.Timeout(60s)` применяется также к WebSocket,
    sync-запросам и скачиванию результатов в
    [`server.go`](../internal/transport/rest/server.go#L43). WebSocket наследует
    этот контекст в [`ws/server.go`](../internal/transport/ws/server.go#L46),
    поэтому соединение принудительно заканчивается примерно через минуту.

## Остальные пробелы спецификации

- `defaults.query_timeout` объявлен, но нигде не применяется.
- При отсутствии `mode` поведение async, но в сохранённых options остаётся
  пустая строка вместо `"async"`.
- Stats и progress не обновляются во время streaming. Значения записываются
  только при завершении.
- `ResultRef.Checksum` всегда пустой.
- GC удаляет metadata даже при ошибке удаления результата из storage в
  [`manager.go`](../internal/core/manager/manager.go#L570).
- Нет состояния `STOPPABLE`.
- Нет реальных integration-тестов Redis, S3, ClickHouse и драйверов. Текущие
  e2e-тесты прямо используют in-memory metastore, FS и fake driver в
  [`suite_test.go`](../test/e2e/suite_test.go#L1).
- Нет `testcontainers-go` и CI workflow.
- OTLP metrics практически не создаются, а execution span теряет родительский
  transport span из-за перехода на background context.
- Несколько interface/layout расхождений уже перечислены в
  [`spec-divergences.md`](spec-divergences.md), но этот список не покрывает
  ошибки выше.

## Статус обязательных инвариантов

| Инвариант | Статус |
|---|---|
| I1 - независимость от входящего соединения | Частично: DB execution отделён, но timeout/cancel storage работает неверно |
| I2 - чтение с любого instance | Частично: polling/download работают с Redis и S3, Watch и Kubernetes deployment не работают корректно |
| I3 - идемпотентность | Не гарантируется |
| I4 - однократная материализация | Не гарантируется из-за игнорирования `Close` и metadata errors |
| I5 - safe drain | Локальный active registry реализован, но multi-node ownership deployment сломан |

## Выполненные проверки

- `go test ./...` - успешно.
- `go test -race ./...` - успешно, но опасный watcher-сценарий тестами не
  покрыт.
- `go vet ./...` - успешно.
- `go build ./...`, `go mod verify`, `gofmt -l` - успешно.
- `golangci-lint run ./...` - не запускается: [`.golangci.yml`](../.golangci.yml)
  не содержит версию формата, необходимую для установленного golangci-lint
  2.12.2.
- `buf lint` - падает: package `dbbridge.v1` расположен в
  `api/proto/dbbridge/v1`, что не соответствует module root из
  [`buf.yaml`](../buf.yaml).

## Разбиение на отдельные PR

Каждый PR должен создаваться в отдельной ветке от актуальной основной ветки.
Зависимые PR следует открывать и сливать в указанном порядке, не объединяя их
изменения в одну ветку.

### Безопасность выполнения и обязательные инварианты

| PR | Ветка | Commit | Scope | Зависимости |
|---|---|---|---|---|
| 1 | `fix/query-watcher-race` | `fix(manager): prevent watcher send-close race` | Устранить гонку между `notifyWatchers` и закрытием канала, добавить regression-тест с отменой подписки во время уведомления | Нет |
| 2 | `fix/result-finalization` | `fix(manager): handle result writer close errors` | Проверять ошибку `writer.Close`, не переводить запрос в `SUCCEEDED` при неуспешном upload/commit, очищать частичный результат | Нет |
| 3 | `fix/query-state-persistence` | `fix(manager): handle query state persistence failures` | Перестать игнорировать ошибки сохранения `RUNNING` и терминальных состояний, не оставлять metadata в ложном состоянии | Нет |
| 4 | `fix/k8s-instance-id` | `fix(deploy): assign unique instance ids` | Передавать уникальный pod ID в конфигурацию приложения и проверить ownership двух реплик | Нет |
| 5 | `fix/database-pool-reload` | `fix(config): apply database pool changes on reload` | Пересоздавать изменённые pool, корректно обрабатывать добавление и удаление, возвращать ошибку при неприменённой конфигурации, не закрывать используемый pool до drain | Нет |
| 6 | `fix/query-cancellation-state` | `fix(manager): preserve canceled query state` | Распознавать `context.Canceled` во время `Pool.Exec` и сохранять `CANCELED`, покрыть отмену до получения `RowStream` | Нет |
| 7 | `fix/storage-cancellation` | `fix(storage): propagate query cancellation to writers` | Передавать execution context в storage writer и прекращать S3/ClickHouse upload после stop или timeout | PR 2, PR 6 |
| 8 | `fix/idempotency-retention` | `fix(state): align idempotency with result retention` | Исключить ключи без metadata, выровнять TTL с retention от `FinishedAt`, возвращать существующий query ID независимо от повторного `Ping`, заполнять `QueryRecord.IdempotencyKey` | PR 3 |

### Domain и подписки

| PR | Ветка | Commit | Scope | Зависимости |
|---|---|---|---|---|
| 9 | `refactor/query-error-codes` | `refactor(domain): define query error codes` | Заменить свободные строки кодов ошибок на ограниченный domain-тип и обновить transport mapping | Нет |
| 10 | `fix/watch-current-state` | `fix(manager): validate watches and emit current state` | Проверять существование query ID, сразу отправлять текущее состояние и завершать подписку после терминального состояния | PR 1 |
| 11 | `feat/distributed-query-watch` | `feat(state): publish query events across instances` | Доставлять query events через MetaStore, чтобы Watch работал на любом instance | PR 10 |

### Форматы, storage и API

| PR | Ветка | Commit | Scope | Зависимости |
|---|---|---|---|---|
| 12 | `feat/parquet-results` | `feat(storage): implement parquet result encoding` | Реализовать настоящий Parquet encoder и тест чтения созданного файла | PR 2 |
| 13 | `feat/clickhouse-result-storage` | `feat(storage): register clickhouse result backend` | Инициализировать и регистрировать ClickHouse ResultStore из config, добавить проверку конфигурации и lifecycle cleanup | PR 2, PR 7 |
| 14 | `fix/query-option-defaults` | `fix(manager): normalize query option defaults` | Сохранять `mode: async` при отсутствии mode и применять `defaults.query_timeout` | Нет |
| 15 | `fix/rest-openapi-contract` | `fix(rest): align responses with openapi schema` | Привести имена и единицы duration-полей REST-ответов к OpenAPI и добавить contract-тесты | PR 9, PR 14 |
| 16 | `fix/connect-zero-timestamps` | `fix(grpc): map absent timestamps to zero` | Возвращать ноль для незаполненных `StartedAt`, `FinishedAt` и `LeaseDeadline` | Нет |
| 17 | `fix/long-lived-transports` | `fix(rest): exempt streams from request timeout` | Не применять общий 60-секундный timeout к WebSocket, sync query и result download, сохранив timeout для обычных HTTP-операций | PR 10 |
| 18 | `feat/query-progress-stats` | `feat(manager): persist query progress statistics` | Обновлять rows, bytes и streaming progress до терминального состояния и публиковать progress events | PR 3, PR 11 |
| 19 | `feat/result-checksums` | `feat(storage): calculate result checksums` | Рассчитывать и сохранять `ResultRef.Checksum` при streaming без повторного чтения результата | PR 2 |
| 20 | `fix/gc-storage-cleanup` | `fix(manager): retain metadata when storage cleanup fails` | Не удалять metadata при ошибке удаления storage, сохранять возможность повторной очистки | PR 3 |

### Lifecycle и telemetry

| PR | Ветка | Commit | Scope | Зависимости |
|---|---|---|---|---|
| 21 | `feat/stoppable-lifecycle` | `feat(lifecycle): add stoppable state` | Добавить переход `DRAINING -> STOPPABLE` при нулевом количестве owned queries и согласовать REST/Connect ответы | PR 3, PR 4 |
| 22 | `feat/telemetry-pipeline` | `feat(telemetry): complete query metrics and tracing` | Добавить OTLP domain metrics и связать transport, service, manager, driver и storage spans при отделённом execution context | PR 3 |

### Тестирование, CI и документация

| PR | Ветка | Commit | Scope | Зависимости |
|---|---|---|---|---|
| 23 | `chore/backend-integration-tests` | `chore(test): add backend integration coverage` | Добавить testcontainers-тесты Redis, PostgreSQL, MySQL, ClickHouse и S3/MinIO, включая multi-node idempotency, cancellation и download | PR 4, PR 7, PR 8, PR 11, PR 13 |
| 24 | `chore/ci-quality-gates` | `chore(ci): enable lint proto and test checks` | Исправить формат `.golangci.yml`, устранить ошибку `buf lint`, добавить CI для unit, race, integration, vet, lint и proto lint | PR 23 |
| 25 | `docs/spec-code-alignment` | `docs(spec): reconcile interface and layout differences` | После функциональных исправлений согласовать interface sketches, layout, config loader и фактические контракты в `spec.md`, OpenAPI, proto и README | PR 9 - PR 24 |

### Порядок выполнения

1. Сначала PR 1 - PR 8, поскольку они закрывают падения процесса и нарушения
   I1 - I4.
2. Затем PR 9 - PR 11 для стабильного domain-контракта и multi-node Watch.
3. После этого PR 12 - PR 20 для форматов, storage и transport contracts.
4. PR 21 и PR 22 завершают lifecycle и telemetry.
5. PR 23 - PR 25 добавляют реальную проверку backends, обязательные quality
   gates и финальную синхронизацию документации.
