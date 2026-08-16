# Аудит соответствия спецификации

Дата аудита: 2026-08-16.

## Итог

На текущем `main` (`e2ecd80`) есть существенные отклонения от
[`docs/spec.md`](spec.md). Наиболее серьёзные затрагивают I3, I4 и I5,
распределённую работу через Redis и безопасность production-развёртывания.

## Критические проблемы

1. Redis может вернуть завершённый запрос в `RUNNING`. Heartbeat читает весь
   `QueryRecord`, меняет `LeaseDeadline` и сохраняет запись целиком в
   [`redis.go`](../internal/state/redis.go#L143). Если между чтением и записью
   запрос сохранит `SUCCEEDED` в
   [`manager.go`](../internal/core/manager/manager.go#L346), heartbeat перезапишет
   результат старой копией без `ResultRef`. Нужны CAS или атомарные обновления
   отдельных полей.

2. Запрос может получить `SUCCEEDED`, хотя результат не сохранён. Ошибка
   `writer.Close()` только логируется в
   [`manager.go`](../internal/core/manager/manager.go#L320). Для S3 именно `Close`
   ждёт окончания upload и возвращает его ошибку в
   [`storage.go`](../internal/storage/backends/s3/storage.go#L65). Это прямое
   нарушение I4.

3. Ошибки записи терминального состояния игнорируются. Сохранение `SUCCEEDED`,
   `FAILED` и `CANCELED` только логирует ошибку Redis в
   [`manager.go`](../internal/core/manager/manager.go#L357), после чего запрос
   удаляется из локального реестра. Metadata может навсегда остаться `RUNNING`,
   хотя результат уже записан.

4. I5 имеет drain-гонку. Проверка `IsDraining()` выполняется отдельно от
   регистрации запроса в
   [`service.go`](../internal/core/service/service.go#L38). Между этой проверкой
   и добавлением запроса в `activeReg` процесс может перейти в drain, увидеть
   ноль запросов в [`main.go`](../cmd/dbbridge/main.go#L169) и начать остановку.
   Сами query goroutines запускаются обычным `go` и не входят в `WaitGroup` в
   [`manager.go`](../internal/core/manager/manager.go#L231).

5. Kubernetes создаёт два pod с одинаковым owner ID. Строка
   `dbbridge-$(POD_NAME)` в
   [`configmap.yaml`](../deploy/k8s/configmap.yaml#L9) не интерполируется, а
   переменная из [`deployment.yaml`](../deploy/k8s/deployment.yaml#L32)
   приложением не читается. Lease, remote cancellation и owner-loss detection
   становятся недостоверными.

## Другие важные отклонения

- Идемпотентность I3 не гарантируется. `SET NX` выполняется до `PutQuery`,
  поэтому ошибка metadata оставляет ключ без записи запроса. TTL начинается при
  submission, а retention считается от `FinishedAt`. Повторный запрос также
  сначала делает `Ping`, поэтому при недоступной БД существующий ID не
  возвращается. Верхнее поле `QueryRecord.IdempotencyKey` не заполняется. См.
  [`manager.go`](../internal/core/manager/manager.go#L151).

- Hot reload сообщает об обновлении DB, но существующий pool переиспользуется
  только по ID, без учёта нового engine, DSN или `max_conns`, в
  [`manager.go`](../internal/core/manager/manager.go#L83). Ошибки создания pool
  скрываются, удалённые pool закрываются без общего механизма drain, а
  параллельные reload не сериализованы.

- Stop и timeout не распространяются на storage upload. Writer получает
  отдельный `context.Background()` в
  [`manager.go`](../internal/core/manager/manager.go#L306), S3 ещё раз заменяет
  его на background в
  [`storage.go`](../internal/storage/backends/s3/storage.go#L81). Отмена во время
  `Pool.Exec` сохраняется как `FAILED`, а не `CANCELED`.

- `WatchQuery` хранит подписки только локально в
  [`manager.go`](../internal/core/manager/manager.go#L454). Подписка через другой
  instance не получает события, не проверяет существование запроса, не
  отправляет текущее состояние и не завершается после terminal state.

- GC удаляет metadata даже после ошибки удаления результата из storage в
  [`manager.go`](../internal/core/manager/manager.go#L611). После этого повторить
  очистку невозможно.

- `parquet` фактически записывает JSONL в
  [`encoder.go`](../internal/storage/encoder.go#L77). ClickHouse ResultStore
  реализован, но не регистрируется в executable в
  [`main.go`](../cmd/dbbridge/main.go#L76).

- `defaults.query_timeout` не применяется, пустой mode сохраняется как `""`,
  значения options и enum не валидируются. Неверный storage или format
  обнаруживается только после выполнения SQL.

- REST-ответы используют доменные имена и наносекунды, тогда как OpenAPI обещает
  `timeout_ms`, `result_ttl_seconds` и duration в миллисекундах. Сравнение:
  [`models.go`](../internal/core/domain/models.go#L49) и
  [`dbbridge.yaml`](../api/openapi/dbbridge.yaml#L364).

- Connect преобразует нулевые timestamps через `UnixNano`, возвращая
  отрицательные значения в
  [`handler.go`](../internal/transport/grpcconnect/handler.go#L248).

- Общий HTTP timeout 60 секунд применяется к sync queries, WebSocket и
  скачиванию результатов в
  [`server.go`](../internal/transport/rest/server.go#L43).

- Не обновляются progress stats, `ResultRef.Checksum` всегда пустой, нет
  состояния `STOPPABLE`, tracing не является end-to-end, а реальные
  Redis/S3/DB integration-тесты отсутствуют.

## Проблемы безопасности

- API не имеет аутентификации или авторизации. Любой доступ к REST или Connect
  позволяет выполнять SQL, останавливать чужие запросы, читать результаты и
  вызывать reload. Спецификация не определяет модель авторизации, поэтому это
  пробел спецификации и production-блокер, а не только расхождение кода.

- Нет read-only policy, rate limit, ограничения числа активных запросов или
  размера JSON body. В сочетании с `query_timeout: 0` это позволяет создавать
  неограниченные DB и storage нагрузки.

- FS backend доверяет `ResultRef.Locator` и открывает или удаляет любой указанный
  путь в [`storage.go`](../internal/storage/backends/fs/storage.go#L51). При
  возможности записи в Redis можно подложить metadata и читать конфигурацию или
  удалять файлы процесса. Compose при этом публикует Redis port и не задаёт
  пароль в [`docker-compose.yaml`](../deploy/docker-compose.yaml#L2).

- Dev deployment делает весь MinIO bucket анонимно публичным в
  [`docker-compose.yaml`](../deploy/docker-compose.yaml#L45). Результаты SQL
  становятся доступны напрямую по известному object key.

- WebSocket отключает origin validation через `InsecureSkipVerify` в
  [`server.go`](../internal/transport/ws/server.go#L34).

- HTTP servers не задают `ReadHeaderTimeout` и не ограничивают body. Это
  оставляет возможности для Slowloris и memory/resource DoS.

- `govulncheck` подтвердил две достижимые уязвимости:

  - `google.golang.org/grpc v1.81.1`, GO-2026-6061, исправлено в `v1.82.1`.
    Официальный advisory включает HTTP/2 DoS и xDS RBAC проблемы. Публичный API
    использует Connect, а не grpc-go server, поэтому не все сценарии применимы
    напрямую, но уязвимые символы достижимы через OTLP.
    [Официальная запись](https://pkg.go.dev/vuln/GO-2026-6061).
  - `golang.org/x/text v0.38.0`, GO-2026-5970, исправлено в `v0.39.0`.
    Возможен бесконечный цикл на некорректном UTF-8; достижимый путь найден через
    pgx config.
    [Официальная запись](https://pkg.go.dev/vuln/GO-2026-5970).

## Статус инвариантов

| Инвариант | Статус |
|---|---|
| I1 - независимость от соединения | Частично: DB execution отделён, но storage cancellation работает неправильно |
| I2 - чтение с любого instance | Частично: polling/download возможны с Redis/S3, Watch и Kubernetes ownership нарушены |
| I3 - идемпотентность | Не гарантируется |
| I4 - однократная материализация | Не гарантируется |
| I5 - безопасный drain | Не гарантируется из-за admission race |

## Выполненные проверки

- `go test -race ./... -count=1` - успешно.
- `go vet ./...` - успешно.
- `golangci-lint run ./...` - успешно, `0 issues`.
- `buf lint` - успешно.
- `gofmt -l .` - изменений форматирования не требуется.
- `govulncheck ./...` - найдены две достижимые уязвимости зависимостей.

Race detector не видит найденные Redis lost-update сценарии, потому что тесты
используют in-memory store и fake drivers.
