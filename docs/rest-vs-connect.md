# Why a separate REST API when Connect already speaks HTTP/JSON?

Honest answer: the REST layer **partially** overlaps with Connect's HTTP/JSON —
but not entirely, and the part that does *not* overlap is exactly what justifies
it.

Split the operations into two groups.

## 1. What cannot be done over Connect (this is what REST is really for)

**Result download with `Range` — the main argument.**
`GET /v1/queries/{id}/result` returns the **raw bytes of the result file**
(`text/csv`, `application/x-jsonlines`) and supports `Range` /
`Content-Range` / `206 Partial Content`. By contrast, `DownloadResult` in Connect
is a *server-streaming RPC*: even over the Connect protocol the streamed response
is **enveloped** (a 5-byte "flags + length" prefix before each
`DownloadResultResponse{chunk}` message). That is **not a file**: neither
`curl -o`, nor `curl -C -` (resume), nor a browser `<a download>`, nor a CDN, nor
a download manager can consume such a stream as a file — you would need a client
that strips the Connect envelope. Range / resumable download simply cannot be
expressed as an RPC stream. This can only be obtained through a dedicated HTTP
endpoint.

**WebSocket `/v1/ws`** is an HTTP upgrade handshake — it does not exist in
gRPC/Connect at all.

**`/healthz`, `/readyz`, `/metrics`** are not RPCs: Kubernetes probes and
Prometheus scrape with a plain `GET`, so they cannot live inside the proto
service.

→ Because of these alone, a **plain HTTP server is required regardless** of
Connect.

## 2. What can be done over Connect — where REST really is redundant

For the unary methods (`StartQuery`, `GetQueryStatus`, `StopQuery`,
`GetQueryStats`, `ListDatabases`, `ReloadConfig`, `CanIBeStopped`) Connect/JSON
fully covers the functionality — they are the same calls, invokable with `curl`.
Here the REST layer **essentially re-packages the same thing** for:

- resource-style URLs and HTTP verbs (`GET /v1/queries/{id}` instead of
  `POST /…/GetQueryStatus`),
- native status codes (`202`, `503`, `404`) and headers (`Idempotency-Key`),
- access without Connect tooling / stub generation, friendly to API gateways and
  caches.

This is convenience and idiomatic HTTP, but strictly speaking it **overlaps**. A
minimalist project could keep only download/health/ws on bare `net/http` and
expose the CRUD-style methods through Connect/JSON, writing no REST for them.

## Why the full REST API was written anyway

1. **An HTTP router is needed already** (download + health + metrics + ws), so
   adding resource-style REST routes to it is a **small increment**.
2. **The cost is low:** the REST layer is a thin adapter over the same
   `QueryService` (`internal/transport/rest`); no business logic is duplicated,
   only DTO↔domain mapping.
3. **REST is the lowest common denominator** for integrators: scripts, browsers,
   legacy clients, and load balancers work without knowing the Connect protocol.
4. **The spec (§7)** explicitly requires three transports as thin facades over a
   single core — a deliberate decision, not an accident.

## Bottom line

- Dropping a dedicated HTTP surface entirely is **not possible**: file download
  with `Range`, health/metrics, and WebSocket do not live in Connect.
- For the unary methods, REST **duplicates** Connect/JSON — the price of
  idiomatic HTTP and a zero-tooling entry point, but a cheap price (a thin
  adapter).

If the goal were to minimize code, REST could be kept only for `/result`,
`/healthz`, `/readyz`, `/metrics`, and `/ws`, with everything else served by
Connect/JSON. Making REST complete is a choice in favor of convenience and
spec conformance, not a technical necessity for every method.
