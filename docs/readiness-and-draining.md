# Readiness, liveness, and graceful draining

This document describes how `/healthz`, `/readyz`, and the draining lifecycle
work together, and why `/readyz` reflects the draining state.

## The two probes

| Endpoint | Probe kind | Returns | Meaning |
|---|---|---|---|
| `GET /healthz` | liveness | always `200 OK` | "the process is alive and the HTTP server responds" |
| `GET /readyz` | readiness | `200 READY` / `503 NOT READY` | "this node should / should not receive new traffic" |

In Kubernetes these map to `livenessProbe → /healthz` and
`readinessProbe → /readyz` (see `deploy/k8s/deployment.yaml`).

- **Liveness** answers "is the process broken?" A failure makes the orchestrator
  **restart** the pod. It must not depend on external systems (Redis, the target
  databases), otherwise a transient dependency outage would trigger a restart
  loop. Hence `/healthz` is intentionally trivial — it always returns `200`.
- **Readiness** answers "should traffic be routed here right now?" A failure makes
  the orchestrator **remove the pod from the Service endpoints** (stop sending
  traffic) without restarting it.

## New `/readyz` behavior

`/readyz` now returns **`503 NOT READY`** when the instance is draining (or when
the service is not wired), and `200 READY` otherwise:

```
serving   -> 200 READY
draining  -> 503 NOT READY   (LB removes this node from rotation)
```

Implemented as `QueryService.IsDraining()` (delegating to the lifecycle manager),
consumed by `handleReadyz` in `internal/transport/rest/server.go`.

### Why

Before this change, `/readyz` only checked that the service object was
constructed, so a **draining** node kept reporting ready and the load balancer
kept routing new requests to it. Those requests were then rejected by
`StartQuery` with `503` / `CodeUnavailable` — visible errors for clients instead
of a clean handoff.

By tying readiness to the draining state, the standard readiness mechanism does
the traffic gating for us: as soon as draining starts, the LB / Kubernetes takes
the node out of rotation and stops sending it new work, while in-flight queries
keep running to completion. No custom polling is required to stop new traffic.

Note the timing intent: `/readyz` flips to not-ready **immediately** when
draining begins (so new traffic stops right away). It does **not** wait for
in-flight queries to reach zero — those finish in the background.

## When does DRAINING happen

The instance enters `DRAINING` when the process receives **`SIGTERM` or `SIGINT`**
(graceful shutdown). `SIGHUP` reloads config and does **not** drain. See the
signal handler in `cmd/dbbridge/main.go`.

In practice `SIGTERM` is sent by Kubernetes on pod termination (rollout,
scale-down, eviction), by `docker stop`, or by the orchestrator during a
blue/green switch. `SIGINT` is `Ctrl+C` in local runs.

The lifecycle has two states, `SERVING` and `DRAINING`; the transition is
one-way (a draining instance is heading to shutdown). State is in-memory per
instance, so nodes drain independently.

## Full graceful-drain sequence

1. Orchestrator sends `SIGTERM`.
2. Instance sets `DRAINING`.
   - `/readyz` → `503` → LB stops routing **new** traffic to this node.
   - New `StartQuery` calls are rejected with `503` / `CodeUnavailable`
     (typed `domain.DrainingError`) as a safety net for anything still routed
     mid-transition.
3. In-flight **owned** queries keep executing (their context is decoupled from
   the request connection — invariant I1).
4. The process waits until `CountInFlight() == 0` (polled every second, with a
   30s deadline; on timeout it forces stop).
5. HTTP servers (REST + gRPC) shut down gracefully (10s timeout).

## Relationship to `/v1/admin/can-stop`

`/readyz` and `/v1/admin/can-stop` are complementary, not redundant:

- **`/readyz`** — traffic gating. Binary "route new requests here or not". Used
  by the LB / readiness probe.
- **`/v1/admin/can-stop`** — termination signal. Returns
  `{can_be_stopped, in_flight}` with `can_be_stopped == true` iff there are **0
  in-flight owned queries** (invariant I5). Used by the orchestrator to know when
  the node has fully quiesced and is safe to `SIGKILL` / move past in a
  blue/green rollout.

Typical flow: `SIGTERM` → `/readyz` = 503 (traffic drained away) → in-flight
finish → `/v1/admin/can-stop` reports `0 / true` → orchestrator terminates the
node. Readiness says "don't send work here"; can-stop says "safe to stop now".

## Operational note

The Kubernetes `readinessProbe` defaults to `periodSeconds: 10` and
`failureThreshold: 3`, so it can take up to ~30s to observe the not-ready state
and pull the pod from rotation. For faster traffic removal during blue/green,
lower `periodSeconds` / `failureThreshold` in `deploy/k8s/deployment.yaml`.
