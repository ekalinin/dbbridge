# Sample PostgreSQL database (dvdrental)

The [dvdrental](https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-sample-database/)
demo database used as a target for manually testing dbbridge against a real
PostgreSQL.

## What's inside

- `dvdrental.sql` — plain SQL dump (schema + data).

The `postgres` service is defined in the main `deploy/docker-compose.yaml`.
It loads this dump automatically on first initialization (the file is mounted
into `/docker-entrypoint-initdb.d/`) and is wired into dbbridge via the
`dvdrental` entry in `deploy/configs/dbbridge-{blue,green}.yaml`.

## Quick start

```bash
# from the repository root — starts the whole stack, postgres included
make up
```

Check readiness via the healthcheck:

```bash
docker compose -f deploy/docker-compose.yaml ps
```

## Connection parameters

| Parameter | Value                                    |
|-----------|------------------------------------------|
| host      | `localhost` (host) / `postgres` (compose)|
| port      | `5432`                                    |
| database  | `dvdrental`                               |
| user      | `postgres`                                |
| password  | `postgres`                                |

Verify:

```bash
psql "postgres://postgres:postgres@localhost:5432/dvdrental" -c '\dt'
```

## Reset

The init script runs **only** when the `pgdata` volume is empty. To reload the
dump after changes, drop the volume:

```bash
docker compose -f deploy/docker-compose.yaml down -v
make up
```
