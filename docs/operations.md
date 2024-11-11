# Operations

## Health

| Check | Command | Passes when |
|---|---|---|
| API liveness | `media-engine healthcheck api` | `GET /api/v1/health` on the bind port returns 200 |
| API readiness | `GET /api/v1/ready` | PostgreSQL, Redis, and the bucket respond |
| Worker | `media-engine healthcheck worker` | PostgreSQL, Redis, and the bucket respond |

The image healthcheck calls the binary. It does not use curl. Metrics are served on `observability.metrics_bind` (`/metrics`), which is separate from the API listener. The default file bind is `127.0.0.1:9090`. Compose publishes API metrics on host `127.0.0.1:9090` and worker metrics on `127.0.0.1:9091`.

Prometheus names use the `media_engine_` prefix. Useful series include cache lookups, job claims, queue depth, and orphaned object deletes.

## Shutdown

Compose sets a stop grace period of 30 seconds for the API and 40 seconds for workers. The process cancels on `SIGTERM`, the HTTP server stops accepting connections, and workers stop taking new jobs. A job whose lease expires is requeued by the recovery loop, or failed permanently when the attempt budget is spent.

## Migrations

```bash
media-engine migrate
```

`MIGRATE_ON_START=true` runs migrations only when `MEDIA_ENGINE_ENVIRONMENT=development`. The same variable is ignored in staging and production. Apply migrations as a one-shot `migrate` task before rolling those environments.

## Backup

PostgreSQL is the system of record.

```bash
docker compose exec postgres pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc -f /tmp/media.dump
docker compose cp postgres:/tmp/media.dump ./media.dump
```

Restore into an empty database, then start the API only after `media-engine migrate` has reached the same schema version:

```bash
docker compose exec -T postgres pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists < media.dump
```

Redis holds caches, locks, rate windows, and the job stream. It is rebuildable. After a Redis loss, wait for in-flight leases to expire or restart workers so recovery republishes jobs that are still queued in PostgreSQL. Do not treat a Redis snapshot as required for correctness. AOF is enabled so a short outage does not drop an unacknowledged stream entry.

Object storage holds the bytes. Enable bucket versioning on the production bucket if you need to recover an overwrite. Do not add a lifecycle rule that expires objects in the `ready` status. Incomplete multipart uploads should be aborted after one day by a bucket lifecycle rule. The cleanup worker deletes `uploading` rows whose grace period has passed, and deletes the matching object.

An orphan object is one whose database row was removed after a failed completion. The cleanup loop lists stale uploading artifacts, deletes the object, then deletes the row. A crash between those two steps is retried on the next pass.

## Capacity

Worker concurrency and per-platform concurrency come from configuration. The global semaphore and seven platform semaphores cap in-flight downloads. The stream consumer reads one message at a time and does not ack a job that lost its lease. Duplicate deliveries ack once the claim reports that another worker owns the job.

API body size is bounded by the idempotency buffer (1 MiB) and the server request timeout. Download size is bounded by `limits.max_download_bytes` (default 2 GiB). yt-dlp receives `--max-filesize` with that value.

## Secrets

API keys are printed once by `issue-key` and stored as SHA-256. Database, Redis, S3, and Telegram credentials come from the environment. `Debug` output redacts `Secret` values. Do not put those values in the toml files or in the image.

## Staging and production

Set `MEDIA_ENGINE_ENVIRONMENT` to `staging` or `production` so the matching toml overlay is loaded. Point `DATABASE_URL`, `REDIS_URL`, and `S3_*` at the managed services. Leave `MIGRATE_ON_START` unset. Run one API replica for Telegram long-polling, or leave Telegram disabled and scale the API statelessly. Scale workers with the queue depth and `media_engine_queue_depth`.
