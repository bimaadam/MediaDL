# Media engine

Production-oriented multi-platform media downloader. HTTP and Telegram are delivery adapters. They share one application core. The core deduplicates identical media across users and channels, then workers download, process, and store a single artifact.

## Legal boundary

The operator is responsible for copyright, platform terms, and the law that applies to each download.

This engine does not bypass DRM, logins, paywalls, or access controls. It does not accept cookies or account credentials. Private, age-gated, or otherwise restricted content is a permanent error. Only request media you are allowed to obtain.

## What it runs

| Piece | Role |
|---|---|
| `api` | Axum API, optional Telegram long-polling |
| `worker` | Queue consumer, recovery, cleanup, Telegram file delivery |
| PostgreSQL | Jobs, artifacts, outbox, users |
| Redis | Cache, locks, rate limits, job stream |
| S3-compatible storage | Artifact bytes. Point `S3_ENDPOINT` at Cloudflare R2 or another S3 API |
| yt-dlp and FFmpeg | Extraction and remux, invoked as argument vectors |

Scale `api` and `worker` independently. Run Telegram long-polling on one API process. Workers may send files; they do not poll Telegram.

## Local stack

```bash
cp .env.example .env
docker compose up --build
```

Compose fails if `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, or `S3_SECRET_KEY` are unset. Object storage is external. The old `minio/minio` image is no longer on Docker Hub. `MIGRATE_ON_START` applies only when `MEDIA_ENGINE_ENVIRONMENT=development`. Postgres is published on `127.0.0.1:15432` and Redis on `127.0.0.1:16379` so they do not collide with other local services.

Scale workers without adding API replicas:

```bash
docker compose up --scale worker=2
```

Issue an API key (printed once; only its SHA-256 is stored):

```bash
docker compose exec api media-engine issue-key "local operator"
```

Call the API with `Authorization: Bearer <key>`. OpenAPI is at `/swagger-ui`. Liveness is `GET /api/v1/health`. Readiness is `GET /api/v1/ready`. Metrics stay off the public router, on `127.0.0.1:9090` for the API and `127.0.0.1:9091` for the worker.

## Process roles

```text
media-engine api
media-engine worker
media-engine all
media-engine migrate
media-engine issue-key <name>
media-engine healthcheck api
media-engine healthcheck worker
```

Configuration is loaded from `config/default.toml`, then `config/<environment>.toml`, then `MEDIA_ENGINE__*` variables. Secrets are overlaid from `DATABASE_URL`, `REDIS_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_ENDPOINT`, `S3_BUCKET`, `S3_REGION`, `TELEGRAM_BOT_TOKEN`, and `TELEGRAM_BOT_ID`.

Environment files:

| File | Use |
|---|---|
| `config/development.toml` | Local compose |
| `config/staging.toml` | Staging limits |
| `config/production.toml` | Production limits |

## Tests

```bash
CARGO_BUILD_JOBS=2 cargo test
CARGO_BUILD_JOBS=2 cargo clippy --all-targets -- -D warnings
```

The deduplication tests prove that 100 concurrent requests for the same video create one job. `tests/live.rs` talks to PostgreSQL and Redis and is ignored by default:

```bash
DATABASE_URL=postgres://media:media-dev-password@127.0.0.1:15432/media \
REDIS_URL=redis://127.0.0.1:16379 \
cargo test --test live -- --ignored
```
