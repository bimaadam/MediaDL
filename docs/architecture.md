# Architecture

The process is a modular monolith. Domain types do not import HTTP, Telegram, PostgreSQL, Redis, or S3. Adapters implement ports. `bootstrap` is the only place that wires concrete adapters into `Arc<dyn Port>`.

## Request path

1. The HTTP or Telegram adapter validates the caller and builds a `SubmitCommand`.
2. The URL guard allows only `http` and `https`, rejects credentials, blocks private and metadata addresses, and pins DNS. Shorteners are followed for at most five hops, and each hop is checked again.
3. `ProviderRegistry` maps the public URL to a platform and a canonical content id. Tracking parameters are stripped before the id is derived. The raw URL is not the cache key.
4. `MediaIdentity` is the platform, the content id, and a SHA-256 of the processing profile (kind, quality, container).
5. The engine checks the Telegram `file_id` for this bot, then the artifact cache, then PostgreSQL.
6. On a miss it takes a Redis lock (`SET NX PX`, owner-checked renew and release) and inserts one row into `active_media_jobs`. The primary key is `(media_id, profile_hash)`, so a second creator attaches to the existing job.
7. The insert and the `job.queued` outbox row commit together. The dispatcher publishes that event to the Redis stream. A crash between publish and the outbox mark can duplicate a message. Claiming the job is idempotent, so the duplicate is acknowledged.
8. A worker downloads with yt-dlp, or with the pinned HTTP client for a direct file. Arguments are a vector. User text is never passed to a shell. yt-dlp runs only for an allowlisted platform host.
9. FFmpeg remuxes or encodes when the profile requires it. The worker hashes the file while streaming, uploads it to object storage, then marks the job complete in one transaction. The upload happens before the status becomes `uploading`, so an upload failure can be retried.
10. Completion deletes the active-media row, writes the artifact, and enqueues a notification. Redis then stores the artifact reference. Cache values hold keys and metadata, not media bytes.

## Identity and delivery

A YouTube watch URL and a `youtu.be` URL for the same id and profile share one job. A different height or an audio profile is a different job.

Telegram `file_id` values are stored per bot and artifact. `file_unique_id` is identification only. An invalid `file_id` is dropped and the worker sends the object again when it is within Telegram's upload limit. Larger files are retrieved through `GET /api/v1/artifacts/{id}/content` with the caller's API key. Responses do not contain storage credentials or raw object URLs.

## Job states

`pending` and `queued` can move to `processing`. `processing` moves to `uploading` after a successful upload, or back to `queued` on a transient failure. `uploading` moves to `completed`, `failed`, or `queued` when a lease is abandoned. `completed`, `cancelled`, and `expired` are terminal. Only transient failures are retried, with exponential backoff and deterministic jitter, and never past `max_attempts`.

## Data ownership

PostgreSQL is authoritative for jobs and artifacts. Redis can be emptied. The next request rebuilds cache entries from PostgreSQL after it confirms the object still exists. A storage error is not treated as a missing object.

## Processes

`api` serves HTTP and, when enabled, one Telegram dispatcher. `worker` consumes the stream, renews job leases, recovers expired leases, deletes orphan uploads, and delivers Telegram files. `all` runs both in one process for a single host. Shutdown listens for Ctrl-C and `SIGTERM`, stops new work, and leaves in-flight leases to expire so recovery can requeue them.
