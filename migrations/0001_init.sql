-- Authoritative job and artifact state. Redis is a cache and a queue, not the source of truth.
-- Status columns are text with checks so new states do not require an enum migration.

CREATE TABLE users (
    id uuid PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('telegram', 'api', 'system')),
    external_ref text NOT NULL,
    display_name text,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (kind, external_ref)
);

CREATE TABLE api_credentials (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users (id),
    key_hash text NOT NULL UNIQUE CHECK (key_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz
);

CREATE TABLE platforms (
    id smallint PRIMARY KEY,
    code text NOT NULL UNIQUE,
    display_name text NOT NULL,
    enabled boolean NOT NULL DEFAULT true
);

INSERT INTO platforms (id, code, display_name) VALUES
    (1, 'youtube', 'YouTube'),
    (2, 'tiktok', 'TikTok'),
    (3, 'facebook', 'Facebook'),
    (4, 'instagram', 'Instagram'),
    (5, 'x', 'X'),
    (6, 'reddit', 'Reddit'),
    (7, 'direct', 'Direct');

CREATE TABLE media (
    id uuid PRIMARY KEY,
    platform_id smallint NOT NULL REFERENCES platforms (id),
    canonical_content_id text NOT NULL CHECK (char_length(canonical_content_id) BETWEEN 1 AND 200),
    canonical_url text,
    title text,
    duration_seconds integer CHECK (duration_seconds IS NULL OR duration_seconds >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (platform_id, canonical_content_id)
);

CREATE TABLE media_formats (
    id uuid PRIMARY KEY,
    media_id uuid NOT NULL REFERENCES media (id),
    format_code text NOT NULL,
    kind text NOT NULL,
    quality text NOT NULL,
    container text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (media_id, format_code)
);

CREATE TABLE media_artifacts (
    id uuid PRIMARY KEY,
    media_id uuid NOT NULL REFERENCES media (id),
    profile_hash text NOT NULL CHECK (profile_hash ~ '^[0-9a-f]{64}$'),
    version integer NOT NULL CHECK (version > 0),
    object_key text NOT NULL CHECK (
        char_length(object_key) BETWEEN 1 AND 1023
        AND left(object_key, 1) <> '/'
        AND position('..' in object_key) = 0
    ),
    checksum_sha256 text NOT NULL CHECK (checksum_sha256 ~ '^[0-9a-f]{64}$'),
    byte_size bigint NOT NULL DEFAULT 0 CHECK (byte_size >= 0),
    content_type text NOT NULL,
    status text NOT NULL CHECK (status IN ('uploading', 'ready', 'deleted')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (media_id, profile_hash, version)
);

CREATE UNIQUE INDEX media_artifacts_one_ready
    ON media_artifacts (media_id, profile_hash)
    WHERE status = 'ready';

CREATE INDEX media_artifacts_uploading ON media_artifacts (created_at)
    WHERE status = 'uploading';

CREATE TABLE download_jobs (
    id uuid PRIMARY KEY,
    media_id uuid NOT NULL REFERENCES media (id),
    profile_hash text NOT NULL CHECK (profile_hash ~ '^[0-9a-f]{64}$'),
    profile jsonb NOT NULL,
    status text NOT NULL CHECK (status IN (
        'pending', 'queued', 'processing', 'uploading', 'completed', 'failed', 'cancelled', 'expired'
    )),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts > 0),
    failure_class text CHECK (failure_class IS NULL OR failure_class IN ('transient', 'permanent', 'restricted', 'cancelled')),
    failure_reason text,
    artifact_id uuid REFERENCES media_artifacts (id),
    requested_by uuid REFERENCES users (id),
    locked_by text,
    lease_until timestamptz,
    next_attempt_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX download_jobs_lease ON download_jobs (lease_until)
    WHERE lease_until IS NOT NULL AND status IN ('queued', 'processing', 'uploading');

CREATE INDEX download_jobs_requested_by ON download_jobs (requested_by);

-- One active extraction per media identity. Losers attach to the winner's job.
CREATE TABLE active_media_jobs (
    media_id uuid NOT NULL,
    profile_hash text NOT NULL CHECK (profile_hash ~ '^[0-9a-f]{64}$'),
    job_id uuid NOT NULL REFERENCES download_jobs (id) ON DELETE CASCADE,
    PRIMARY KEY (media_id, profile_hash)
);

CREATE TABLE job_subscriptions (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES download_jobs (id),
    user_id uuid NOT NULL REFERENCES users (id),
    channel text NOT NULL CHECK (channel IN ('http', 'telegram')),
    telegram_bot_id text,
    telegram_chat_id bigint NOT NULL DEFAULT 0,
    telegram_message_id integer,
    notify_status text NOT NULL DEFAULT 'pending' CHECK (notify_status IN ('pending', 'sent', 'failed', 'skipped')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (job_id, user_id, channel, telegram_chat_id)
);

CREATE INDEX job_subscriptions_user ON job_subscriptions (user_id);
CREATE INDEX job_subscriptions_pending ON job_subscriptions (notify_status) WHERE notify_status = 'pending';

CREATE TABLE telegram_deliveries (
    id uuid PRIMARY KEY,
    bot_id text NOT NULL,
    media_artifact_id uuid NOT NULL REFERENCES media_artifacts (id),
    file_id text NOT NULL,
    file_unique_id text,
    kind text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    invalidated_at timestamptz,
    UNIQUE (bot_id, media_artifact_id)
);

CREATE TABLE processing_attempts (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES download_jobs (id),
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    worker_id text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    outcome text,
    error text,
    UNIQUE (job_id, attempt_number)
);

CREATE TABLE idempotency_records (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users (id),
    idempotency_key text NOT NULL,
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    status integer NOT NULL,
    body jsonb NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, idempotency_key)
);

CREATE INDEX idempotency_expiry ON idempotency_records (expires_at);

CREATE TABLE audit_logs (
    id uuid PRIMARY KEY,
    actor_user_id uuid REFERENCES users (id),
    action text NOT NULL,
    target_type text NOT NULL,
    target_id text,
    request_id uuid,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX audit_logs_created ON audit_logs (created_at);

CREATE TABLE outbox_events (
    id bigserial PRIMARY KEY,
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX outbox_unpublished ON outbox_events (id) WHERE published_at IS NULL;

CREATE TABLE worker_heartbeats (
    worker_id text PRIMARY KEY,
    role text NOT NULL,
    last_seen_at timestamptz NOT NULL DEFAULT now()
);
