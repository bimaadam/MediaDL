//! Layered configuration. Files hold non-secret defaults. Credentials come from
//! the process environment and are wrapped so Debug output redacts them.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::application::{AppError, RuntimeConfig};
use crate::shared::Secret;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub environment: String,
    pub server: ServerSettings,
    pub worker: WorkerSettings,
    pub postgres: PostgresSettings,
    pub redis: RedisSettings,
    pub storage: StorageSettings,
    pub tools: ToolSettings,
    pub telegram: TelegramSettings,
    pub limits: LimitSettings,
    pub observability: ObservabilitySettings,
}

#[derive(Debug, Deserialize)]
pub struct ServerSettings {
    pub bind: String,
    pub request_timeout_secs: u64,
}

#[derive(Debug, Deserialize)]
pub struct WorkerSettings {
    pub concurrency: usize,
    pub per_platform_concurrency: usize,
    pub id: String,
}

#[derive(Debug, Deserialize)]
pub struct PostgresSettings {
    pub max_connections: u32,
    #[serde(default)]
    pub url: Secret,
}

#[derive(Debug, Deserialize)]
pub struct RedisSettings {
    #[serde(default)]
    pub url: Secret,
    pub stream: String,
    pub group: String,
    pub dead_stream: String,
}

#[derive(Debug, Deserialize)]
pub struct StorageSettings {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    #[serde(default)]
    pub access_key: Secret,
    #[serde(default)]
    pub secret_key: Secret,
}

#[derive(Debug, Deserialize)]
pub struct ToolSettings {
    pub ytdlp: PathBuf,
    pub ffmpeg: PathBuf,
    pub ffprobe: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct TelegramSettings {
    pub enabled: bool,
    pub bot_id: String,
    #[serde(default)]
    pub token: Secret,
}

#[derive(Debug, Deserialize)]
pub struct LimitSettings {
    pub max_download_bytes: u64,
    pub download_timeout_secs: u64,
    pub tool_timeout_secs: u64,
    pub lock_ttl_secs: u64,
    pub lease_secs: u64,
    pub lease_renew_secs: u64,
    pub artifact_cache_secs: u64,
    pub metadata_cache_secs: u64,
    pub telegram_file_secs: u64,
    pub max_attempts: u32,
    pub rate_limit_per_minute: u32,
    pub daily_quota: u32,
    pub telegram_max_upload_bytes: u64,
    pub job_expiry_secs: u64,
}

#[derive(Debug, Deserialize)]
pub struct ObservabilitySettings {
    pub metrics_bind: String,
}

impl Settings {
    pub fn load(config_dir: &Path) -> Result<Self, AppError> {
        let environment =
            std::env::var("MEDIA_ENGINE_ENVIRONMENT").unwrap_or_else(|_| "development".to_owned());
        let files = config::Config::builder()
            .add_source(config::File::from(config_dir.join("default.toml")).required(true))
            .add_source(
                config::File::from(config_dir.join(format!("{environment}.toml"))).required(false),
            )
            .add_source(
                config::Environment::with_prefix("MEDIA_ENGINE")
                    .prefix_separator("__")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .map_err(|error| AppError::Dependency(format!("config: {error}")))?;
        let mut settings: Settings = files
            .try_deserialize()
            .map_err(|error| AppError::Dependency(format!("config: {error}")))?;
        settings.environment = environment;
        overlay_secret("DATABASE_URL", &mut settings.postgres.url);
        overlay_secret("REDIS_URL", &mut settings.redis.url);
        overlay_secret("S3_ACCESS_KEY", &mut settings.storage.access_key);
        overlay_secret("S3_SECRET_KEY", &mut settings.storage.secret_key);
        overlay_secret("TELEGRAM_BOT_TOKEN", &mut settings.telegram.token);
        if let Ok(bot_id) = std::env::var("TELEGRAM_BOT_ID")
            && !bot_id.is_empty()
        {
            settings.telegram.bot_id = bot_id;
        }
        if !settings.telegram.token.is_empty() {
            settings.telegram.enabled = true;
        }
        if let Ok(endpoint) = std::env::var("S3_ENDPOINT")
            && !endpoint.is_empty()
        {
            settings.storage.endpoint = endpoint;
        }
        if let Ok(bucket) = std::env::var("S3_BUCKET")
            && !bucket.is_empty()
        {
            settings.storage.bucket = bucket;
        }
        if let Ok(region) = std::env::var("S3_REGION")
            && !region.is_empty()
        {
            settings.storage.region = region;
        }
        Ok(settings)
    }

    pub fn migrate_on_start(&self) -> bool {
        self.environment == "development" && env_flag("MIGRATE_ON_START")
    }

    pub fn runtime(&self) -> RuntimeConfig {
        RuntimeConfig {
            max_download_bytes: self.limits.max_download_bytes,
            download_timeout: Duration::from_secs(self.limits.download_timeout_secs),
            tool_timeout: Duration::from_secs(self.limits.tool_timeout_secs),
            lock_ttl: Duration::from_secs(self.limits.lock_ttl_secs),
            lease: Duration::from_secs(self.limits.lease_secs),
            lease_renew_every: Duration::from_secs(self.limits.lease_renew_secs),
            artifact_cache_ttl: Duration::from_secs(self.limits.artifact_cache_secs),
            metadata_cache_ttl: Duration::from_secs(self.limits.metadata_cache_secs),
            telegram_file_ttl: Duration::from_secs(self.limits.telegram_file_secs),
            max_attempts: self.limits.max_attempts,
            rate_limit_per_minute: self.limits.rate_limit_per_minute,
            daily_quota: self.limits.daily_quota,
            telegram_max_upload_bytes: self.limits.telegram_max_upload_bytes,
            job_expiry: Duration::from_secs(self.limits.job_expiry_secs),
            worker_concurrency: self.worker.concurrency,
            per_platform_concurrency: self.worker.per_platform_concurrency,
            ytdlp_path: self.tools.ytdlp.clone(),
            ffmpeg_path: self.tools.ffmpeg.clone(),
            ffprobe_path: self.tools.ffprobe.clone(),
            ..RuntimeConfig::default()
        }
    }

    pub fn worker_id(&self) -> String {
        if !self.worker.id.is_empty() {
            return self.worker.id.clone();
        }
        if let Ok(id) = std::env::var("MEDIA_ENGINE_WORKER_ID")
            && !id.is_empty()
        {
            return id;
        }
        format!("worker-{}", uuid::Uuid::now_v7())
    }
}

fn overlay_secret(name: &str, slot: &mut Secret) {
    if let Ok(value) = std::env::var(name)
        && !value.is_empty()
    {
        *slot = Secret::new(value);
    }
}

fn env_flag(name: &str) -> bool {
    matches!(std::env::var(name).as_deref(), Ok("1") | Ok("true") | Ok("TRUE"))
}
