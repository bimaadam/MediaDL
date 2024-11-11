use std::sync::Arc;

use crate::adapters::outbound::extractor::YtDlpExtractor;
use crate::adapters::outbound::ffmpeg::FfmpegProcessor;
use crate::adapters::outbound::guard::SafeHttp;
use crate::adapters::outbound::postgres::PostgresStore;
use crate::adapters::outbound::redis::RedisAdapter;
use crate::adapters::outbound::storage::S3Storage;
use crate::application::{
    AppError, Cache, Clock, Engine, Ids, Locks, MediaExtractor, MediaProcessor, MetadataSource,
    ObjectStorage, Pipeline, PlatformRegistry, RuntimeConfig, Store, SystemClock, UrlGuard,
    UuidIds,
};
use crate::infrastructure::config::Settings;

pub struct App {
    pub settings: Settings,
    pub engine: Arc<Engine>,
    pub store: Arc<PostgresStore>,
    pub redis: Arc<RedisAdapter>,
    pub storage: Arc<S3Storage>,
    pub pipeline: Arc<Pipeline>,
    pub config: Arc<RuntimeConfig>,
}

pub async fn build(settings: Settings) -> Result<App, AppError> {
    if settings.postgres.url.is_empty() {
        return Err(AppError::Dependency("DATABASE_URL is required".into()));
    }
    if settings.redis.url.is_empty() {
        return Err(AppError::Dependency("REDIS_URL is required".into()));
    }
    if settings.storage.access_key.is_empty() || settings.storage.secret_key.is_empty() {
        return Err(AppError::Dependency("S3_ACCESS_KEY and S3_SECRET_KEY are required".into()));
    }
    let store = Arc::new(
        PostgresStore::connect(settings.postgres.url.expose(), settings.postgres.max_connections)
            .await?,
    );
    if settings.migrate_on_start() {
        migrate(store.pool()).await?;
    }
    let redis = Arc::new(
        RedisAdapter::connect(
            settings.redis.url.expose(),
            &settings.redis.stream,
            &settings.redis.group,
            &settings.redis.dead_stream,
        )
        .await?,
    );
    let storage = Arc::new(S3Storage::new(
        &settings.storage.endpoint,
        &settings.storage.region,
        &settings.storage.bucket,
        settings.storage.access_key.expose(),
        settings.storage.secret_key.expose(),
    )?);
    storage.ensure_bucket().await?;
    let http = Arc::new(SafeHttp::new()?);
    let extractor = Arc::new(YtDlpExtractor::new(settings.tools.ytdlp.clone(), Arc::clone(&http)));
    let processor = Arc::new(FfmpegProcessor::new(
        settings.tools.ffmpeg.clone(),
        settings.tools.ffprobe.clone(),
    ));
    let config = Arc::new(settings.runtime());
    let registry = Arc::new(crate::adapters::outbound::extractor::ProviderRegistry);
    let engine = Arc::new(Engine::new(
        store.clone() as Arc<dyn Store>,
        redis.clone() as Arc<dyn Cache>,
        redis.clone() as Arc<dyn Locks>,
        http as Arc<dyn UrlGuard>,
        registry as Arc<dyn PlatformRegistry>,
        extractor.clone() as Arc<dyn MetadataSource>,
        redis.clone() as Arc<dyn crate::application::Admission>,
        storage.clone() as Arc<dyn ObjectStorage>,
        Arc::new(UuidIds) as Arc<dyn Ids>,
        Arc::new(SystemClock) as Arc<dyn Clock>,
        Arc::clone(&config),
    ));
    let pipeline = Arc::new(Pipeline::new(
        store.clone() as Arc<dyn Store>,
        redis.clone() as Arc<dyn Cache>,
        extractor as Arc<dyn MediaExtractor>,
        processor as Arc<dyn MediaProcessor>,
        storage.clone() as Arc<dyn ObjectStorage>,
        Arc::new(UuidIds),
        Arc::clone(&config),
    ));
    Ok(App { settings, engine, store, redis, storage, pipeline, config })
}

pub async fn migrate(pool: &sqlx::PgPool) -> Result<(), AppError> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .map_err(|error| AppError::Dependency(format!("migrate: {error}")))?;
    Ok(())
}

pub async fn issue_api_key(pool: &sqlx::PgPool, name: &str) -> Result<String, AppError> {
    let store = PostgresStore::connect_pool(pool.clone());
    let raw = format!("me_{}.{}", uuid::Uuid::now_v7().simple(), uuid::Uuid::now_v7().simple());
    let hash = crate::shared::sha256_hex(raw.as_bytes());
    store.create_api_user(name, &hash).await?;
    Ok(raw)
}
