//! Process entry. `api` and `worker` scale separately. `all` runs both in one process.

use std::path::PathBuf;
use std::time::Duration;

use media_engine::adapters::inbound::http::{AppState, router};
use media_engine::adapters::inbound::telegram::{self, TeloxideSender};
use media_engine::adapters::outbound::postgres::PostgresStore;
use media_engine::adapters::outbound::redis::RedisAdapter;
use media_engine::adapters::outbound::storage::S3Storage;
use media_engine::application::{AppError, Cache, ObjectStorage, Store, TelegramSender};
use media_engine::bootstrap::{build, issue_api_key, migrate};
use media_engine::infrastructure::config::Settings;
use media_engine::infrastructure::observability::{
    install_metrics, install_tracing, serve_metrics,
};
use media_engine::infrastructure::shutdown;
use media_engine::workers::{self, WorkerSet};

#[tokio::main]
async fn main() {
    let code = match run().await {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("media-engine: {error}");
            1
        }
    };
    std::process::exit(code);
}

async fn run() -> Result<(), AppError> {
    let _ = dotenvy::dotenv();
    install_tracing();
    let mut args = std::env::args().skip(1);
    let role = args.next().unwrap_or_else(|| "all".to_owned());
    let settings = Settings::load(&config_dir())?;
    match role.as_str() {
        "migrate" => {
            let store = connect_postgres(&settings).await?;
            migrate(store.pool()).await
        }
        "issue-key" => {
            let name = args
                .next()
                .ok_or_else(|| AppError::Validation("issue-key requires a user name".into()))?;
            let store = connect_postgres(&settings).await?;
            let key = issue_api_key(store.pool(), &name).await?;
            println!("{key}");
            Ok(())
        }
        "healthcheck" => {
            let mode = args.next().unwrap_or_else(|| "api".to_owned());
            healthcheck(&settings, &mode).await
        }
        "api" => serve(settings, false).await,
        "worker" => serve(settings, true).await,
        "all" => serve_all(settings).await,
        other => Err(AppError::Validation(format!(
            "unknown role {other}; expected api, worker, all, migrate, issue-key, or healthcheck"
        ))),
    }
}

fn config_dir() -> PathBuf {
    std::env::var_os("MEDIA_ENGINE_CONFIG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config"))
}

async fn connect_postgres(settings: &Settings) -> Result<PostgresStore, AppError> {
    if settings.postgres.url.is_empty() {
        return Err(AppError::Dependency("DATABASE_URL is required".into()));
    }
    PostgresStore::connect(settings.postgres.url.expose(), 1).await
}

async fn healthcheck(settings: &Settings, mode: &str) -> Result<(), AppError> {
    match mode {
        "api" => {
            let url = loopback_health_url(&settings.server.bind)?;
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .map_err(|error| AppError::Dependency(format!("health client: {error}")))?;
            let response = client
                .get(url)
                .send()
                .await
                .map_err(|error| AppError::Dependency(format!("health request: {error}")))?;
            if response.status().is_success() {
                Ok(())
            } else {
                Err(AppError::Dependency(format!("health status {}", response.status())))
            }
        }
        "worker" => {
            let store = connect_postgres(settings).await?;
            store.ping().await?;
            if settings.redis.url.is_empty() {
                return Err(AppError::Dependency("REDIS_URL is required".into()));
            }
            let redis = RedisAdapter::connect(
                settings.redis.url.expose(),
                &settings.redis.stream,
                &settings.redis.group,
                &settings.redis.dead_stream,
            )
            .await?;
            Cache::ping(&redis).await?;
            if settings.storage.access_key.is_empty() || settings.storage.secret_key.is_empty() {
                return Err(AppError::Dependency(
                    "S3_ACCESS_KEY and S3_SECRET_KEY are required".into(),
                ));
            }
            let storage = S3Storage::new(
                &settings.storage.endpoint,
                &settings.storage.region,
                &settings.storage.bucket,
                settings.storage.access_key.expose(),
                settings.storage.secret_key.expose(),
            )?;
            ObjectStorage::ping(&storage).await
        }
        other => Err(AppError::Validation(format!("unknown healthcheck mode {other}"))),
    }
}

fn loopback_health_url(bind: &str) -> Result<String, AppError> {
    let port = bind.rsplit(':').next().unwrap_or("");
    let port: u16 = port
        .parse()
        .map_err(|_| AppError::Dependency(format!("server.bind has no port: {bind}")))?;
    Ok(format!("http://127.0.0.1:{port}/api/v1/health"))
}

async fn serve(settings: Settings, worker_only: bool) -> Result<(), AppError> {
    let app = build(settings).await?;
    let cancel = shutdown::install();
    spawn_metrics(&app.settings.observability.metrics_bind, cancel.clone());
    if worker_only {
        let sender = telegram_sender(&app.settings)?;
        workers::run(worker_set(&app, sender), cancel).await;
        return Ok(());
    }
    if app.settings.telegram.enabled {
        let token = app.settings.telegram.token.expose().to_owned();
        let bot_id = require_bot_id(&app.settings)?;
        let engine = std::sync::Arc::clone(&app.engine);
        let child = cancel.clone();
        tokio::spawn(async move { telegram::run(&token, bot_id, engine, child).await });
    }
    let state = AppState { engine: std::sync::Arc::clone(&app.engine) };
    let listener =
        tokio::net::TcpListener::bind(&app.settings.server.bind).await.map_err(|error| {
            AppError::Dependency(format!("bind {}: {error}", app.settings.server.bind))
        })?;
    let timeout = Duration::from_secs(app.settings.server.request_timeout_secs);
    let api = router(state).layer(timeout_layer(timeout));
    axum::serve(listener, api)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await
        .map_err(|error| AppError::Dependency(format!("http server: {error}")))
}

fn timeout_layer(timeout: Duration) -> tower_http::timeout::TimeoutLayer {
    tower_http::timeout::TimeoutLayer::with_status_code(http::StatusCode::REQUEST_TIMEOUT, timeout)
}

async fn serve_all(settings: Settings) -> Result<(), AppError> {
    let app = build(settings).await?;
    let cancel = shutdown::install();
    spawn_metrics(&app.settings.observability.metrics_bind, cancel.clone());
    let sender = telegram_sender(&app.settings)?;
    let workers = workers::run(worker_set(&app, sender), cancel.clone());
    let http = serve_http(app, cancel);
    let (_, http_result) = tokio::join!(workers, http);
    http_result
}

async fn serve_http(
    app: media_engine::bootstrap::App,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), AppError> {
    if app.settings.telegram.enabled {
        let token = app.settings.telegram.token.expose().to_owned();
        let bot_id = require_bot_id(&app.settings)?;
        let engine = std::sync::Arc::clone(&app.engine);
        let child = cancel.clone();
        tokio::spawn(async move { telegram::run(&token, bot_id, engine, child).await });
    }
    let state = AppState { engine: std::sync::Arc::clone(&app.engine) };
    let listener =
        tokio::net::TcpListener::bind(&app.settings.server.bind).await.map_err(|error| {
            AppError::Dependency(format!("bind {}: {error}", app.settings.server.bind))
        })?;
    let timeout = Duration::from_secs(app.settings.server.request_timeout_secs);
    let api = router(state).layer(timeout_layer(timeout));
    axum::serve(listener, api)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await
        .map_err(|error| AppError::Dependency(format!("http server: {error}")))
}

fn worker_set(
    app: &media_engine::bootstrap::App,
    sender: Option<std::sync::Arc<dyn TelegramSender>>,
) -> WorkerSet {
    WorkerSet {
        store: app.store.clone(),
        queue: app.redis.clone(),
        cache: app.redis.clone(),
        pipeline: std::sync::Arc::clone(&app.pipeline),
        storage: app.storage.clone(),
        sender,
        config: std::sync::Arc::clone(&app.config),
        worker_id: app.settings.worker_id(),
    }
}

fn telegram_sender(
    settings: &Settings,
) -> Result<Option<std::sync::Arc<dyn TelegramSender>>, AppError> {
    if !settings.telegram.enabled {
        return Ok(None);
    }
    if settings.telegram.token.is_empty() {
        return Err(AppError::Dependency(
            "TELEGRAM_BOT_TOKEN is required when telegram is enabled".into(),
        ));
    }
    let _ = require_bot_id(settings)?;
    Ok(Some(std::sync::Arc::new(TeloxideSender::new(settings.telegram.token.expose()))))
}

fn require_bot_id(settings: &Settings) -> Result<String, AppError> {
    if settings.telegram.bot_id.is_empty() {
        return Err(AppError::Dependency(
            "TELEGRAM_BOT_ID is required when telegram is enabled".into(),
        ));
    }
    Ok(settings.telegram.bot_id.clone())
}

fn spawn_metrics(bind: &str, cancel: tokio_util::sync::CancellationToken) {
    let Some(handle) = install_metrics() else {
        return;
    };
    let bind = bind.to_owned();
    tokio::spawn(async move { serve_metrics(&bind, handle, cancel).await });
}
