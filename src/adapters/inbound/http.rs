//! Versioned HTTP API. Handlers call the engine and never talk to a platform directly.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{FromRequestParts, Path, State};
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio_util::io::ReaderStream;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use uuid::Uuid;

use crate::application::{
    AppError, Engine, FormatSelection, MediaMetadata, SubmitCommand, SubmitOutcome,
};
use crate::domain::{
    Actor, Container, DeliveryChannel, MediaKind, PlatformKind, ProcessingProfile, Quality,
};
use crate::shared::sha256_hex;

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<Engine>,
}

pub fn router(state: AppState) -> Router {
    let (api_router, openapi) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(health))
        .routes(routes!(ready))
        .routes(routes!(platforms))
        .routes(routes!(resolve_media))
        .routes(routes!(download_media))
        .routes(routes!(get_media))
        .routes(routes!(get_job))
        .routes(routes!(cancel_job))
        .routes(routes!(artifact_content))
        .split_for_parts();
    let api_router = api_router.layer(axum::middleware::from_fn(attach_request_id));
    Router::new()
        .merge(api_router)
        .merge(
            utoipa_swagger_ui::SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", openapi),
        )
        .with_state(state)
}

#[derive(OpenApi)]
#[openapi(
    info(title = "Media Engine", version = "1.0.0", description = "Authenticated media resolve and download API. Object storage credentials are not exposed."),
    tags((name = "media", description = "Resolve and download"), (name = "system", description = "Health"))
)]
struct ApiDoc;

#[derive(Debug, Serialize, ToSchema)]
struct HealthBody {
    status: &'static str,
}

#[derive(Debug, Serialize, ToSchema)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Debug, Serialize, ToSchema)]
struct ErrorDetail {
    code: String,
    message: String,
    request_id: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct ResolveBody {
    url: String,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
struct DownloadBody {
    url: String,
    kind: String,
    quality: String,
    container: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct ResolveResponse {
    platform: String,
    content_id: String,
    canonical_url: String,
    metadata: MetadataBody,
}

#[derive(Debug, Serialize, ToSchema)]
struct MetadataBody {
    title: Option<String>,
    duration_seconds: Option<i32>,
    profiles: Vec<ProfileBody>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ProfileBody {
    kind: String,
    quality: String,
    container: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct DownloadResponse {
    state: String,
    media_id: Option<Uuid>,
    job_id: Option<Uuid>,
    artifact_id: Option<Uuid>,
    attached: bool,
}

#[derive(Debug, Serialize, ToSchema)]
struct JobResponse {
    id: Uuid,
    media_id: Uuid,
    platform: String,
    content_id: String,
    status: String,
    attempt_count: u32,
    failure_class: Option<String>,
    failure_reason: Option<String>,
    artifact_id: Option<Uuid>,
    progress: Option<ProgressBody>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ProgressBody {
    phase: String,
    percent: Option<u8>,
}

#[derive(Debug, Serialize, ToSchema)]
struct MediaResponse {
    id: Uuid,
    platform: String,
    content_id: String,
    title: Option<String>,
    duration_seconds: Option<i32>,
}

#[derive(Debug, Serialize, ToSchema)]
struct PlatformBody {
    code: String,
    display_name: String,
    enabled: bool,
}

#[derive(Clone)]
struct RequestId(String);

struct Failure {
    request_id: String,
    error: AppError,
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        error_response(
            self.error.http_status(),
            self.error.code(),
            &self.error.to_string(),
            &self.request_id,
        )
    }
}

fn error_response(status: u16, code: &str, message: &str, request_id: &str) -> Response {
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let body = ErrorBody {
        error: ErrorDetail {
            code: code.to_owned(),
            message: message.to_owned(),
            request_id: request_id.to_owned(),
        },
    };
    (status, Json(body)).into_response()
}

async fn attach_request_id(mut request: Request<Body>, next: axum::middleware::Next) -> Response {
    let incoming = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .map(str::to_owned);
    let id = incoming.unwrap_or_else(|| Uuid::now_v7().to_string());
    request.extensions_mut().insert(RequestId(id.clone()));
    let mut response = next.run(request).await;
    if let Ok(value) = axum::http::HeaderValue::from_str(&id) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

impl<S> FromRequestParts<S> for RequestId
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _: &S,
    ) -> Result<Self, Self::Rejection> {
        Ok(parts
            .extensions
            .get::<RequestId>()
            .cloned()
            .unwrap_or_else(|| RequestId(Uuid::now_v7().to_string())))
    }
}

struct AuthUser(Actor);

impl FromRequestParts<AppState> for AuthUser {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let request_id = parts
            .extensions
            .get::<RequestId>()
            .map(|id| id.0.clone())
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        let header = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        let Some(token) = header.filter(|token| !token.is_empty()) else {
            return Err(error_response(
                401,
                "unauthorized",
                "bearer token is required",
                &request_id,
            ));
        };
        match state.engine.authenticate_api_key(token).await {
            Ok(actor) => Ok(AuthUser(actor)),
            Err(error) => Err(error_response(
                error.http_status(),
                error.code(),
                &error.to_string(),
                &request_id,
            )),
        }
    }
}

#[utoipa::path(get, path = "/api/v1/health", tag = "system", responses((status = 200, body = HealthBody)))]
async fn health() -> Json<HealthBody> {
    Json(HealthBody { status: "ok" })
}

#[utoipa::path(get, path = "/api/v1/ready", tag = "system", responses((status = 200, body = HealthBody), (status = 503, body = ErrorBody)))]
async fn ready(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
) -> Result<Json<HealthBody>, Failure> {
    state.engine.ready().await.map_err(|error| Failure { request_id, error })?;
    Ok(Json(HealthBody { status: "ready" }))
}

#[utoipa::path(get, path = "/api/v1/platforms", tag = "media", responses((status = 200, body = [PlatformBody])))]
async fn platforms(
    State(state): State<AppState>,
    AuthUser(_actor): AuthUser,
    RequestId(request_id): RequestId,
) -> Result<Json<Vec<PlatformBody>>, Failure> {
    let platforms =
        state.engine.platforms().await.map_err(|error| Failure { request_id, error })?;
    Ok(Json(
        platforms
            .into_iter()
            .map(|platform| PlatformBody {
                code: platform.code,
                display_name: platform.display_name,
                enabled: platform.enabled,
            })
            .collect(),
    ))
}

#[utoipa::path(post, path = "/api/v1/media/resolve", tag = "media", request_body = ResolveBody, responses((status = 200, body = ResolveResponse)))]
async fn resolve_media(
    State(state): State<AppState>,
    AuthUser(actor): AuthUser,
    RequestId(request_id): RequestId,
    headers: HeaderMap,
    Json(body): Json<ResolveBody>,
) -> Result<Response, Failure> {
    let encoded = serde_json::to_vec(&serde_json::json!({"url": body.url})).unwrap_or_default();
    idempotent(&state, &actor, &headers, &request_id, &encoded, |state, actor| {
        let url = body.url.clone();
        async move {
            let view = state.engine.resolve(&actor, &url).await?;
            Ok(Json(resolve_response(
                view.platform,
                view.content_id,
                view.canonical_url,
                view.metadata,
            )))
        }
    })
    .await
}

#[utoipa::path(post, path = "/api/v1/media/download", tag = "media", request_body = DownloadBody, responses((status = 202, body = DownloadResponse), (status = 200, body = DownloadResponse)))]
async fn download_media(
    State(state): State<AppState>,
    AuthUser(actor): AuthUser,
    RequestId(request_id): RequestId,
    headers: HeaderMap,
    Json(body): Json<DownloadBody>,
) -> Result<Response, Failure> {
    let encoded = serde_json::to_vec(&body).unwrap_or_default();
    idempotent(&state, &actor, &headers, &request_id, &encoded, |state, actor| {
        let body = body_clone(&body);
        async move {
            let profile = parse_profile(&body.kind, &body.quality, &body.container)?;
            let outcome = state
                .engine
                .submit(SubmitCommand {
                    url: body.url,
                    profile,
                    actor,
                    channel: DeliveryChannel::Http,
                })
                .await?;
            let (status, response) = download_response(outcome);
            Ok((status, Json(response)))
        }
    })
    .await
}

fn body_clone(body: &DownloadBody) -> DownloadBody {
    DownloadBody {
        url: body.url.clone(),
        kind: body.kind.clone(),
        quality: body.quality.clone(),
        container: body.container.clone(),
    }
}

#[utoipa::path(get, path = "/api/v1/media/{id}", tag = "media", params(("id" = Uuid, Path, description = "Media id")), responses((status = 200, body = MediaResponse), (status = 404, body = ErrorBody)))]
async fn get_media(
    State(state): State<AppState>,
    AuthUser(actor): AuthUser,
    RequestId(request_id): RequestId,
    Path(id): Path<Uuid>,
) -> Result<Json<MediaResponse>, Failure> {
    let media = state
        .engine
        .media_for_user(actor.user_id, id)
        .await
        .map_err(|error| Failure { request_id, error })?;
    Ok(Json(MediaResponse {
        id: media.id,
        platform: media.platform.code().to_owned(),
        content_id: media.content_id,
        title: media.title,
        duration_seconds: media.duration_seconds,
    }))
}

#[utoipa::path(get, path = "/api/v1/jobs/{id}", tag = "media", params(("id" = Uuid, Path, description = "Job id")), responses((status = 200, body = JobResponse)))]
async fn get_job(
    State(state): State<AppState>,
    AuthUser(actor): AuthUser,
    RequestId(request_id): RequestId,
    Path(id): Path<Uuid>,
) -> Result<Json<JobResponse>, Failure> {
    let job = state
        .engine
        .job_for_user(actor.user_id, id)
        .await
        .map_err(|error| Failure { request_id: request_id.clone(), error })?;
    let progress = state
        .engine
        .progress(id)
        .await
        .map_err(|error| Failure { request_id, error })?
        .map(|progress| ProgressBody { phase: progress.phase, percent: progress.percent });
    Ok(Json(JobResponse {
        id: job.id,
        media_id: job.media_id,
        platform: job.identity.platform().code().to_owned(),
        content_id: job.identity.content_id().as_str().to_owned(),
        status: job.status.as_str().to_owned(),
        attempt_count: job.attempt_count,
        failure_class: job.failure_class.map(|class| class.as_str().to_owned()),
        failure_reason: job.failure_reason,
        artifact_id: job.artifact_id,
        progress,
    }))
}

#[utoipa::path(post, path = "/api/v1/jobs/{id}/cancel", tag = "media", params(("id" = Uuid, Path, description = "Job id")), responses((status = 200, body = JobResponse)))]
async fn cancel_job(
    State(state): State<AppState>,
    AuthUser(actor): AuthUser,
    RequestId(request_id): RequestId,
    Path(id): Path<Uuid>,
) -> Result<Json<JobResponse>, Failure> {
    let job = state
        .engine
        .cancel(actor.user_id, id)
        .await
        .map_err(|error| Failure { request_id, error })?;
    Ok(Json(JobResponse {
        id: job.id,
        media_id: job.media_id,
        platform: job.identity.platform().code().to_owned(),
        content_id: job.identity.content_id().as_str().to_owned(),
        status: job.status.as_str().to_owned(),
        attempt_count: job.attempt_count,
        failure_class: job.failure_class.map(|class| class.as_str().to_owned()),
        failure_reason: job.failure_reason,
        artifact_id: job.artifact_id,
        progress: None,
    }))
}

#[utoipa::path(get, path = "/api/v1/artifacts/{id}/content", tag = "media", params(("id" = Uuid, Path, description = "Artifact id")), responses((status = 200, description = "Authenticated media bytes")))]
async fn artifact_content(
    State(state): State<AppState>,
    AuthUser(actor): AuthUser,
    RequestId(request_id): RequestId,
    Path(id): Path<Uuid>,
) -> Result<Response, Failure> {
    let object = state
        .engine
        .open_artifact(actor.user_id, id)
        .await
        .map_err(|error| Failure { request_id, error })?;
    let stream = ReaderStream::new(object.reader);
    let mut response = Response::new(Body::from_stream(stream));
    if let Ok(content_type) = axum::http::HeaderValue::from_str(&object.content_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, content_type);
    }
    let filename = crate::application::upload_filename(&object.content_type);
    let disposition = format!("attachment; filename=\"{filename}\"");
    if let Ok(value) = axum::http::HeaderValue::from_str(&disposition) {
        response.headers_mut().insert(header::CONTENT_DISPOSITION, value);
    }
    if let Some(length) = object.content_length
        && let Ok(value) = axum::http::HeaderValue::from_str(&length.to_string())
    {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    Ok(response)
}

fn resolve_response(
    platform: PlatformKind,
    content_id: String,
    canonical_url: String,
    metadata: MediaMetadata,
) -> ResolveResponse {
    ResolveResponse {
        platform: platform.code().to_owned(),
        content_id,
        canonical_url,
        metadata: MetadataBody {
            title: metadata.title,
            duration_seconds: metadata.duration_seconds,
            profiles: metadata.profiles.into_iter().map(profile_body).collect(),
        },
    }
}

fn profile_body(profile: ProcessingProfile) -> ProfileBody {
    ProfileBody {
        kind: match profile.kind {
            MediaKind::Video => "video",
            MediaKind::Audio => "audio",
            MediaKind::Image => "image",
        }
        .to_owned(),
        quality: profile.quality_label(),
        container: profile.container.as_str().to_owned(),
    }
}

fn download_response(outcome: SubmitOutcome) -> (StatusCode, DownloadResponse) {
    match outcome {
        SubmitOutcome::Ready { media_id, artifact, .. } => (
            StatusCode::OK,
            DownloadResponse {
                state: "ready".into(),
                media_id: Some(media_id),
                job_id: None,
                artifact_id: Some(artifact.id),
                attached: false,
            },
        ),
        SubmitOutcome::Queued { job_id, media_id, attached, .. } => (
            StatusCode::ACCEPTED,
            DownloadResponse {
                state: if attached { "attached".into() } else { "queued".into() },
                media_id: Some(media_id),
                job_id: Some(job_id),
                artifact_id: None,
                attached,
            },
        ),
        SubmitOutcome::TelegramCached { .. } => (
            StatusCode::OK,
            DownloadResponse {
                state: "telegram_cached".into(),
                media_id: None,
                job_id: None,
                artifact_id: None,
                attached: false,
            },
        ),
    }
}

fn parse_profile(
    kind: &str,
    quality: &str,
    container: &str,
) -> Result<ProcessingProfile, AppError> {
    let kind = match kind {
        "video" => MediaKind::Video,
        "audio" => MediaKind::Audio,
        "image" => MediaKind::Image,
        _ => return Err(AppError::Validation("kind must be video, audio, or image".into())),
    };
    let quality = if quality == "best" {
        Quality::Best
    } else {
        let height = quality
            .parse::<u16>()
            .map_err(|_| AppError::Validation("quality must be best or a height".into()))?;
        Quality::Height(height)
    };
    let container = match container {
        "original" => Container::Original,
        "mp4" => Container::Mp4,
        "webm" => Container::Webm,
        "mkv" => Container::Mkv,
        "mp3" => Container::Mp3,
        "m4a" => Container::M4a,
        "jpeg" | "jpg" => Container::Jpeg,
        _ => return Err(AppError::Validation("unsupported container".into())),
    };
    let profile = ProcessingProfile { kind, quality, container };
    profile.validate()?;
    Ok(profile)
}

async fn idempotent<F, Fut, R>(
    state: &AppState,
    actor: &Actor,
    headers: &HeaderMap,
    request_id: &str,
    body: &[u8],
    action: F,
) -> Result<Response, Failure>
where
    F: FnOnce(AppState, Actor) -> Fut,
    Fut: std::future::Future<Output = Result<R, AppError>>,
    R: IntoResponse,
{
    let hash = sha256_hex(body);
    if let Some(key) = headers.get("idempotency-key").and_then(|value| value.to_str().ok()) {
        if key.is_empty() || key.len() > 200 {
            return Err(Failure {
                request_id: request_id.to_owned(),
                error: AppError::Validation("invalid idempotency key".into()),
            });
        }
        match state.engine.idempotent_hit(actor.user_id, key, &hash).await {
            Ok(Some(record)) => {
                let status = StatusCode::from_u16(u16::try_from(record.status).unwrap_or(200))
                    .unwrap_or(StatusCode::OK);
                return Ok((status, Json(record.body)).into_response());
            }
            Ok(None) => {}
            Err(error) => return Err(Failure { request_id: request_id.to_owned(), error }),
        }
        let response = action(state.clone(), actor.clone())
            .await
            .map_err(|error| Failure { request_id: request_id.to_owned(), error })?;
        let response = response.into_response();
        let status = response.status().as_u16() as i32;
        let bytes =
            axum::body::to_bytes(response.into_body(), 1024 * 1024).await.map_err(|error| {
                Failure {
                    request_id: request_id.to_owned(),
                    error: AppError::Dependency(error.to_string()),
                }
            })?;
        let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        let remember = state
            .engine
            .remember_response(crate::application::NewIdempotency {
                id: Uuid::now_v7(),
                user_id: actor.user_id,
                key: key.to_owned(),
                request_hash: hash,
                status,
                body: json.clone(),
                expires_at: chrono::Utc::now() + chrono::Duration::hours(24),
            })
            .await;
        if let Err(error) = remember {
            return Err(Failure { request_id: request_id.to_owned(), error });
        }
        let status =
            StatusCode::from_u16(u16::try_from(status).unwrap_or(200)).unwrap_or(StatusCode::OK);
        return Ok((status, Json(json)).into_response());
    }
    action(state.clone(), actor.clone())
        .await
        .map(IntoResponse::into_response)
        .map_err(|error| Failure { request_id: request_id.to_owned(), error })
}

pub fn selection_token() -> String {
    Uuid::now_v7().simple().to_string().chars().take(16).collect()
}

pub fn profile_from_parts(
    kind: &str,
    quality: &str,
    container: &str,
) -> Result<ProcessingProfile, AppError> {
    parse_profile(kind, quality, container)
}

pub async fn store_format_selection(
    engine: &Engine,
    token: &str,
    selection: &FormatSelection,
) -> Result<(), AppError> {
    engine.put_selection(token, selection).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{RuntimeConfig, Store, SystemClock, UuidIds};
    use crate::testkit::{
        AllowAll, FixedMetadata, MemoryCache, MemoryLocks, MemoryStore, PresentStorage, Registry,
        TestGuard,
    };
    use axum::body::Body;
    use tower::ServiceExt;

    fn test_state() -> (AppState, Arc<MemoryStore>) {
        let store = Arc::new(MemoryStore::new());
        let engine = Arc::new(Engine::new(
            store.clone(),
            Arc::new(MemoryCache::default()),
            Arc::new(MemoryLocks::default()),
            Arc::new(TestGuard),
            Arc::new(Registry),
            Arc::new(FixedMetadata),
            Arc::new(AllowAll),
            Arc::new(PresentStorage),
            Arc::new(UuidIds),
            Arc::new(SystemClock),
            Arc::new(RuntimeConfig::default()),
        ));
        (AppState { engine }, store)
    }

    #[tokio::test]
    async fn health_is_public_and_download_requires_a_key() {
        let (state, _) = test_state();
        let app = router(state);
        let health = app
            .clone()
            .oneshot(Request::builder().uri("/api/v1/health").body(Body::empty()).expect("request"))
            .await
            .expect("health");
        assert_eq!(health.status(), StatusCode::OK);
        let denied = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/media/download")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"url":"https://youtu.be/dQw4w9WgXcQ","kind":"video","quality":"720","container":"mp4"}"#))
                    .expect("request"),
            )
            .await
            .expect("download");
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn idempotency_conflict_on_a_different_body() {
        let (state, store) = test_state();
        let token = "local-test-token";
        store.create_api_user("tester", &sha256_hex(token.as_bytes())).await.expect("user");
        let app = router(state);
        let first = send_download(&app, token, "720", "same-key").await;
        assert_eq!(first, StatusCode::ACCEPTED);
        let second = send_download(&app, token, "360", "same-key").await;
        assert_eq!(second, StatusCode::CONFLICT);
    }

    async fn send_download(app: &Router, token: &str, quality: &str, key: &str) -> StatusCode {
        let body = format!(
            r#"{{"url":"https://youtu.be/dQw4w9WgXcQ","kind":"video","quality":"{quality}","container":"mp4"}}"#
        );
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/media/download")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .header("idempotency-key", key)
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("response")
            .status()
    }
}
