use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::EnvFilter;

pub fn install_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = tracing_subscriber::fmt().with_env_filter(filter).json().finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

pub fn install_metrics() -> Option<PrometheusHandle> {
    match PrometheusBuilder::new().install_recorder() {
        Ok(handle) => Some(handle),
        Err(error) => {
            tracing::warn!(%error, "prometheus recorder is already installed");
            None
        }
    }
}

pub async fn serve_metrics(
    bind: &str,
    handle: PrometheusHandle,
    cancel: tokio_util::sync::CancellationToken,
) {
    use axum::Router;
    use axum::routing::get;

    let app = Router::new().route("/metrics", get(render)).with_state(handle);
    let listener = match tokio::net::TcpListener::bind(bind).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(%error, %bind, "metrics listener failed");
            return;
        }
    };
    let server =
        axum::serve(listener, app).with_graceful_shutdown(async move { cancel.cancelled().await });
    if let Err(error) = server.await {
        tracing::error!(%error, "metrics server stopped");
    }
}

async fn render(axum::extract::State(handle): axum::extract::State<PrometheusHandle>) -> String {
    handle.render()
}
