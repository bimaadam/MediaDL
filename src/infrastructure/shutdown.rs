use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

/// Cancel the token on Ctrl-C or SIGTERM. Teloxide's own ctrl-c handler stays off.
pub fn install() -> CancellationToken {
    let token = CancellationToken::new();
    let child = token.clone();
    tokio::spawn(async move {
        let mut terminate = signal(SignalKind::terminate()).ok();
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::warn!(%error, "ctrl-c handler failed");
                }
            }
            _ = async {
                if let Some(signal) = terminate.as_mut() {
                    signal.recv().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {}
        }
        tracing::info!("shutdown signal received");
        child.cancel();
    });
    token
}
