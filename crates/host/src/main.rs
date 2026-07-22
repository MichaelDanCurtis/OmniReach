use omnireach_host::{config::Config, sessions::SessionRegistry, transport};
use std::sync::Arc;
use std::time::Duration;

/// How often finished sessions are swept for eviction. Retention itself is an
/// hour (`DEFAULT_DEAD_RETENTION`); this only controls sweep granularity.
const REAP_INTERVAL: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "omnireach_host=info".into()),
        )
        .init();

    let config = Arc::new(Config::load()?);
    let registry = Arc::new(SessionRegistry::new(config.scrollback_bytes));

    let state = transport::AppState::new(Arc::clone(&registry), Arc::clone(&config));

    // `Authenticator` only exists under the `access-auth` feature, so both the
    // construction and the warning have to be gated.
    #[cfg(feature = "access-auth")]
    let state = {
        let auth = match &config.access {
            Some(a) => {
                tracing::info!("Cloudflare Access enabled for team {}", a.team_domain);
                Some(Arc::new(omnireach_host::auth::Authenticator::new(
                    a.clone(),
                )))
            }
            None => {
                tracing::warn!(
                    "Cloudflare Access is NOT configured — this host accepts unauthenticated \
                     websocket connections. Keep it bound to localhost."
                );
                None
            }
        };
        state.with_auth(auth)
    };

    #[cfg(not(feature = "access-auth"))]
    tracing::warn!(
        "built without the 'access-auth' feature — authentication is unavailable \
         and every websocket connection is accepted. Keep it bound to localhost."
    );

    // Sweep finished sessions. Retention is deliberately generous: a client
    // reconnecting long after a task finished must still be able to read it.
    {
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(REAP_INTERVAL);
            loop {
                tick.tick().await;
                registry.reap_dead(omnireach_host::sessions::DEFAULT_DEAD_RETENTION);
            }
        });
    }

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    tracing::info!("omnireach-host listening on {}", config.listen_addr);

    axum::serve(listener, transport::router(state)).await?;
    Ok(())
}
