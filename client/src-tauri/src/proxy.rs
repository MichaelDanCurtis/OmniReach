//! A loopback WebSocket proxy that attaches Cloudflare Access credentials.
//!
//! # Why this exists
//!
//! The webview's `WebSocket` API cannot set request headers, so it cannot
//! present `Cf-Access-Jwt-Assertion` or a service-token pair — meaning a
//! webview-originated socket can never authenticate through Cloudflare Access.
//!
//! The usual workaround is to pass the token as a URL query parameter. That is
//! rejected here: query strings land in proxy logs, server logs and browser
//! history, so it turns a credential into a logged artifact.
//!
//! Routing the stream through Tauri's IPC instead would also work, but IPC
//! serialises payloads as JSON — which would base64 every PTY chunk and every
//! screencast frame, the ~33% overhead the wire protocol was explicitly
//! designed to avoid.
//!
//! So: the webview opens a plain, fast, unauthenticated socket to
//! `127.0.0.1:<ephemeral>`, and this proxy dials the real host with the
//! credentials attached, pumping frames verbatim in both directions. Binary
//! stays binary, and the credential never enters a URL.
//!
//! The listener binds loopback only, so nothing off-machine can reach it.

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

/// How the client reaches its host, and what it presents to Cloudflare Access.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostConfig {
    /// Upstream websocket URL, e.g. `wss://omnireach.example.com/ws` or
    /// `ws://127.0.0.1:7717/ws` for a local host.
    pub url: String,

    /// Cloudflare Access **service token** id. Paired with `access_client_secret`.
    /// Validated at Cloudflare's edge, which then injects the JWT the host
    /// checks — so the client never handles the JWT itself.
    #[serde(default)]
    pub access_client_id: Option<String>,
    #[serde(default)]
    pub access_client_secret: Option<String>,

    /// A pre-obtained Access JWT, for the interactive-SSO path
    /// (`cloudflared access login`). Sent as `Cf-Access-Jwt-Assertion`.
    #[serde(default)]
    pub access_jwt: Option<String>,
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            url: "ws://127.0.0.1:7717/ws".to_string(),
            access_client_id: None,
            access_client_secret: None,
            access_jwt: None,
        }
    }
}

impl HostConfig {
    pub fn config_path() -> Option<PathBuf> {
        dirs::config_dir().map(|d| d.join("OmniReach").join("client.json"))
    }

    /// Load from disk, then let environment variables override. Absent config
    /// means a local host with no authentication, which is the dev default.
    pub fn load() -> Self {
        let mut cfg = Self::config_path()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|s| serde_json::from_str::<HostConfig>(&s).ok())
            .unwrap_or_default();

        if let Ok(url) = std::env::var("OMNIREACH_HOST_URL") {
            cfg.url = url;
        }
        if let Ok(v) = std::env::var("OMNIREACH_ACCESS_CLIENT_ID") {
            cfg.access_client_id = Some(v);
        }
        if let Ok(v) = std::env::var("OMNIREACH_ACCESS_CLIENT_SECRET") {
            cfg.access_client_secret = Some(v);
        }
        if let Ok(v) = std::env::var("OMNIREACH_ACCESS_JWT") {
            cfg.access_jwt = Some(v);
        }
        cfg
    }

    fn has_credentials(&self) -> bool {
        self.access_jwt.is_some()
            || (self.access_client_id.is_some() && self.access_client_secret.is_some())
    }
}

/// Bind a loopback listener and proxy every connection upstream.
/// Returns the port the webview should connect to.
pub async fn start(cfg: HostConfig) -> std::io::Result<u16> {
    // Port 0 = let the OS pick. Loopback only: this listener is deliberately
    // unauthenticated, so it must never be reachable off-machine.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    tracing_log(&format!(
        "proxy listening on 127.0.0.1:{port} -> {} (credentials: {})",
        cfg.url,
        if cfg.has_credentials() { "yes" } else { "none" }
    ));

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let cfg = cfg.clone();
            tokio::spawn(async move {
                if let Err(e) = bridge(stream, cfg).await {
                    tracing_log(&format!("proxy connection ended: {e}"));
                }
            });
        }
    });

    Ok(port)
}

/// Pump one accepted connection to the upstream host and back.
async fn bridge(stream: TcpStream, cfg: HostConfig) -> Result<(), String> {
    let downstream = tokio_tungstenite::accept_async(stream)
        .await
        .map_err(|e| format!("local handshake failed: {e}"))?;

    let mut request = cfg
        .url
        .as_str()
        .into_client_request()
        .map_err(|e| format!("bad upstream url: {e}"))?;

    {
        let headers = request.headers_mut();
        if let Some(jwt) = &cfg.access_jwt {
            headers.insert(
                "Cf-Access-Jwt-Assertion",
                jwt.parse().map_err(|_| "invalid jwt header value")?,
            );
        }
        if let (Some(id), Some(secret)) = (&cfg.access_client_id, &cfg.access_client_secret) {
            headers.insert(
                "CF-Access-Client-Id",
                id.parse().map_err(|_| "invalid client id header value")?,
            );
            headers.insert(
                "CF-Access-Client-Secret",
                secret
                    .parse()
                    .map_err(|_| "invalid client secret header value")?,
            );
        }
    }

    let (upstream, _) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| format!("upstream connect failed: {e}"))?;

    let (mut dn_tx, mut dn_rx) = downstream.split();
    let (mut up_tx, mut up_rx) = upstream.split();

    // Verbatim in both directions. Binary stays binary — no decoding, no
    // re-framing, no base64 — so the host's wire protocol is untouched.
    let to_upstream = async {
        while let Some(Ok(msg)) = dn_rx.next().await {
            if matches!(msg, Message::Close(_)) {
                let _ = up_tx.send(msg).await;
                break;
            }
            if up_tx.send(msg).await.is_err() {
                break;
            }
        }
    };

    let to_downstream = async {
        while let Some(Ok(msg)) = up_rx.next().await {
            if matches!(msg, Message::Close(_)) {
                let _ = dn_tx.send(msg).await;
                break;
            }
            if dn_tx.send(msg).await.is_err() {
                break;
            }
        }
    };

    // Either side closing tears down both.
    tokio::select! {
        _ = to_upstream => {}
        _ = to_downstream => {}
    }
    Ok(())
}

fn tracing_log(msg: &str) {
    eprintln!("[omnireach-proxy] {msg}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_a_local_unauthenticated_host() {
        let cfg = HostConfig::default();
        assert_eq!(cfg.url, "ws://127.0.0.1:7717/ws");
        assert!(
            !cfg.has_credentials(),
            "the default must not imply credentials that do not exist"
        );
    }

    #[test]
    fn a_service_token_pair_counts_as_credentials_but_half_a_pair_does_not() {
        let mut cfg = HostConfig::default();
        cfg.access_client_id = Some("id".into());
        assert!(
            !cfg.has_credentials(),
            "half a service token would fail at the edge; do not report it as usable"
        );
        cfg.access_client_secret = Some("secret".into());
        assert!(cfg.has_credentials());
    }

    #[test]
    fn a_jwt_alone_counts_as_credentials() {
        let mut cfg = HostConfig::default();
        cfg.access_jwt = Some("token".into());
        assert!(cfg.has_credentials());
    }

    #[test]
    fn parses_a_config_file() {
        let json = r#"{
            "url": "wss://omnireach.example.com/ws",
            "access_client_id": "abc.access",
            "access_client_secret": "shh"
        }"#;
        let cfg: HostConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.url, "wss://omnireach.example.com/ws");
        assert!(cfg.has_credentials());
        assert!(cfg.access_jwt.is_none());
    }

    /// The proxy must carry binary frames through untouched — the whole point
    /// of not routing them via Tauri IPC.
    #[tokio::test]
    async fn proxies_binary_and_text_verbatim() {
        use futures_util::{SinkExt, StreamExt};

        // A stand-in upstream that echoes whatever it receives, and records the
        // headers it was given.
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_port = upstream.local_addr().unwrap().port();
        let (seen_tx, seen_rx) = std::sync::mpsc::channel::<Option<String>>();

        tokio::spawn(async move {
            let (stream, _) = upstream.accept().await.unwrap();
            let mut header_value = None;
            let ws = tokio_tungstenite::accept_hdr_async(
                stream,
                |req: &tokio_tungstenite::tungstenite::handshake::server::Request, res| {
                    header_value = req
                        .headers()
                        .get("CF-Access-Client-Id")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    Ok(res)
                },
            )
            .await
            .unwrap();
            let _ = seen_tx.send(header_value);
            let (mut tx, mut rx) = ws.split();
            while let Some(Ok(msg)) = rx.next().await {
                if tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        let cfg = HostConfig {
            url: format!("ws://127.0.0.1:{up_port}/ws"),
            access_client_id: Some("test-id".into()),
            access_client_secret: Some("test-secret".into()),
            access_jwt: None,
        };
        let port = start(cfg).await.unwrap();

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws"))
            .await
            .unwrap();

        // Bytes that are NOT valid UTF-8: if anything in the path decoded or
        // re-encoded them, this would come back mangled.
        let payload: Vec<u8> = vec![0x00, 0xff, 0xfe, 0x80, 0xc3, 0x28];
        ws.send(Message::Binary(payload.clone().into()))
            .await
            .unwrap();
        match ws.next().await.unwrap().unwrap() {
            Message::Binary(b) => assert_eq!(b.to_vec(), payload, "binary must survive verbatim"),
            other => panic!("expected binary, got {other:?}"),
        }

        ws.send(Message::Text("hello".into())).await.unwrap();
        match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => assert_eq!(t.as_str(), "hello"),
            other => panic!("expected text, got {other:?}"),
        }

        assert_eq!(
            seen_rx.recv().unwrap().as_deref(),
            Some("test-id"),
            "the proxy must attach Access credentials the webview cannot send itself"
        );
    }
}
