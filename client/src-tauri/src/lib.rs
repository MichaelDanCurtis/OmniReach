mod proxy;

use std::sync::OnceLock;

/// The loopback port the webview connects to, and the runtime owning the proxy.
static WS_PORT: OnceLock<u16> = OnceLock::new();
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

/// The websocket URL the frontend should use.
///
/// Always loopback: the Rust core proxies to the real host and attaches
/// Cloudflare Access credentials, which the webview's WebSocket API cannot do
/// itself. See `proxy.rs` for why this indirection exists.
///
/// Errors rather than falling back to a direct connection. A silent fallback
/// would drop the credentials and then fail at Cloudflare's edge with a
/// confusing 401, instead of naming the actual problem here.
#[tauri::command]
fn ws_url() -> Result<String, String> {
    match WS_PORT.get() {
        Some(port) => Ok(format!("ws://127.0.0.1:{port}/ws")),
        None => Err("the local proxy is not listening; check the client log".to_string()),
    }
}

/// Where the client reads its host configuration from, for the UI to show.
#[tauri::command]
fn config_path() -> Option<String> {
    proxy::HostConfig::config_path().map(|p| p.to_string_lossy().to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let cfg = proxy::HostConfig::load();

    // The runtime must outlive `run`, since it owns the proxy's spawned tasks.
    let runtime = RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio runtime"));

    // Bind before the webview loads so `ws_url` is answerable on its first call.
    match runtime.block_on(proxy::start(cfg)) {
        Ok(port) => {
            let _ = WS_PORT.set(port);
        }
        Err(e) => eprintln!("[omnireach] proxy failed to bind: {e}"),
    }

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![ws_url, config_path])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
