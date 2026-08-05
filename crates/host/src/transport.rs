//! The multiplexed WebSocket transport.
//!
//! One socket per client carries everything:
//!
//! * **text frames** are JSON control messages — [`ControlRequest`] in,
//!   [`ControlResponse`] and [`ControlEvent`] out;
//! * **binary frames** are [`BinaryFrame`]-framed bulk PTY bytes.
//!
//! PTY bytes never touch the text channel and are never decoded, validated or
//! base64-encoded anywhere on this path. A multi-byte sequence split across two
//! reads would be corrupted irrecoverably by a single `from_utf8_lossy`, so the
//! host stays byte-transparent end to end and lets xterm.js reassemble.
//!
//! # Backpressure
//!
//! A PTY can produce output far faster than a laptop on hotel wifi can accept
//! it. Each attachment therefore keeps a [`SendQueue`]: the pump adds a frame's
//! payload length before handing it to the writer task, and the writer subtracts
//! it once the frame has actually been pushed into the socket (or discarded).
//! That counter — bytes *accepted but not yet written* — is the only honest
//! measure of how far behind a client is, and it is what
//! [`SEND_QUEUE_HIGH_WATER`] is compared against.
//!
//! Crossing the high-water mark bumps the attachment's epoch, which makes the
//! writer drop every frame it has already queued for that session instead of
//! spending socket time on output the client will never catch up with. The pump
//! then goes quiet until the backlog drains below [`SEND_QUEUE_LOW_WATER`],
//! emits `ControlEvent::PtyResync { from_seq }`, sends the ring tail, and
//! resumes. The hysteresis matters: resyncing on every chunk while the queue is
//! still full would replace one unbounded queue with an unbounded stream of
//! resync events.

use crate::config::Config;
use crate::pty::{PtySession, PtySpec};
use crate::sessions::SessionRegistry;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use omnireach_protocol::control::{
    ControlEvent, ControlRequest, ControlResponse, PtyAttachParams, PtyCreateParams,
    PtyResizeParams,
};
use omnireach_protocol::{BinaryFrame, FrameKind};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use uuid::Uuid;

#[cfg(feature = "access-auth")]
use crate::auth::Authenticator;

/// Payload bytes a single attachment may have queued for the socket before the
/// host gives up on delivering them in order and fast-forwards the client.
const SEND_QUEUE_HIGH_WATER: usize = 4 * 1024 * 1024;

/// Resume streaming once the backlog has drained this far. Without a low-water
/// mark the pump would resync on every wake-up while the queue stayed full.
const SEND_QUEUE_LOW_WATER: usize = SEND_QUEUE_HIGH_WATER / 4;

/// Message slots between the pumps and the writer task. A second, coarser bound
/// on the same problem: 1024 pending messages also means the client is behind.
const OUTBOUND_CHANNEL_DEPTH: usize = 1024;

/// Once this many payload bytes are already waiting for the socket, a pump stops
/// forwarding on every broadcast chunk and lets [`PUMP_TICK`] drive instead, so
/// `read_since` coalesces the backlog into one large frame.
///
/// Two reasons. A client that is behind wants fewer, larger frames, not
/// thousands of 300-byte ones. And without this the queue fills with tiny
/// messages and the *message count* becomes the binding limit, which would make
/// the byte budget below decorative — the exact failure this design exists to
/// avoid. Below the threshold every chunk is forwarded the moment it arrives, so
/// interactive echo latency is untouched.
const COALESCE_ABOVE: usize = 64 * 1024;

/// How often a pump wakes up even when the broadcast is quiet, to notice a dead
/// child and to re-check whether a throttled queue has drained.
const PUMP_TICK: Duration = Duration::from_millis(100);

/// Consecutive quiet ticks after `is_alive()` goes false before `pty.exit` is
/// announced. The child is reaped on a different thread from the one draining
/// the pty, so the last bytes can land after the process is gone.
const DEAD_TICKS_BEFORE_EXIT: u32 = 3;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<SessionRegistry>,
    pub config: Arc<Config>,
    pub fs: Arc<crate::fs::FsService>,
    /// The remote browser, launched lazily: Chrome is expensive and most
    /// sessions never open it. `None` until the first `browser.launch`.
    #[cfg(feature = "remote-browser")]
    pub browser: Arc<tokio::sync::Mutex<Option<Arc<crate::browser::BrowserService>>>>,
    /// Cloudflare Access validator, or `None` for an unauthenticated host.
    ///
    /// Gated on the same feature as [`crate::auth`]: that module does not exist
    /// under `--no-default-features`, so a field naming its types cannot either.
    /// Build the state with [`AppState::new`] and [`AppState::with_auth`] rather
    /// than a struct literal to keep call sites feature-agnostic.
    #[cfg(feature = "access-auth")]
    pub auth: Option<Arc<Authenticator>>,
}

impl AppState {
    /// State with authentication disabled. Compiles with and without the
    /// `access-auth` feature.
    pub fn new(registry: Arc<SessionRegistry>, config: Arc<Config>) -> Self {
        Self {
            registry,
            config,
            fs: Arc::new(crate::fs::FsService::new()),
            #[cfg(feature = "remote-browser")]
            browser: Arc::new(tokio::sync::Mutex::new(None)),
            #[cfg(feature = "access-auth")]
            auth: None,
        }
    }

    #[cfg(feature = "access-auth")]
    #[must_use]
    pub fn with_auth(mut self, auth: Option<Arc<Authenticator>>) -> Self {
        self.auth = auth;
        self
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        // axum 0.8 spells path parameters `/{name}`; the 0.7 `/:name` form
        // panics at router construction. No parameters here, but keep it in
        // mind before adding one.
        .route("/health", get(health))
        .route("/ws", get(ws_upgrade))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        r#"{"status":"ok"}"#,
    )
}

async fn ws_upgrade(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    #[cfg(feature = "access-auth")]
    if let Some(auth) = &state.auth {
        let token = headers
            .get("cf-access-jwt-assertion")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if let Err(e) = auth.ensure_and_validate(token).await {
            tracing::warn!("rejected websocket upgrade: {e}");
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }
    #[cfg(not(feature = "access-auth"))]
    let _ = &headers;

    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Per-attachment outbound accounting, shared between its pump and the writer.
#[derive(Debug, Default)]
struct SendQueue {
    /// Payload bytes handed to the writer but not yet written to the socket.
    queued: AtomicUsize,
    /// Bumped on every resync. Frames stamped with an older epoch are dropped
    /// by the writer instead of being sent.
    epoch: AtomicU64,
}

impl SendQueue {
    fn queued(&self) -> usize {
        self.queued.load(Ordering::Acquire)
    }

    fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    fn reserve(&self, n: usize) {
        self.queued.fetch_add(n, Ordering::AcqRel);
    }

    /// Called exactly once per queued frame, whether it was sent or discarded.
    fn release(&self, n: usize) {
        let _ = self
            .queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                Some(cur.saturating_sub(n))
            });
    }

    /// Invalidates every frame currently queued for this attachment and returns
    /// the new epoch.
    fn invalidate(&self) -> u64 {
        self.epoch.fetch_add(1, Ordering::AcqRel) + 1
    }
}

struct PtyData {
    frame: Vec<u8>,
    /// Payload bytes this frame contributes to the queue accounting. Tracked
    /// separately from `frame.len()` so the header never distorts the budget.
    payload_len: usize,
    queue: Arc<SendQueue>,
    epoch: u64,
}

/// What the writer task is asked to send.
enum Outbound {
    Text(String),
    Pty(PtyData),
    /// Bulk bytes that MUST arrive: file transfer chunks. Unlike PTY output
    /// these are pulled one chunk at a time by the client, so the client's own
    /// request pacing is the backpressure and no queue accounting is needed.
    Bulk(Vec<u8>),
    /// Bytes that may be dropped under load: screencast frames. A dropped
    /// frame is superseded by the next one, so discarding beats queueing —
    /// the opposite of PTY output, where a gap is data loss.
    #[cfg(feature = "remote-browser")]
    Droppable(Vec<u8>),
}

/// The peer is gone; unwind the task that noticed.
struct Disconnected;

type Sent = Result<(), Disconnected>;

struct Attachment {
    session: Arc<PtySession>,
    pump: tokio::task::JoinHandle<()>,
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::channel::<Outbound>(OUTBOUND_CHANNEL_DEPTH);

    // The only task that touches the sink, so frames never interleave.
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let result = match msg {
                Outbound::Text(t) => sink.send(Message::Text(t.into())).await,
                Outbound::Pty(data) => {
                    // A resync overtook this frame: the client is being
                    // fast-forwarded, so sending it would waste socket time on
                    // output that has already been superseded.
                    let stale = data.queue.epoch() != data.epoch;
                    let r = if stale {
                        Ok(())
                    } else {
                        sink.send(Message::Binary(data.frame.into())).await
                    };
                    data.queue.release(data.payload_len);
                    r
                }
                Outbound::Bulk(b) => sink.send(Message::Binary(b.into())).await,
                #[cfg(feature = "remote-browser")]
                Outbound::Droppable(b) => sink.send(Message::Binary(b.into())).await,
            };
            if result.is_err() {
                break;
            }
        }
    });

    let mut attachments: HashMap<u32, Attachment> = HashMap::new();
    let mut next_handle: u32 = 1;
    // Upload chunks arrive as a binary frame immediately BEFORE the `fs.write`
    // that consumes them, keyed by the client's own request id. WebSocket
    // ordering makes that deterministic.
    let mut pending_uploads: HashMap<u32, Vec<u8>> = HashMap::new();

    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            Message::Text(text) => {
                let req = match serde_json::from_str::<ControlRequest>(&text) {
                    Ok(req) => req,
                    Err(e) => {
                        // A malformed control frame is the client's problem,
                        // not grounds for tearing down its terminals.
                        tracing::debug!("ignoring malformed control frame: {e}");
                        continue;
                    }
                };
                let resp = handle_control(
                    &state,
                    &req,
                    &mut attachments,
                    &mut next_handle,
                    &tx,
                    &mut pending_uploads,
                )
                .await;
                let json =
                    serde_json::to_string(&resp).expect("ControlResponse is always serializable");
                if tx.send(Outbound::Text(json)).await.is_err() {
                    break;
                }
            }
            Message::Binary(bytes) => {
                let Ok(frame) = BinaryFrame::decode(&bytes) else {
                    tracing::debug!("ignoring undecodable binary frame");
                    continue;
                };
                match frame.kind {
                    FrameKind::PtyInput => {
                        if let Some(a) = attachments.get(&frame.handle) {
                            if let Err(e) = a.session.write_input(&frame.payload) {
                                tracing::debug!("pty write failed: {e}");
                            }
                        }
                    }
                    FrameKind::FsChunk => {
                        // Park the bytes; the matching `fs.write` claims them.
                        // Bounded by the client's own one-chunk-at-a-time
                        // pacing, so this cannot grow without bound.
                        pending_uploads.insert(frame.handle, frame.payload);
                    }
                    _ => {}
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    for (_, a) in attachments.drain() {
        a.pump.abort();
    }
    writer.abort();
}

/// Filesystem ops. Returns `None` for any op that is not `fs.*`, so the caller
/// can fall through to the other families.
async fn handle_fs(
    state: &AppState,
    req: &ControlRequest,
    tx: &mpsc::Sender<Outbound>,
    pending_uploads: &mut HashMap<u32, Vec<u8>>,
) -> Option<ControlResponse> {
    let fs = &state.fs;
    let id = req.id;

    /// Unpack a required string param, or answer BAD_PARAMS.
    macro_rules! str_param {
        ($name:literal) => {
            match req.params.get($name).and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => {
                    return Some(ControlResponse::err(
                        id,
                        "BAD_PARAMS",
                        concat!("`", $name, "` is required"),
                    ))
                }
            }
        };
    }

    /// Map an `FsError` to the wire, preserving its own code.
    fn wire<T>(
        id: u64,
        r: Result<T, crate::fs::FsError>,
        ok: serde_json::Value,
    ) -> ControlResponse {
        match r {
            Ok(_) => ControlResponse::ok(id, ok),
            Err(e) => ControlResponse::err(id, e.code(), e.to_string()),
        }
    }

    match req.op.as_str() {
        "fs.roots" => Some(ControlResponse::ok(
            id,
            serde_json::json!({ "roots": fs.roots(), "home": fs.home() }),
        )),

        "fs.list" => {
            let path = str_param!("path");
            match fs.list(&path) {
                // Echo the canonicalized path back: the client displays it, and
                // showing the raw input would hide that `..` was resolved.
                Ok(entries) => {
                    let canonical = fs
                        .stat(&path)
                        .map(|e| e.path)
                        .unwrap_or_else(|_| path.clone());
                    Some(ControlResponse::ok(
                        id,
                        serde_json::json!({ "path": canonical, "entries": entries }),
                    ))
                }
                Err(e) => Some(ControlResponse::err(id, e.code(), e.to_string())),
            }
        }

        "fs.stat" => {
            let path = str_param!("path");
            match fs.stat(&path) {
                Ok(e) => Some(ControlResponse::ok(id, serde_json::json!({ "entry": e }))),
                Err(e) => Some(ControlResponse::err(id, e.code(), e.to_string())),
            }
        }

        "fs.read" => {
            let path = str_param!("path");
            let offset = req
                .params
                .get("offset")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let len = req
                .params
                .get("len")
                .and_then(|v| v.as_u64())
                .unwrap_or(64 * 1024) as usize;

            match fs.read_chunk(&path, offset, len) {
                Ok((bytes, total)) => {
                    let n = bytes.len();
                    // The binary frame is keyed by the client's own request id,
                    // so its collector is registered before this can arrive.
                    let frame = BinaryFrame {
                        kind: FrameKind::FsChunk,
                        handle: id as u32,
                        seq: offset,
                        payload: bytes,
                    };
                    if n > 0 {
                        let _ = tx.send(Outbound::Bulk(frame.encode())).await;
                    }
                    Some(ControlResponse::ok(
                        id,
                        serde_json::json!({
                            "len": n,
                            "total": total,
                            "eof": offset + n as u64 >= total,
                        }),
                    ))
                }
                Err(e) => Some(ControlResponse::err(id, e.code(), e.to_string())),
            }
        }

        "fs.write" => {
            let path = str_param!("path");
            let offset = req
                .params
                .get("offset")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let bytes = pending_uploads.remove(&(id as u32)).unwrap_or_default();
            let written = bytes.len();
            Some(wire(
                id,
                fs.write_chunk(&path, offset, &bytes),
                serde_json::json!({ "written": written }),
            ))
        }

        "fs.mkdir" => {
            let path = str_param!("path");
            Some(wire(id, fs.mkdir(&path), serde_json::json!({})))
        }

        "fs.remove" => {
            let path = str_param!("path");
            Some(wire(id, fs.remove(&path), serde_json::json!({})))
        }

        "fs.rename" => {
            let from = str_param!("from");
            let to = str_param!("to");
            Some(wire(id, fs.rename(&from, &to), serde_json::json!({})))
        }

        _ => None,
    }
}

/// Remote-browser ops. Returns `None` for anything not `browser.*`.
#[cfg(feature = "remote-browser")]
async fn handle_browser(
    state: &AppState,
    req: &ControlRequest,
    tx: &mpsc::Sender<Outbound>,
) -> Option<ControlResponse> {
    use crate::browser::BrowserService;

    let id = req.id;
    if !req.op.starts_with("browser.") {
        return None;
    }

    macro_rules! tab_id {
        () => {
            match req.params.get("tab_id").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => {
                    return Some(ControlResponse::err(
                        id,
                        "BAD_PARAMS",
                        "`tab_id` is required",
                    ))
                }
            }
        };
    }

    /// The service, launching Chrome on first use.
    async fn service(
        state: &AppState,
    ) -> Result<Arc<BrowserService>, crate::browser::BrowserError> {
        let mut guard = state.browser.lock().await;
        if let Some(svc) = guard.as_ref() {
            return Ok(Arc::clone(svc));
        }
        let downloads = state.config.downloads_dir();
        let svc = BrowserService::launch(state.config.chrome_path.clone(), downloads).await?;
        *guard = Some(Arc::clone(&svc));
        Ok(svc)
    }

    let svc = match service(state).await {
        Ok(s) => s,
        Err(e) => return Some(ControlResponse::err(id, e.code(), e.to_string())),
    };

    fn wire<T>(
        id: u64,
        r: Result<T, crate::browser::BrowserError>,
        ok: serde_json::Value,
    ) -> ControlResponse {
        match r {
            Ok(_) => ControlResponse::ok(id, ok),
            Err(e) => ControlResponse::err(id, e.code(), e.to_string()),
        }
    }

    match req.op.as_str() {
        "browser.launch" => {
            // Reaching here already launched it, and started the forwarders.
            start_browser_forwarders(&svc, tx);
            Some(ControlResponse::ok(id, serde_json::json!({ "ok": true })))
        }

        "browser.new_tab" => {
            let url = req
                .params
                .get("url")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            match svc.new_tab(url).await {
                Ok(tab) => Some(ControlResponse::ok(
                    id,
                    serde_json::json!({ "tab": tab_json(&tab) }),
                )),
                Err(e) => Some(ControlResponse::err(id, e.code(), e.to_string())),
            }
        }

        "browser.list_tabs" => match svc.list_tabs().await {
            Ok(tabs) => {
                let v: Vec<_> = tabs.iter().map(tab_json).collect();
                Some(ControlResponse::ok(id, serde_json::json!({ "tabs": v })))
            }
            Err(e) => Some(ControlResponse::err(id, e.code(), e.to_string())),
        },

        "browser.close_tab" => {
            let t = tab_id!();
            Some(wire(id, svc.close_tab(&t).await, serde_json::json!({})))
        }

        "browser.activate_tab" => {
            let t = tab_id!();
            Some(wire(id, svc.activate_tab(&t).await, serde_json::json!({})))
        }

        "browser.navigate" => {
            let t = tab_id!();
            let url = match req.params.get("url").and_then(|v| v.as_str()) {
                Some(u) => u.to_string(),
                None => return Some(ControlResponse::err(id, "BAD_PARAMS", "`url` is required")),
            };
            Some(wire(
                id,
                svc.navigate(&t, &url).await,
                serde_json::json!({}),
            ))
        }

        "browser.screencast_start" => {
            let t = tab_id!();
            let quality = req
                .params
                .get("quality")
                .and_then(|v| v.as_u64())
                .unwrap_or(60) as u32;
            let max_width = req
                .params
                .get("max_width")
                .and_then(|v| v.as_u64())
                .unwrap_or(1600) as u32;
            Some(wire(
                id,
                svc.start_screencast(&t, quality, max_width).await,
                serde_json::json!({}),
            ))
        }

        "browser.screencast_stop" => {
            let t = tab_id!();
            Some(wire(
                id,
                svc.stop_screencast(&t).await,
                serde_json::json!({}),
            ))
        }

        "browser.mouse" => {
            let t = tab_id!();
            let kind = req
                .params
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("mouseMoved");
            let x = req.params.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = req.params.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let button = req
                .params
                .get("button")
                .and_then(|v| v.as_str())
                .unwrap_or("left");
            let clicks = req
                .params
                .get("click_count")
                .and_then(|v| v.as_i64())
                .unwrap_or(1);
            Some(wire(
                id,
                svc.send_mouse(&t, kind, x, y, button, clicks).await,
                serde_json::json!({}),
            ))
        }

        "browser.key" => {
            let t = tab_id!();
            let kind = req
                .params
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("keyDown");
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let text = req
                .params
                .get("text")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            Some(wire(
                id,
                svc.send_key(&t, kind, key, text).await,
                serde_json::json!({}),
            ))
        }

        "browser.evaluate" => {
            let t = tab_id!();
            let expr = req
                .params
                .get("expression")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match svc.evaluate(&t, expr).await {
                Ok(result) => Some(ControlResponse::ok(
                    id,
                    serde_json::json!({ "result": result }),
                )),
                Err(e) => Some(ControlResponse::err(id, e.code(), e.to_string())),
            }
        }

        other => Some(ControlResponse::err(
            id,
            "UNKNOWN_OP",
            format!("unknown op '{other}'"),
        )),
    }
}

#[cfg(feature = "remote-browser")]
fn tab_json(t: &crate::browser::TabInfo) -> serde_json::Value {
    serde_json::json!({
        "tab_id": t.tab_id,
        // A stable u32 the binary frame header can carry — CDP target ids are
        // strings, and the frame's `handle` field is numeric.
        "handle": tab_handle(&t.tab_id),
        "url": t.url,
        "title": t.title,
        "active": t.active,
    })
}

/// Map a CDP target id onto a `u32` frame handle, stably for the process.
#[cfg(feature = "remote-browser")]
fn tab_handle(tab_id: &str) -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    tab_id.hash(&mut h);
    // Fold to 32 bits and avoid 0, which reads as "no handle" elsewhere.
    (h.finish() as u32) | 1
}

/// Forward screencast frames and console lines to this connection. Idempotent
/// per connection: a second `browser.launch` must not double-subscribe.
#[cfg(feature = "remote-browser")]
fn start_browser_forwarders(
    svc: &Arc<crate::browser::BrowserService>,
    tx: &mpsc::Sender<Outbound>,
) {
    // Forwarders are per-connection. A repeated `browser.launch` spawns another
    // pair, but each exits as soon as its channel closes, so the duplicate is
    // bounded and harmless — and the client only launches once per session.
    let mut frames = svc.subscribe_frames();
    let ftx = tx.clone();
    tokio::spawn(async move {
        loop {
            match frames.recv().await {
                Ok(f) => {
                    let frame = BinaryFrame {
                        kind: FrameKind::BrowserFrame,
                        handle: tab_handle(&f.tab_id),
                        seq: f.seq,
                        payload: f.jpeg,
                    };
                    // Metadata rides a control event so the client can map
                    // clicks; it is small and must not be dropped.
                    let meta = serde_json::json!({
                        "event": "browser.frame_meta",
                        "tab_id": f.tab_id,
                        "width": f.width,
                        "height": f.height,
                        "device_width": f.device_width,
                        "device_height": f.device_height,
                        "offset_top": f.offset_top,
                        "page_scale": f.page_scale,
                    });
                    if ftx.send(Outbound::Text(meta.to_string())).await.is_err() {
                        return;
                    }
                    // Screencast frames are droppable: if the queue is full the
                    // next frame supersedes this one anyway, so discarding beats
                    // stalling the whole connection behind stale video.
                    if ftx.try_send(Outbound::Droppable(frame.encode())).is_err() {
                        tracing::trace!("dropped a screencast frame; client is behind");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::debug!("screencast forwarder lagged {n} frames");
                }
                Err(_) => return,
            }
        }
    });

    let mut console = svc.subscribe_console();
    let ctx = tx.clone();
    tokio::spawn(async move {
        while let Ok(line) = console.recv().await {
            let msg = serde_json::json!({
                "event": "browser.console",
                "tab_id": line.tab_id,
                "level": line.level,
                "text": line.text,
            });
            if ctx.send(Outbound::Text(msg.to_string())).await.is_err() {
                return;
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn handle_control(
    state: &AppState,
    req: &ControlRequest,
    attachments: &mut HashMap<u32, Attachment>,
    next_handle: &mut u32,
    tx: &mpsc::Sender<Outbound>,
    pending_uploads: &mut HashMap<u32, Vec<u8>>,
) -> ControlResponse {
    if let Some(resp) = handle_fs(state, req, tx, pending_uploads).await {
        return resp;
    }
    #[cfg(feature = "remote-browser")]
    if let Some(resp) = handle_browser(state, req, tx).await {
        return resp;
    }
    match req.op.as_str() {
        "pty.list" => match serde_json::to_value(state.registry.list()) {
            Ok(v) => ControlResponse::ok(req.id, serde_json::json!({ "sessions": v })),
            Err(e) => ControlResponse::err(req.id, "INTERNAL", e.to_string()),
        },

        "pty.create" => {
            let p: PtyCreateParams = match serde_json::from_value(req.params.clone()) {
                Ok(p) => p,
                Err(e) => return ControlResponse::err(req.id, "BAD_PARAMS", e.to_string()),
            };
            let shell_cfg = match &p.shell {
                Some(name) => match state.config.shell_by_name(name) {
                    Some(s) => s,
                    None => {
                        return ControlResponse::err(
                            req.id,
                            "SHELL_NOT_FOUND",
                            format!("no shell named '{name}' in config"),
                        )
                    }
                },
                None => state.config.default_shell(),
            };
            let spec = PtySpec {
                shell: shell_cfg.command.clone(),
                args: shell_cfg.args.clone(),
                cwd: p.cwd.clone(),
                cols: p.cols,
                rows: p.rows,
            };
            match state.registry.create(spec) {
                Ok(session) => {
                    let handle = *next_handle;
                    *next_handle += 1;
                    let session_id = session.id();
                    let pump = spawn_pump(Arc::clone(&session), handle, 0, tx.clone());
                    attachments.insert(handle, Attachment { session, pump });
                    ControlResponse::ok(
                        req.id,
                        serde_json::json!({ "session_id": session_id, "handle": handle }),
                    )
                }
                Err(e) => ControlResponse::err(req.id, "SPAWN_FAILED", e.to_string()),
            }
        }

        "pty.attach" => {
            let p: PtyAttachParams = match serde_json::from_value(req.params.clone()) {
                Ok(p) => p,
                Err(e) => return ControlResponse::err(req.id, "BAD_PARAMS", e.to_string()),
            };
            let Some(session) = state.registry.get(p.session_id) else {
                return ControlResponse::err(req.id, "NO_SUCH_SESSION", "unknown session_id");
            };
            let handle = *next_handle;
            *next_handle += 1;
            // The ring may have lapped the client's cursor while it was away.
            // `read_since` clamps forward and reports where the stream really
            // resumes, which is what the client needs in order to redraw.
            let (start, _) = session.read_since(p.since);
            let truncated_from = if start > p.since { Some(start) } else { None };
            let (cols, rows) = session.size();

            let pump = spawn_pump(Arc::clone(&session), handle, p.since, tx.clone());
            attachments.insert(handle, Attachment { session, pump });

            ControlResponse::ok(
                req.id,
                serde_json::json!({
                    "handle": handle,
                    "cols": cols,
                    "rows": rows,
                    "truncated_from": truncated_from,
                }),
            )
        }

        "pty.detach" => {
            let handle = req
                .params
                .get("handle")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32;
            if let Some(a) = attachments.remove(&handle) {
                a.pump.abort();
            }
            ControlResponse::ok(req.id, serde_json::json!({}))
        }

        "pty.resize" => {
            let p: PtyResizeParams = match serde_json::from_value(req.params.clone()) {
                Ok(p) => p,
                Err(e) => return ControlResponse::err(req.id, "BAD_PARAMS", e.to_string()),
            };
            match attachments.get(&p.handle) {
                Some(a) => match a.session.resize(p.cols, p.rows) {
                    Ok(()) => ControlResponse::ok(req.id, serde_json::json!({})),
                    Err(e) => ControlResponse::err(req.id, "RESIZE_FAILED", e.to_string()),
                },
                None => ControlResponse::err(req.id, "NO_SUCH_HANDLE", "unknown handle"),
            }
        }

        "pty.kill" => {
            let id: Uuid = match req.params.get("session_id").and_then(|v| v.as_str()) {
                Some(s) => match s.parse() {
                    Ok(id) => id,
                    Err(e) => return ControlResponse::err(req.id, "BAD_PARAMS", e.to_string()),
                },
                None => return ControlResponse::err(req.id, "BAD_PARAMS", "session_id required"),
            };
            match state.registry.kill(id) {
                Ok(()) => ControlResponse::ok(req.id, serde_json::json!({})),
                Err(e) => ControlResponse::err(req.id, "KILL_FAILED", e.to_string()),
            }
        }

        other => ControlResponse::err(req.id, "UNKNOWN_OP", format!("unknown op '{other}'")),
    }
}

fn spawn_pump(
    session: Arc<PtySession>,
    handle: u32,
    since: u64,
    tx: mpsc::Sender<Outbound>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        Pump {
            session,
            handle,
            next_seq: since,
            tx,
            queue: Arc::new(SendQueue::default()),
            throttled: false,
        }
        .run()
        .await
    })
}

/// Feeds one attachment: replays `since..head`, then streams live output.
struct Pump {
    session: Arc<PtySession>,
    handle: u32,
    /// The first sequence number this attachment has *not* been sent.
    next_seq: u64,
    tx: mpsc::Sender<Outbound>,
    queue: Arc<SendQueue>,
    /// True between crossing the high-water mark and the queue draining back
    /// below the low-water mark. While set, no new output is queued.
    throttled: bool,
}

impl Pump {
    async fn run(mut self) {
        // Subscribe BEFORE reading the backlog. Output produced while the
        // replay is being assembled lands in this receiver instead of being
        // lost between the two calls. The resulting overlap cannot duplicate
        // anything: every frame is cut from the ring at `next_seq`, so a chunk
        // the broadcast redelivers simply yields no new bytes.
        let mut rx = self.session.subscribe();

        if self.forward().await.is_err() {
            return;
        }

        let mut ticker = tokio::time::interval(PUMP_TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut dead_ticks: u32 = 0;

        loop {
            tokio::select! {
                received = rx.recv() => match received {
                    // The chunk itself is discarded: it is a wake-up, and the
                    // ring is the source of truth for both content and
                    // sequence numbers. Once the client is far enough behind,
                    // forwarding is left to the ticker so the backlog is
                    // coalesced — see COALESCE_ABOVE.
                    Ok(_) => {
                        if self.queue.queued() < COALESCE_ABOVE
                            && self.forward().await.is_err()
                        {
                            return;
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        tracing::debug!(
                            session = %self.session.id(),
                            "pump lagged {n} broadcast chunks; resyncing from the ring"
                        );
                        if self.resync().await.is_err() {
                            return;
                        }
                    }
                    Err(RecvError::Closed) => {
                        let _ = self.send_event(ControlEvent::PtyExit {
                            session_id: self.session.id(),
                            status: self.session.exit_status().unwrap_or(0),
                        }).await;
                        return;
                    }
                },
                _ = ticker.tick() => {
                    if self.forward().await.is_err() {
                        return;
                    }
                    if self.session.is_alive() {
                        dead_ticks = 0;
                    } else {
                        dead_ticks += 1;
                        // Never announce the exit with output still owed. A
                        // session that ends while its client is throttled
                        // still has a ring tail to deliver, and returning here
                        // would silently truncate the last thing the shell
                        // ever printed. The pump keeps ticking until the queue
                        // drains and the tail goes out, or until the socket
                        // errors and unwinds it.
                        let flushed =
                            !self.throttled && self.next_seq >= self.session.head_seq();
                        if dead_ticks >= DEAD_TICKS_BEFORE_EXIT && flushed {
                            // The reaper stores the status before flipping
                            // `alive`, and DEAD_TICKS_BEFORE_EXIT adds ~300ms
                            // on top, so by here the status is present. The
                            // unwrap_or(0) covers only the theoretical race.
                            let _ = self.send_event(ControlEvent::PtyExit {
                                session_id: self.session.id(),
                                status: self.session.exit_status().unwrap_or(0),
                            }).await;
                            return;
                        }
                    }
                }
            }
        }
    }

    /// Sends everything the ring holds from `next_seq` onward, unless the
    /// client is too far behind to keep up.
    async fn forward(&mut self) -> Sent {
        if self.throttled {
            if self.queue.queued() > SEND_QUEUE_LOW_WATER {
                return Ok(()); // still draining; stay quiet
            }
            self.throttled = false;
            return self.resync().await;
        }

        if self.queue.queued() > SEND_QUEUE_HIGH_WATER || self.tx.capacity() == 0 {
            // Drop what is queued for this attachment rather than let the
            // backlog grow. The resync frame follows once the socket has caught
            // up enough to be worth writing to.
            self.queue.invalidate();
            self.throttled = true;
            tracing::debug!(
                session = %self.session.id(),
                "outbound queue over high water; dropping queued output"
            );
            return Ok(());
        }

        let (start, bytes) = self.session.read_since(self.next_seq);
        if start > self.next_seq {
            // The ring lapped bytes this attachment never received.
            self.send_event(ControlEvent::PtyResync {
                session_id: self.session.id(),
                from_seq: start,
            })
            .await?;
        }
        if bytes.is_empty() {
            self.next_seq = start;
            return Ok(());
        }
        self.next_seq = start + bytes.len() as u64;
        self.send_pty(start, bytes).await
    }

    /// Announces a discontinuity and restarts the stream from the ring tail.
    async fn resync(&mut self) -> Sent {
        // Anything still queued predates the jump.
        self.queue.invalidate();

        let (start, tail) = self.session.read_since(self.next_seq);
        self.send_event(ControlEvent::PtyResync {
            session_id: self.session.id(),
            from_seq: start,
        })
        .await?;
        self.next_seq = start + tail.len() as u64;
        if tail.is_empty() {
            return Ok(());
        }
        self.send_pty(start, tail).await
    }

    async fn send_event(&self, event: ControlEvent) -> Sent {
        let json = serde_json::to_string(&event).expect("ControlEvent is always serializable");
        self.tx
            .send(Outbound::Text(json))
            .await
            .map_err(|_| Disconnected)
    }

    async fn send_pty(&self, seq: u64, payload: Vec<u8>) -> Sent {
        let payload_len = payload.len();
        let frame = BinaryFrame {
            kind: FrameKind::PtyOutput,
            handle: self.handle,
            seq,
            payload,
        }
        .encode();

        // Reserve before queueing so the counter can never read low while a
        // frame is in flight; the writer releases it on the way out.
        self.queue.reserve(payload_len);
        let data = PtyData {
            frame,
            payload_len,
            queue: Arc::clone(&self.queue),
            epoch: self.queue.epoch(),
        };
        match self.tx.send(Outbound::Pty(data)).await {
            Ok(()) => Ok(()),
            Err(_) => {
                self.queue.release(payload_len);
                Err(Disconnected)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_accounts_bytes_that_are_reserved_but_not_yet_written() {
        let q = SendQueue::default();
        assert_eq!(q.queued(), 0);
        q.reserve(1000);
        q.reserve(2000);
        // The draft this replaces added and subtracted the same length on the
        // same code path, so the counter always read zero and the high-water
        // check could never fire.
        assert_eq!(q.queued(), 3000);
        q.release(1000);
        assert_eq!(q.queued(), 2000);
        q.release(2000);
        assert_eq!(q.queued(), 0);
    }

    #[test]
    fn queue_release_saturates_instead_of_wrapping() {
        let q = SendQueue::default();
        q.reserve(10);
        q.release(999);
        assert_eq!(q.queued(), 0, "the counter must never wrap around");
    }

    #[test]
    fn invalidate_makes_previously_stamped_frames_stale() {
        let q = SendQueue::default();
        let stamped = q.epoch();
        assert_eq!(q.epoch(), stamped, "unchanged epochs stay valid");
        let after = q.invalidate();
        assert_ne!(after, stamped);
        assert_eq!(q.epoch(), after);
        assert_ne!(
            q.epoch(),
            stamped,
            "frames queued before a resync must be recognised as stale"
        );
    }

    /// Compile-time guards, not runtime checks: the hysteresis is only
    /// hysteresis if the marks are ordered, and 4 MiB is the figure the
    /// transport design specifies.
    #[test]
    fn watermarks_are_ordered_and_match_the_spec() {
        const { assert!(SEND_QUEUE_LOW_WATER < SEND_QUEUE_HIGH_WATER) };
        const { assert!(SEND_QUEUE_HIGH_WATER == 4 * 1024 * 1024) };
        const { assert!(COALESCE_ABOVE < SEND_QUEUE_HIGH_WATER) };
    }
}
