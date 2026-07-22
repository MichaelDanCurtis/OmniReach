//! The load-bearing test for OmniReach.
//!
//! A client that disconnects mid-stream and reattaches must observe a stream
//! byte-identical to one that never disconnected. If this file does not pass,
//! session persistence is unverified no matter how good the feature looks by
//! hand — every other guarantee in the product rests on it.
//!
//! # Two traps that make failures here look like hangs
//!
//! 1. `Runtime::drop` WAITS for `spawn_blocking` tasks and cannot cancel them.
//!    Every PTY read loop is one. So a test that leaves a live shell running
//!    hangs teardown forever — and if the test was *already* panicking, the
//!    panic message is swallowed and the whole thing reads as an infinite hang
//!    rather than a failed assertion. Every test below therefore kills its
//!    session BEFORE asserting anything.
//!
//! 2. A PTY applies ONLCR: the shell writes `\n`, the client receives `\r\n`.
//!    Asserting on `line-3\n` silently never matches. Verified directly:
//!    `/bin/sh -c "printf 'line-0\n'"` through a pty yields `b"line-0\r\n"`.
//!    `normalize` below strips the carriage returns so assertions read clearly.

use futures_util::{SinkExt, StreamExt};
use omnireach_protocol::{BinaryFrame, FrameKind};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Boot a host on an ephemeral port and return its websocket URL.
///
/// Uses `AppState::new` rather than a struct literal because `auth` is
/// `#[cfg(feature = "access-auth")]` — a literal breaks `--no-default-features`,
/// which is exactly the configuration the Windows cross-check runs in.
async fn start_host(scrollback: usize) -> String {
    let config = Arc::new(omnireach_host::config::Config::from_str("").unwrap());
    let registry = Arc::new(omnireach_host::sessions::SessionRegistry::new(scrollback));
    let state = omnireach_host::transport::AppState::new(registry, config);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, omnireach_host::transport::router(state))
            .await
            .unwrap();
    });
    format!("ws://{addr}/ws")
}

async fn connect(url: &str) -> Ws {
    tokio_tungstenite::connect_async(url).await.unwrap().0
}

/// Send a control request and wait for the matching response, ignoring
/// interleaved events and binary frames.
async fn request(ws: &mut Ws, id: u64, op: &str, params: serde_json::Value) -> serde_json::Value {
    ws.send(Message::Text(
        serde_json::json!({ "id": id, "op": op, "params": params })
            .to_string()
            .into(),
    ))
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_secs(2), ws.next()).await
        else {
            continue;
        };
        if let Message::Text(t) = msg {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            if v["id"] == id {
                assert_eq!(v["ok"], true, "{op} failed: {v}");
                return v["result"].clone();
            }
        }
    }
    panic!("no response to {op} (id {id})");
}

/// Drain PtyOutput frames for `dur`. Returns the concatenated bytes and the
/// highest sequence consumed, which is the client's resume point.
async fn drain(ws: &mut Ws, dur: Duration) -> (Vec<u8>, u64) {
    let mut out = Vec::new();
    let mut high = 0u64;
    let deadline = tokio::time::Instant::now() + dur;
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_millis(300), ws.next()).await
        else {
            continue;
        };
        if let Message::Binary(b) = msg {
            if let Ok(f) = BinaryFrame::decode(&b) {
                if f.kind == FrameKind::PtyOutput {
                    high = f.seq + f.payload.len() as u64;
                    out.extend_from_slice(&f.payload);
                }
            }
        }
    }
    (out, high)
}

async fn send_input(ws: &mut Ws, handle: u32, data: &str) {
    let frame = BinaryFrame {
        kind: FrameKind::PtyInput,
        handle,
        seq: 0,
        payload: data.as_bytes().to_vec(),
    };
    ws.send(Message::Binary(frame.encode().into()))
        .await
        .unwrap();
}

/// Kill a session over a fresh connection. Must run BEFORE assertions — see
/// the module docs.
async fn kill(url: &str, session_id: &str) {
    let mut ws = connect(url).await;
    request(
        &mut ws,
        9_999,
        "pty.kill",
        serde_json::json!({ "session_id": session_id }),
    )
    .await;
}

/// Undo the PTY's ONLCR translation so assertions can talk about `\n`.
fn normalize(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).replace('\r', "")
}

/// Numbered lines, paced so the stream straddles a disconnect.
fn paced_counter(n: u32) -> String {
    format!("i=0; while [ $i -lt {n} ]; do printf 'line-%d\\n' $i; i=$((i+1)); sleep 0.005; done\n")
}

/// Numbered lines as fast as the shell can emit them — no `sleep`, so no fork
/// per iteration. Used where the goal is volume, not duration.
fn fast_counter(n: u32) -> String {
    format!("i=0; while [ $i -lt {n} ]; do printf 'line-%d\\n' $i; i=$((i+1)); done\n")
}

#[tokio::test]
async fn reconnect_and_replay_produces_a_byte_identical_stream() {
    const LINES: u32 = 400;
    let url = start_host(1024 * 1024).await;
    let mut ws = connect(&url).await;

    let created = request(
        &mut ws,
        1,
        "pty.create",
        serde_json::json!({"shell": null, "cwd": null, "cols": 80, "rows": 24}),
    )
    .await;
    let session_id = created["session_id"].as_str().unwrap().to_string();
    let handle = created["handle"].as_u64().unwrap() as u32;

    send_input(&mut ws, handle, &paced_counter(LINES)).await;

    // Consume part of the output, then hard-drop the connection mid-stream.
    let (first_half, last_seq) = drain(&mut ws, Duration::from_millis(700)).await;
    drop(ws);

    // Stay away while the child keeps producing.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Reattach from exactly where we left off.
    let mut ws2 = connect(&url).await;
    let attached = request(
        &mut ws2,
        2,
        "pty.attach",
        serde_json::json!({"session_id": session_id, "since": last_seq}),
    )
    .await;
    let (second_half, _) = drain(&mut ws2, Duration::from_secs(6)).await;
    drop(ws2);

    // Kill BEFORE asserting, or a failed assert hangs instead of reporting.
    kill(&url, &session_id).await;

    assert!(!first_half.is_empty(), "expected output before disconnect");
    assert!(
        attached["truncated_from"].is_null(),
        "a 1MB ring must not truncate this test's output; got {attached}"
    );

    let mut combined = first_half;
    combined.extend_from_slice(&second_half);
    let text = normalize(&combined);

    // Every line exactly once. A duplicate means the resume offset was too
    // early; a missing line means it was too late. Both are silent corruption
    // of an agent transcript, which is the one failure this product cannot have.
    let mut duplicated = Vec::new();
    let mut missing = Vec::new();
    for i in 0..LINES {
        match text.matches(&format!("line-{i}\n")).count() {
            1 => {}
            0 => missing.push(i),
            n => duplicated.push((i, n)),
        }
    }
    assert!(
        missing.is_empty() && duplicated.is_empty(),
        "replay was not byte-exact across the disconnect.\n  missing: {missing:?}\n  duplicated: {duplicated:?}"
    );
}

#[tokio::test]
async fn session_keeps_running_while_no_client_is_attached() {
    let url = start_host(256 * 1024).await;
    let mut ws = connect(&url).await;

    let created = request(
        &mut ws,
        1,
        "pty.create",
        serde_json::json!({"shell": null, "cwd": null, "cols": 80, "rows": 24}),
    )
    .await;
    let session_id = created["session_id"].as_str().unwrap().to_string();
    let handle = created["handle"].as_u64().unwrap() as u32;

    send_input(
        &mut ws,
        handle,
        "sleep 0.5; printf 'produced-while-away\\n'\n",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(ws); // leave before the output exists

    tokio::time::sleep(Duration::from_millis(1200)).await;

    let mut ws2 = connect(&url).await;
    request(
        &mut ws2,
        2,
        "pty.attach",
        serde_json::json!({"session_id": session_id, "since": 0}),
    )
    .await;
    let (bytes, _) = drain(&mut ws2, Duration::from_secs(3)).await;
    drop(ws2);

    kill(&url, &session_id).await;

    assert!(
        normalize(&bytes).contains("produced-while-away"),
        "output produced while no client was attached was lost"
    );
}

#[tokio::test]
async fn attaching_past_the_ring_reports_truncation_rather_than_lying() {
    // A tiny ring guarantees eviction. The host must say so: a silent gap in a
    // Claude Code transcript is worse than an honest marker, because the user
    // cannot tell the reasoning they are reading is missing a step.
    let url = start_host(4096).await;
    let mut ws = connect(&url).await;

    let created = request(
        &mut ws,
        1,
        "pty.create",
        serde_json::json!({"shell": null, "cwd": null, "cols": 80, "rows": 24}),
    )
    .await;
    let session_id = created["session_id"].as_str().unwrap().to_string();
    let handle = created["handle"].as_u64().unwrap() as u32;

    // ~2000 lines of ~11 bytes overflows a 4KB ring many times over. No
    // `sleep` in the loop, so this completes in well under the drain window.
    send_input(&mut ws, handle, &fast_counter(2000)).await;
    let (_, high) = drain(&mut ws, Duration::from_secs(5)).await;
    drop(ws);

    let mut ws2 = connect(&url).await;
    let attached = request(
        &mut ws2,
        2,
        "pty.attach",
        serde_json::json!({"session_id": session_id, "since": 0}),
    )
    .await;
    drop(ws2);

    kill(&url, &session_id).await;

    assert!(
        high > 4096,
        "producer emitted only {high} bytes — it never overflowed the 4KB ring, \
         so this test would not have exercised truncation at all"
    );
    assert!(
        !attached["truncated_from"].is_null(),
        "attaching at seq 0 to an overflowed 4KB ring must report truncation, got {attached}"
    );
    assert!(
        attached["truncated_from"].as_u64().unwrap() > 0,
        "truncated_from must be the oldest surviving sequence number"
    );
}
